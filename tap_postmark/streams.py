"""Stream type classes for tap-postmark."""

from typing import Any, Optional, TypeVar
from urllib import parse

import arrow
import orjson
import requests
from memoization import cached
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.streams import RESTStream


_TToken = TypeVar("_TToken")

# TODO: check how state/config with meltano works
# TODO: selective parent-child requests based on parent response
# TODO: more streams for the other endpoints on postmark, depending on what's necessary


class PostmarkStream(RESTStream):
    url_base = "https://api.postmarkapp.com"

    @property
    def http_headers(self) -> dict:
        return {
            "Accept": "application/json",
        }

    @property
    @cached
    def authenticator(self) -> APIKeyAuthenticator:
        return APIKeyAuthenticator(self, "X-Postmark-Server-Token", self.config['auth_token'])


class OutboundMessageStream(PostmarkStream):
    name = "outbound_messages"

    path = "/messages/outbound"
    records_jsonpath = "$.Messages[*]"
    primary_keys = ["MessageID"]

    start_date: str = '2023-01-12T00:00:00.000000+00:00'
    timewindow_interval_minutes: int = 1
    count_by: int = 500

    schema = th.PropertiesList(
        th.Property("MessageID", th.StringType),
        th.Property("MessageStream", th.StringType),
        th.Property("Tag", th.StringType),
        th.Property("Subject", th.StringType),
        th.Property("Status", th.StringType),
        th.Property("From", th.StringType),
        th.Property("Cc", th.StringType),
        th.Property("Bcc", th.StringType),
        th.Property("To", th.StringType),
        th.Property("ReceivedAt", th.DateTimeType),
        th.Property("TrackOpens", th.BooleanType),
        th.Property("TrackLinks", th.StringType),
        th.Property("Sandboxed", th.StringType),
    ).to_dict()

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any]) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""

        num_messages_in_previous_request = len(response.json()['Messages'])

        # Request params from previous request
        req_url = response.request.url
        req_params = parse.parse_qs(parse.urlparse(req_url).query)
        params = {k: v[0] for (k, v) in req_params.items()}

        # If there were still messages in this datetime range, just increase the offset and keep the datetime range
        if num_messages_in_previous_request:
            params['offset'] = int(params['offset']) + self.count_by
            return params

        # There are no messages in the previous request, so datetime range was exhausted -> shift the datetime range by 1 minute and reset the offset
        params['offset'] = 0
        params['fromdate'] = arrow.get(params['fromdate']).shift(minutes=self.timewindow_interval_minutes).isoformat()
        params['todate'] = arrow.get(params['todate']).shift(minutes=self.timewindow_interval_minutes).isoformat()

        # If we have processed the entire history, stop requesting
        if arrow.get(params['fromdate']) > arrow.utcnow():
            return None

        return params

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[_TToken]) -> dict[str, Any]:
        if next_page_token is None:
            starting_dt = arrow.get(self.start_date).to('US/Eastern')
            next_page_token = {
                'count': self.count_by,
                'offset': next_page_token or 0,
                'fromdate': starting_dt.isoformat(),
                'todate': starting_dt.shift(minutes=1).isoformat(),
            }
        return next_page_token

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row['Cc'] = ','.join([e['Email'] for e in row['Cc']])
        row['Bcc'] = ','.join([e['Email'] for e in row['Bcc']])
        row['To'] = ','.join([e['Email'] for e in row['To']])
        row['From'] = row['From'].split('<')[1].replace('>', '')
        row['ReceivedAt'] = arrow.get(row['ReceivedAt']).to('utc').isoformat()
        return row

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        # TODO: add whether or not it was tracked or not, and then we can skip a request if we dont need it?
        return {
            "MessageID": record["MessageID"],
        }


class SingleOutboundMessageOpenStream(PostmarkStream):
    name = "single_outbound_message_opens"

    path = "/messages/outbound/opens/{MessageID}"
    records_jsonpath = "$.Opens[*]"
    primary_keys = ["MessageID"]

    count_by: int = 500

    # SingleOutboundMessageOpenStream streams should be invoked once per parent epic:
    parent_stream_type = OutboundMessageStream

    # Assume opens don't have `updated_at` incremented when outbound_messages are changed:
    ignore_parent_replication_keys = True

    schema = th.PropertiesList(
        th.Property("MessageID", th.StringType),
        th.Property("MessageStream", th.StringType),
        th.Property("Tag", th.StringType),
        th.Property("ReceivedAt", th.DateTimeType),
        th.Property("Recipient", th.BooleanType),
    ).to_dict()

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[_TToken]) -> dict[str, Any]:
        return {
            'count': self.count_by,
            'offset': 0,
        }

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code == 422:
            if response.json()['ErrorCode'] != 701:
                return super().validate_response(response)

        return None


class SingleOutboundMessageEventStream(PostmarkStream):
    name = "single_outbound_message_events"

    path = "/messages/outbound/{MessageID}/details"
    records_jsonpath = "$.MessageEvents[*]"
    primary_keys = ["ID"]

    count_by: int = 500

    # SingleOutboundMessageOpenStream streams should be invoked once per parent epic:
    parent_stream_type = OutboundMessageStream

    # Assume opens don't have `updated_at` incremented when outbound_messages are changed:
    ignore_parent_replication_keys = True

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("MessageID", th.StringType),
        th.Property("Recipient", th.StringType),
        th.Property("Type", th.StringType),
        th.Property("ReceivedAt", th.DateTimeType),
        th.Property("Details", th.StringType),
    ).to_dict()

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code == 422:
            if response.json()['ErrorCode'] != 701:
                return super().validate_response(response)

        return None

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row['Details'] = orjson.dumps(row['Details']).decode('utf-8')
        row['MessageID'] = context['MessageID']
        row['ID'] = '|'.join([row["MessageID"], row["Recipient"], row["Type"], row["ReceivedAt"]])
        return row
