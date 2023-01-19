"""Stream type classes for tap-postmark."""

from typing import Any, Optional, TypeVar, Iterable
from urllib import parse

import arrow
import orjson
import copy
import logging
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
    replication_key = "ReceivedAt"

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
        th.Property("Cc", th.ArrayType(th.StringType)),
        th.Property("Bcc", th.ArrayType(th.StringType)),
        th.Property("To", th.ArrayType(
            th.ObjectType(
                th.Property("Email", th.StringType),
                th.Property("Name", th.StringType),
            ))
        ),
        th.Property("Recipients", th.ArrayType(th.StringType)),
        th.Property("Attachments", th.ArrayType(th.StringType)),
        th.Property("ReceivedAt", th.DateTimeType),
        th.Property("TrackOpens", th.BooleanType),
        th.Property("TrackLinks", th.StringType),
        th.Property("Sandboxed", th.StringType),
    ).to_dict()

    def __init__(self, *args, **kwargs) -> None:
        self.receivers_done = set()
        super().__init__(*args, **kwargs)

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
        cutoff_date = arrow.utcnow()
        cutoff_date = arrow.get('2023-01-13T00:00:00.000000+00:00')  # try to run for one single day
        if arrow.get(params['fromdate']) >= cutoff_date:
            return None

        return params

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[_TToken]) -> dict[str, Any]:
        if next_page_token is None:

            # Determine the starting_date
            starting_dt = self.get_starting_timestamp(context)
            if starting_dt is None:
                starting_dt = arrow.get(self.start_date).to('US/Eastern')
                # print("starting_dt is None, now: " + starting_dt.isoformat())
            else:
                starting_dt = arrow.get(starting_dt).to('US/Eastern')
                # print("starting_dt exists, now: " + starting_dt.isoformat())

            next_page_token = {
                'count': self.count_by,
                'offset': next_page_token or 0,
                'fromdate': starting_dt.isoformat(),
                'todate': starting_dt.shift(minutes=1).isoformat(),
            }
        return next_page_token

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row['From'] = row['From'].split('<')[1].replace('>', '')
        row['ReceivedAt'] = arrow.get(row['ReceivedAt']).to('utc').isoformat()
        return row

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "MessageID": record["MessageID"],
            "TrackOpens": record["TrackOpens"],
            "TrackLinks": record["TrackLinks"],
            "Recipients": record["Recipients"],
        }


class SingleOutboundMessageEventStream(PostmarkStream):
    name = "single_outbound_message_events"

    path = "/messages/outbound/{MessageID}/details"
    records_jsonpath = "$.MessageEvents[*]"
    primary_keys = ["MessageID", "Recipient", "Type", "ReceivedAt"]

    count_by: int = 500

    parent_stream_type = OutboundMessageStream

    # Assume opens don't have `updated_at` incremented when outbound_messages are changed:
    ignore_parent_replication_keys = True

    schema = th.PropertiesList(
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
        return row


class StatsOutboundOvervewStream(PostmarkStream):
    name = "stats_outbound_overview"

    path = "/stats/outbound"
    primary_keys = ["Tag", "Date"]
    replication_key = "Date"
    start_date: str = '2023-01-01T00:00:00.000000+00:00'

    schema = th.PropertiesList(
        th.Property("Date", th.DateType),
        th.Property("Tag", th.StringType),
        th.Property("Sent", th.IntegerType),
        th.Property("Bounced", th.IntegerType),
        th.Property("SMTPApiErrors", th.IntegerType),
        th.Property("BounceRate", th.NumberType),
        th.Property("SpamComplaints", th.StringType),
        th.Property("SpamComplaintsRate", th.NumberType),
        th.Property("Opens", th.IntegerType),
        th.Property("UniqueOpens", th.IntegerType),
        th.Property("Tracked", th.IntegerType),
        th.Property("WithLinkTracking", th.IntegerType),
        th.Property("WithOpenTracking", th.IntegerType),
        th.Property("TotalTrackedLinksSent", th.IntegerType),
        th.Property("UniqueLinksClicked", th.IntegerType),
        th.Property("TotalClicks", th.IntegerType),
        th.Property("WithClientRecorded", th.IntegerType),
        th.Property("WithPlatformRecorded", th.IntegerType),
    ).to_dict()

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any]) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""

        # Request params from previous request
        req_url = response.request.url
        req_params = parse.parse_qs(parse.urlparse(req_url).query)
        params = {k: v[0] for (k, v) in req_params.items()}

        params['fromdate'] = arrow.get(params['todate']).isoformat()
        params['todate'] = arrow.get(params['fromdate']).shift(days=1).isoformat()

        # If we have processed the entire history, stop requesting
        cutoff_date = arrow.utcnow()
        if arrow.get(params['fromdate']) >= cutoff_date:
            return None

        return params

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[_TToken]) -> dict[str, Any]:
        if next_page_token is None:

            # Determine the starting_date
            starting_dt = self.get_starting_timestamp(context)
            if starting_dt is None:
                starting_dt = arrow.get(self.start_date).to('US/Eastern')
                # print("starting_dt is None, now: " + starting_dt.isoformat())
            else:
                starting_dt = arrow.get(starting_dt).to('US/Eastern')
                # print("starting_dt exists, now: " + starting_dt.isoformat())

            next_page_token = {
                'fromdate': starting_dt.isoformat(),
                'todate': starting_dt.shift(days=1).isoformat(),
            }

        # next_page_token['tag'] = 'lala'
        return next_page_token

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        d = response.json()

        req_params = parse.parse_qs(parse.urlparse(response.request.url).query)
        params = {k: v[0] for (k, v) in req_params.items()}

        d['Tag'] = params.get('tag', 'lala')
        d["Date"] = params['fromdate']

        yield d



class SingleOutboundMessageClickStream(PostmarkStream):
    name = "single_outbound_message_clicks"

    path = "/messages/outbound/clicks/{MessageID}"
    records_jsonpath = "$.Clicks[*]"
    primary_keys = ["MessageID", "Recipient", "Type", "ReceivedAt"]

    count_by: int = 500

    parent_stream_type = OutboundMessageStream

    # Assume opens don't have `updated_at` incremented when outbound_messages are changed:
    ignore_parent_replication_keys = True

    schema = th.PropertiesList(
        th.Property("MessageID", th.StringType),
        th.Property("ClickLocation", th.StringType),
        th.Property("Recipient", th.StringType),
        th.Property("ReceivedAt", th.DateTimeType),
        th.Property("Tag", th.StringType),
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

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row['Details'] = orjson.dumps(row['Details']).decode('utf-8')
        row['MessageID'] = context['MessageID']
        return row
