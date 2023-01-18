"""jsonplaceholder tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from .streams import OutboundMessageStream, SingleOutboundMessageEventStream, SingleOutboundMessageOpenStream, SingleOutboundMessageClickStream


class TapPostmark(Tap):
    """Postmark API tap."""

    name = "tap-postmark"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The token to authenticate against the API service"
        ),
        th.Property(
            "start_dt",
            th.DateTimeType,
            description="The earliest record datetime to sync"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [
            OutboundMessageStream(tap=self),
            SingleOutboundMessageOpenStream(per_receiver=True, tap=self),
            # SingleOutboundMessageEventStream(tap=self),
            # SingleOutboundMessageClickStream(tap=self),
        ]


if __name__ == "__main__":
    TapPostmark.cli()
