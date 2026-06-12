"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from tap_zohobooks.tap import TapZohoBooks

SAMPLE_CONFIG = {
    "access_token": "test-access-token",
    "refresh_token": "test-refresh-token",
    "client_id": "test-client-id",
    "client_secret": "test-client-secret",
    "redirect_uri": "https://example.com/oauth/callback",
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapZohoBooks, config=SAMPLE_CONFIG)
    for test in tests:
        if test.__name__ == "_test_stream_connections":
            continue
        test()


# TODO: Create additional tests as appropriate for your tap.

def test_requested_zoho_books_streams_are_discovered():
    """Verify the requested Zoho Books stream names are discoverable."""
    tap = TapZohoBooks(config=SAMPLE_CONFIG, validate_config=False)
    stream_names = {stream.name for stream in tap.discover_streams()}

    assert {
        "expenses",
        "vendorpayments",
        "banktransactions",
        "bankaccounts",
        "vendorcredits",
        "chartofaccounts",
        "exchangerates",
    }.issubset(stream_names)
