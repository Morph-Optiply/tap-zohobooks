"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from tap_zohobooks.tap import TapZohoBooks

SAMPLE_CONFIG = {
    "access_token": "dummy",
    "refresh_token": "dummy",
    "redirect_uri": "https://callback.oauth.optiply.com",
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
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
