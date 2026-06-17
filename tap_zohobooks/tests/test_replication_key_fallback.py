"""Regression tests for Zoho records that omit incremental bookmarks."""

import datetime

import pytest
import requests

from tap_zohobooks.streams import BankAccountsStream, ExchangeRatesStream, ExpensesDetailsStream, VendorPaymentsStream
from tap_zohobooks.tap import TapZohoBooks


SAMPLE_CONFIG = {
    "access_token": "dummy",
    "refresh_token": "dummy",
    "redirect_uri": "https://callback.oauth.optiply.com",
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
}


def _tap():
    return TapZohoBooks(config=SAMPLE_CONFIG)


def test_post_process_fills_missing_last_modified_time_from_created_time():
    stream = VendorPaymentsStream(tap=_tap())
    row = {
        "payment_id": "payment-1",
        "created_time": "2026-06-17T05:00:00+0000",
    }

    processed = stream.post_process(row, context={"organization_id": "org-1"})

    assert processed["last_modified_time"] == "2026-06-17T05:00:00+0000"


def test_bankaccounts_is_full_table_because_zoho_omits_bookmark():
    stream = BankAccountsStream(tap=_tap())

    assert stream.replication_key is None


def test_child_post_process_fills_missing_last_modified_time_from_parent_context():
    stream = ExpensesDetailsStream(tap=_tap())
    row = {
        "expense_id": "expense-1",
        "date": "2026-06-16",
    }
    context = {
        "expense_id": "expense-1",
        "organization_id": "org-1",
        "parent_last_modified_time": "2026-06-17T05:59:00+0000",
    }

    processed = stream.post_process(row, context=context)

    assert processed["last_modified_time"] == "2026-06-17T05:59:00+0000"


def test_record_write_does_not_strip_replication_key_needed_for_state():
    stream = VendorPaymentsStream(tap=_tap())
    row = {
        "payment_id": "payment-1",
        "created_time": "2026-06-17T05:00:00+0000",
        "last_modified_time": "2026-06-17T05:00:00+0000",
    }
    # Simulate HotGlue/catalog field selection where the bookmark is not
    # selected for output. Singer SDK strips deselected fields in place while
    # writing, then increments state from the same record.
    stream.schema["properties"].pop("last_modified_time")

    stream._write_record_message(row)

    assert row["last_modified_time"] == "2026-06-17T05:00:00+0000"


def test_exchangerates_ignores_per_currency_400_response():
    stream = ExchangeRatesStream(tap=_tap())
    response = requests.Response()
    response.status_code = 400
    response._content = b'{"code": 400, "message": "No exchange rates for currency"}'
    response.url = "https://www.zohoapis.com/books/v3/settings/currencies/currency-1/exchangerates"

    assert stream.validate_response(response) is None


def test_vendorpayments_still_raises_for_400_response():
    stream = VendorPaymentsStream(tap=_tap())
    response = requests.Response()
    response.status_code = 400
    response._content = b'{"code": 400, "message": "Bad request"}'
    response.url = "https://www.zohoapis.com/books/v3/vendorpayments"

    with pytest.raises(Exception):
        stream.validate_response(response)
