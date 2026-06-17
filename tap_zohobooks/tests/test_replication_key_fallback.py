"""Regression tests for Zoho records that omit incremental bookmarks."""

import datetime

from tap_zohobooks.streams import BankAccountsStream, ExpensesDetailsStream
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
    stream = BankAccountsStream(tap=_tap())
    row = {
        "account_id": "bank-1",
        "account_name": "Main bank",
        "created_time": "2026-06-17T05:00:00+0000",
    }

    processed = stream.post_process(row, context={"organization_id": "org-1"})

    assert processed["last_modified_time"] == "2026-06-17T05:00:00+0000"


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
    stream = BankAccountsStream(tap=_tap())
    row = {
        "account_id": "bank-1",
        "account_name": "Main bank",
        "created_time": "2026-06-17T05:00:00+0000",
        "last_modified_time": "2026-06-17T05:00:00+0000",
    }
    # Simulate HotGlue/catalog field selection where the bookmark is not
    # selected for output. Singer SDK strips deselected fields in place while
    # writing, then increments state from the same record.
    stream.schema["properties"].pop("last_modified_time")

    stream._write_record_message(row)

    assert row["last_modified_time"] == "2026-06-17T05:00:00+0000"
