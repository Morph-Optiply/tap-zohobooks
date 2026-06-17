"""Stream type classes for tap-zohobooks."""
from datetime import datetime
import requests

from collections import OrderedDict

from typing import Any, Dict, Optional
from requests.models import Response as Response

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath
from tap_zohobooks.client import ZohoBooksStream


class OrganizationIdStream(ZohoBooksStream):
    name = "organization_id"
    path = "/organizations"
    primary_keys = ["organization_id"]
    replication_key = None
    records_jsonpath = "$.organizations[*]"
    first_run = True

    schema = th.PropertiesList(
        th.Property("organization_id", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "organization_id": record["organization_id"],
        }

    def parse_response(self, response):
        if not self.config.get("organization_id"):
            yield from super().parse_response(response)

        for item in super().parse_response(response):
            if item["organization_id"] == str(self.config.get("organization_id")):
                yield item


class JournalsIdStream(ZohoBooksStream):
    name = "journals_id"
    path = "/journals"
    primary_keys = ["journal_id"]
    replication_key = None
    records_jsonpath: str = "$.journals[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("journal_id", th.StringType),
        th.Property("journal_date", th.StringType),
        th.Property("entry_number", th.StringType),
        th.Property("reference_number", th.StringType),
        th.Property("currency_id", th.StringType),
        th.Property("status", th.StringType),
        th.Property("notes", th.StringType),
        th.Property("journal_type", th.StringType),
        th.Property("entity_type", th.StringType),
        th.Property("total", th.NumberType),
        th.Property("bcy_total", th.NumberType),
        th.Property("created_by_id", th.StringType),
        th.Property("created_by_name", th.StringType),
        th.Property("documents", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "organization_id": context.get("organization_id"),
            "journal_id": record["journal_id"],
        }

    def get_url_params(self, context, next_page_token) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        # remove last_modified_time from params, as it's returning journals with date equal to last_modified_time instead of greater than it
        start_date = params.pop("last_modified_time", None)
        if start_date:
            # use only the date part of start_date from config
            params["date_after"] = start_date.split("T")[0]
        return params


class JournalStream(ZohoBooksStream):
    name = "journals"
    path = "/journals/{journal_id}"
    primary_keys = ["journal_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.journal"
    parent_stream_type = JournalsIdStream
    # no need to set the org_id parent because it's a parent to JournalsIdStream
    # so all journal id returned will be already org_id filtered

    line_object_schema = th.ObjectType(
        th.Property("line_id", th.StringType),
        th.Property("account_id", th.StringType),
        th.Property("customer_id", th.StringType),
        th.Property("customer_name", th.StringType),
        th.Property("account_name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("debit_or_credit", th.StringType),
        th.Property("tax_exemption_id", th.StringType),
        th.Property("tax_exemption_type", th.StringType),
        th.Property("tax_exemption_code", th.StringType),
        th.Property("tax_authority_id", th.StringType),
        th.Property("tax_authority_name", th.StringType),
        th.Property("tax_id", th.StringType),
        th.Property("tax_name", th.StringType),
        th.Property("tax_type", th.StringType),
        th.Property("tax_percentage", th.StringType),
        th.Property("amount", th.NumberType),
        th.Property("bcy_amount", th.NumberType),
        th.Property("acquisition_vat_id", th.StringType),
        th.Property("acquisition_vat_name", th.StringType),
        th.Property("acquisition_vat_percentage", th.StringType),
        th.Property("acquisition_vat_amount", th.StringType),
        th.Property("reverse_charge_vat_id", th.StringType),
        th.Property("reverse_charge_vat_name", th.StringType),
        th.Property("reverse_charge_vat_percentage", th.StringType),
        th.Property("reverse_charge_vat_amount", th.StringType),
        th.Property(
            "tags",
            th.ArrayType(
                th.ObjectType(
                    th.Property("is_tag_mandatory", th.BooleanType),
                    th.Property("tag_id", th.StringType),
                    th.Property("tag_name", th.StringType),
                    th.Property("tag_option_id", th.StringType),
                    th.Property("tag_option_name", th.StringType),
                )
            ),
        ),
        th.Property("project_id", th.StringType),
        th.Property("project_name", th.StringType),
    )

    schema = th.PropertiesList(
        th.Property("journal_id", th.StringType),
        th.Property("entry_number", th.StringType),
        th.Property("reference_number", th.StringType),
        th.Property("notes", th.StringType),
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("currency_symbol", th.StringType),
        th.Property("exchange_rate", th.NumberType),
        th.Property("journal_date", th.StringType),
        th.Property("journal_type", th.StringType),
        th.Property("vat_treatment", th.StringType),
        th.Property("product_type", th.StringType),
        th.Property("include_in_vat_return", th.BooleanType),
        th.Property("is_bas_adjustment", th.BooleanType),
        th.Property("line_items", th.ArrayType(line_object_schema)),
        th.Property("line_item_total", th.NumberType),
        th.Property("total", th.NumberType),
        th.Property("bcy_total", th.NumberType),
        th.Property("price_precision", th.NumberType),
        th.Property(
            "taxes",
            th.ArrayType(
                th.ObjectType(
                    th.Property("tax_name", th.StringType),
                    th.Property("tax_amount", th.IntegerType),
                    th.Property("debit_or_credit", th.StringType),
                    th.Property("tax_account", th.BooleanType),
                )
            ),
        ),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("status", th.StringType),
        th.Property("custom_fields", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()


class ChartOfAccountsStream(ZohoBooksStream):
    name = "chart_of_accounts"
    path = "/chartofaccounts?filter_by=AccountType.All"
    primary_keys = ["account_id"]
    records_jsonpath: str = "$.chartofaccounts[*]"
    parent_stream_type = OrganizationIdStream
    ignore_config_start_date = True

    schema = th.PropertiesList(
        th.Property("account_id", th.StringType),
        th.Property("account_name", th.StringType),
        th.Property("account_code", th.StringType),
        th.Property("account_type", th.StringType),
        th.Property("is_user_created", th.BooleanType),
        th.Property("is_system_account", th.BooleanType),
        th.Property("is_standalone_account", th.BooleanType),
        th.Property("is_active", th.BooleanType),
        th.Property("can_show_in_ze", th.BooleanType),
        th.Property("is_involved_in_transaction", th.BooleanType),
        th.Property("current_balance", th.StringType),
        th.Property("parent_account_id", th.StringType),
        th.Property("parent_account_name", th.StringType),
        th.Property("depth", th.IntegerType),
        th.Property("has_attachment", th.BooleanType),
        th.Property("is_child_present", th.BooleanType),
        th.Property("child_count", th.StringType),
        th.Property("documents", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_modified_time", th.DateTimeType),
    ).to_dict()

    def get_child_context(self, record, context) -> dict:
        return {
            "account_id": record["account_id"],
            "organization_id": context.get("organization_id"),
        }


class ChartOfAccountsAliasStream(ChartOfAccountsStream):
    """Zoho endpoint-name alias for chart of accounts."""

    name = "chartofaccounts"


class ItemsStream(ZohoBooksStream):
    name = "items"
    path = "/items"
    primary_keys = ["item_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.items[*]"
    parent_stream_type = OrganizationIdStream
    use_item_details = False

    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("line_items", th.CustomType({"type": ["array", "string"]})),
        th.Property("rate", th.NumberType),
        th.Property("description", th.StringType),
        th.Property("tax_id", th.StringType),
        th.Property("item_id", th.StringType),
        th.Property("item_name", th.StringType),
        th.Property("crm_owner_id", th.StringType),
        th.Property("unit", th.StringType),
        th.Property("unitkey_code", th.StringType),
        th.Property("status", th.StringType),
        th.Property("source", th.StringType),
        th.Property("is_linked_with_zohocrm", th.BooleanType),
        th.Property("is_fulfillable", th.BooleanType),
        th.Property("zcrm_product_id", th.StringType),
        th.Property("is_taxable", th.BooleanType),
        th.Property("tax_name", th.StringType),
        th.Property("tax_percentage", th.StringType),
        th.Property("sales_rate", th.NumberType),
        th.Property("unit_id", th.StringType),
        th.Property("tax_exemption_id", th.StringType),
        th.Property("purchase_account_id", th.StringType),
        th.Property("purchase_account_name", th.StringType),
        th.Property("purchase_tax_rule_id", th.StringType),
        th.Property("purchase_description", th.StringType),
        th.Property("purchase_rate", th.StringType),
        th.Property("sales_tax_rule_id", th.StringType),
        th.Property("avatax_tax_code", th.StringType),
        th.Property("avatax_use_code", th.StringType),
        th.Property("account_id", th.StringType),
        th.Property("account_name", th.StringType),
        th.Property("item_type", th.StringType),
        th.Property("product_type", th.StringType),
        th.Property("hsn_or_sac", th.StringType),
        th.Property("sat_item_key_code", th.StringType),
        th.Property("stock_on_hand", th.NumberType),
        th.Property("has_attachment", th.BooleanType),
        th.Property("available_stock", th.NumberType),
        th.Property("actual_available_stock", th.NumberType),
        th.Property("committed_stock", th.NumberType),
        th.Property("actual_committed_stock", th.NumberType),
        th.Property("available_for_sale_stock", th.NumberType),
        th.Property("actual_available_for_sale_stock", th.NumberType),
        th.Property("sku", th.StringType),
        th.Property("reorder_level", th.NumberType),
        th.Property("initial_stock", th.StringType),
        th.Property("brand", th.StringType),
        th.Property("initial_stock_rate", th.StringType),
        th.Property("image_name", th.StringType),
        th.Property("image_type", th.StringType),
        th.Property("image_document_id", th.StringType),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("show_in_storefront", th.BooleanType),
        th.Property("vendor_id", th.StringType),
        th.Property("vendor_name", th.StringType),
        th.Property("tax_type", th.StringType),
        th.Property("associated_template_id", th.StringType),
        th.Property("inventory_account_id", th.StringType),
        th.Property("inventory_account_name", th.StringType),
        th.Property("is_combo_product", th.BooleanType),
        th.Property("manufacturer", th.StringType),
        th.Property("pricebook_rate", th.NumberType),
        th.Property(
            "sales_channels", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "price_brackets", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property("package_details", th.ObjectType(
            th.Property("length", th.NumberType),
            th.Property("width", th.NumberType),
            th.Property("height", th.NumberType),
            th.Property("weight", th.NumberType),
            th.Property("weight_unit", th.StringType),
            th.Property("dimension_unit", th.StringType),
        )),
        th.Property(
            "tags", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "item_tax_preferences", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "custom_fields", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "preferred_vendors", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property("warehouses", th.CustomType({"type": ["array", "string"]})),
        th.Property("documents", th.CustomType({"type": ["array", "string"]})),
        th.Property("custom_field_hash", th.CustomType({"type": ["object", "string"]})),
        th.Property("minimum_order_quantity", th.StringType),
        th.Property("maximum_order_quantity", th.StringType),
        th.Property("offline_created_date_with_time", th.DateTimeType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "item_id": record["item_id"],
            "organization_id": context.get("organization_id"),
        }

    def parse_response(self, response):
        """
        This function works getting all of the data from the Stream
        and adds item details to each item of the stream available.

        It does it using the /itemdetails endpoint, chunking with
        100 ids per request.
        """
        if not self.config.get("use_item_details", False):
            yield from super().parse_response(response)
            return

        # gets organization id from the url
        org_id = response.url.replace(
            self.url_base + "/items?", ""
        ).split("&")[0].replace("organization_id=", "")
        details_base_url = self.url_base + "/itemdetails"
        records = list(extract_jsonpath(self.records_jsonpath, input=response.json()))

        # get all item ids from the records and create a dict with it
        record_ids = OrderedDict((record.get("item_id"), record) for record in records)

        # chunks the request, preserving API quota
        for chunk in self._divide_chunks(list(record_ids.keys())):
            params = {
                "organization_id": org_id,
                "item_ids": ",".join(chunk)
            }

            req = requests.Request(
                "GET",
                details_base_url,
                params=params,
                headers=self.authenticator.auth_headers
            )
            detail_response = self._request(req.prepare())

            item_details = extract_jsonpath(self.records_jsonpath, input=detail_response.json())

            for item_detail in item_details:
                for key in [
                    key for key in item_detail.keys()
                    if key not in record_ids[item_detail["item_id"]]
                ]:
                    # Adds data from missing keys
                    record_ids[item_detail["item_id"]][key] = item_detail[key]

                yield record_ids[item_detail["item_id"]]


class ItemsDetailStream(ZohoBooksStream):
    name = "item_details"
    path = "/items/{item_id}"
    primary_keys = ["item_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.item[*]"
    parent_stream_type = ItemsStream

    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("rate", th.NumberType),
        th.Property("description", th.StringType),
        th.Property("tax_id", th.StringType),
        th.Property("item_id", th.StringType),
        th.Property("vendor_id", th.StringType),
        th.Property("item_name", th.StringType),
        th.Property("unit", th.StringType),
        th.Property("unitkey_code", th.StringType),
        th.Property("status", th.StringType),
        th.Property("source", th.StringType),
        th.Property("is_linked_with_zohocrm", th.BooleanType),
        th.Property("is_fulfillable", th.BooleanType),
        th.Property("is_default_tax_applied", th.BooleanType),
        th.Property("crm_owner_id", th.StringType),
        th.Property("zcrm_product_id", th.StringType),
        th.Property("is_taxable", th.BooleanType),
        th.Property("tax_name", th.StringType),
        th.Property("tax_percentage", th.StringType),
        th.Property("tax_type", th.StringType),
        th.Property("tax_exemption_id", th.StringType),
        th.Property("purchase_account_id", th.StringType),
        th.Property("purchase_account_name", th.StringType),
        th.Property("purchase_tax_rule_id", th.StringType),
        th.Property("purchase_description", th.StringType),
        th.Property("purchase_rate", th.StringType),
        th.Property("sales_tax_rule_id", th.StringType),
        th.Property("avatax_tax_code", th.StringType),
        th.Property("avatax_use_code", th.StringType),
        th.Property("account_id", th.StringType),
        th.Property("account_name", th.StringType),
        th.Property("inventory_account_id", th.StringType),
        th.Property("item_type", th.StringType),
        th.Property("product_type", th.StringType),
        th.Property("hsn_or_sac", th.StringType),
        th.Property("sat_item_key_code", th.StringType),
        th.Property("stock_on_hand", th.NumberType),
        th.Property("has_attachment", th.BooleanType),
        th.Property("available_stock", th.NumberType),
        th.Property("actual_available_stock", th.NumberType),
        th.Property("committed_stock", th.NumberType),
        th.Property("actual_committed_stock", th.NumberType),
        th.Property("available_for_sale_stock", th.NumberType),
        th.Property("actual_available_for_sale_stock", th.NumberType),
        th.Property("sku", th.StringType),
        th.Property("reorder_level", th.StringType),
        th.Property("initial_stock", th.StringType),
        th.Property("brand", th.StringType),
        th.Property("associated_template_id", th.StringType),
        th.Property("initial_stock_rate", th.StringType),
        th.Property("image_name", th.StringType),
        th.Property("image_type", th.StringType),
        th.Property("created_at", th.DateType),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("show_in_storefront", th.BooleanType),
        th.Property("vendor_id", th.StringType),
        th.Property("vendor_name", th.StringType),
        th.Property("tax_type", th.StringType),
        th.Property("inventory_account_id", th.StringType),
        th.Property("inventory_account_name", th.StringType),
        th.Property("pricebook_rate", th.NumberType),
        th.Property("sales_rate", th.NumberType),
        th.Property("unit_id", th.StringType),
        th.Property("zcrm_product_id", th.StringType),
        th.Property("reorder_level", th.StringType),
        th.Property(
            "sales_channels", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property("package_details", th.ObjectType(
            th.Property("length", th.NumberType),
            th.Property("width", th.NumberType),
            th.Property("height", th.NumberType),
            th.Property("weight", th.NumberType),
            th.Property("weight_unit", th.StringType),
            th.Property("dimension_unit", th.StringType),
        )),
        th.Property(
            "tags", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "item_tax_preferences", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "custom_fields", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property("manufacturer", th.StringType),
        th.Property("minimum_order_quantity", th.StringType),
        th.Property("maximum_order_quantity", th.StringType),
        th.Property("offline_created_date_with_time", th.DateTimeType),
        th.Property(
            "preferred_vendors", th.ArrayType(th.ObjectType(
                th.Property("is_primary", th.BooleanType),
                th.Property("item_price", th.NumberType),
                th.Property("item_stock", th.NumberType),
                th.Property("last_modified_time", th.DateTimeType),
                th.Property("vendor_id", th.StringType),
                th.Property("vendor_name", th.StringType),
            ))
        ),
        th.Property("warehouses", th.CustomType({"type": ["array", "string"]})),
        th.Property("documents", th.CustomType({"type": ["array", "string"]})),
        th.Property("custom_field_hash", th.CustomType({"type": ["object", "string"]})),
    ).to_dict()


class InvoicesStream(ZohoBooksStream):
    name = "invoices"
    path = "/invoices"
    primary_keys = ["invoice_id"]
    replication_key = "last_modified_time"
    records_jsonpath = "$.invoices[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("invoice_id", th.StringType),
        th.Property("ach_payment_initiated", th.BooleanType),
        th.Property("zcrm_potential_id", th.StringType),
        th.Property("zcrm_potential_name", th.StringType),
        th.Property("customer_name", th.StringType),
        th.Property("customer_id", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("status", th.StringType),
        th.Property("color_code", th.StringType),
        th.Property("current_sub_status_id", th.StringType),
        th.Property("current_sub_status", th.StringType),
        th.Property("invoice_number", th.StringType),
        th.Property("reference_number", th.StringType),
        th.Property("date", th.StringType),
        th.Property("due_date", th.StringType),
        th.Property("due_days", th.StringType),
        th.Property("currency_id", th.StringType),
        th.Property("schedule_time", th.StringType),
        th.Property("email", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("currency_symbol", th.StringType),
        th.Property("template_type", th.StringType),
        th.Property("is_viewed_by_client", th.BooleanType),
        th.Property("has_attachment", th.BooleanType),
        th.Property("client_viewed_time", th.StringType),
        th.Property("invoice_url", th.StringType),
        th.Property("project_name", th.StringType),
        th.Property(
            "billing_address",
            th.ObjectType(
                th.Property("address", th.StringType),
                th.Property("street2", th.StringType),
                th.Property("city", th.StringType),
                th.Property("state", th.StringType),
                th.Property("zipcode", th.StringType),
                th.Property("country", th.StringType),
                th.Property("phone", th.StringType),
                th.Property("fax", th.StringType),
                th.Property("attention", th.StringType),
            ),
        ),
        th.Property(
            "shipping_address",
            th.ObjectType(
                th.Property("address", th.StringType),
                th.Property("street2", th.StringType),
                th.Property("city", th.StringType),
                th.Property("state", th.StringType),
                th.Property("zipcode", th.StringType),
                th.Property("country", th.StringType),
                th.Property("phone", th.StringType),
                th.Property("fax", th.StringType),
                th.Property("attention", th.StringType),
            ),
        ),
        th.Property("country", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("created_by", th.StringType),
        th.Property("updated_time", th.StringType),
        th.Property("transaction_type", th.StringType),
        th.Property("total", th.StringType),
        th.Property("balance", th.StringType),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("is_emailed", th.BooleanType),
        th.Property("is_viewed_in_mail", th.BooleanType),
        th.Property("mail_first_viewed_time", th.StringType),
        th.Property("mail_last_viewed_time", th.StringType),
        th.Property("reminders_sent", th.IntegerType),
        th.Property("last_reminder_sent_date", th.StringType),
        th.Property("payment_expected_date", th.StringType),
        th.Property("last_payment_date", th.StringType),
        th.Property("custom_fields", th.CustomType({"type": ["array", "string"]})),
        th.Property("custom_field_hash", th.CustomType({"type": ["array", "object"]})),
        th.Property("template_id", th.StringType),
        th.Property("documents", th.StringType),
        th.Property("salesperson_id", th.StringType),
        th.Property("salesperson_name", th.StringType),
        th.Property("shipping_charge", th.StringType),
        th.Property("adjustment", th.NumberType),
        th.Property("write_off_amount", th.NumberType),
        th.Property("exchange_rate", th.NumberType),
    ).to_dict()

    def get_child_context(self, record, context) -> dict:
        return {
            "invoice_id": record["invoice_id"],
            "organization_id": context.get("organization_id"),
        }
    
    def get_records(self, context):
        """
        Need to overwrite the get_records because zoho sometimes returns duplicates
        for each unique invoice_url you generate
        """
        seen_ids = set()
        for record in self.request_records(context):
            transformed_record = self.post_process(record, context)

            if transformed_record is None:
                # Record filtered out during post_process()
                continue
            invoice_id = transformed_record.get("invoice_id")
            if invoice_id in seen_ids:
                continue
            seen_ids.add(invoice_id)
            yield transformed_record


class InvoiceDetailsStream(ZohoBooksStream):
    name = "invoice_details"
    path = "/invoices/{invoice_id}"
    primary_keys = ["invoice_id"]
    replication_key = "last_modified_time"
    records_jsonpath = "$.invoice[*]"
    parent_stream_type = InvoicesStream

    schema = th.PropertiesList(
        th.Property("invoice_id", th.StringType),
        th.Property("invoice_number", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("due_date", th.DateTimeType),
        th.Property("offline_created_date_with_time", th.StringType),
        th.Property("customer_id", th.StringType),
        th.Property("customer_name", th.StringType),
        th.Property(
            "customer_custom_fields",
            th.ArrayType(
                th.ObjectType(
                    th.Property("field_id", th.StringType),
                    th.Property("customfield_id", th.StringType),
                    th.Property("show_in_store", th.BooleanType),
                    th.Property("show_in_portal", th.BooleanType),
                    th.Property("is_active", th.BooleanType),
                    th.Property("index", th.IntegerType),
                    th.Property("label", th.StringType),
                    th.Property("show_on_pdf", th.BooleanType),
                    th.Property("edit_on_portal", th.BooleanType),
                    th.Property("edit_on_store", th.BooleanType),
                    th.Property("api_name", th.StringType),
                    th.Property("show_in_all_pdf", th.BooleanType),
                    th.Property("value_formatted", th.StringType),
                    th.Property("search_entity", th.StringType),
                    th.Property("data_type", th.StringType),
                    th.Property("placeholder", th.StringType),
                    th.Property("value", th.StringType),
                    th.Property("is_dependent_field", th.BooleanType),
                )
            ),
        ),
        th.Property("cf_iban", th.StringType),
        th.Property("cf_iban_unformatted", th.StringType),
        th.Property("cf_kvk", th.StringType),
        th.Property("cf_kvk_unformatted", th.StringType),
        th.Property("cf_accountmanager", th.StringType),
        th.Property("cf_accountmanager_unformatted", th.StringType),
        th.Property(
            "customer_custom_field_hash",
            th.ObjectType(
                th.Property("cf_iban", th.StringType),
                th.Property("cf_iban_unformatted", th.StringType),
                th.Property("cf_kvk", th.StringType),
                th.Property("cf_kvk_unformatted", th.StringType),
                th.Property("cf_accountmanager", th.StringType),
                th.Property("cf_accountmanager_unformatted", th.StringType),
            ),
        ),
        th.Property("email", th.StringType),
        th.Property("currency_id", th.StringType),
        th.Property("invoice_source", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("currency_symbol", th.StringType),
        th.Property("currency_name_formatted", th.StringType),
        th.Property("status", th.StringType),
        th.Property(
            "custom_fields",
            th.ArrayType(
                th.ObjectType(
                    th.Property("field_id", th.StringType),
                    th.Property("customfield_id", th.StringType),
                    th.Property("show_in_store", th.BooleanType),
                    th.Property("show_in_portal", th.BooleanType),
                    th.Property("is_active", th.BooleanType),
                    th.Property("index", th.IntegerType),
                    th.Property("label", th.StringType),
                    th.Property("show_on_pdf", th.BooleanType),
                    th.Property("edit_on_portal", th.BooleanType),
                    th.Property("edit_on_store", th.BooleanType),
                    th.Property("api_name", th.StringType),
                    th.Property("show_in_all_pdf", th.BooleanType),
                    th.Property("value_formatted", th.StringType),
                    th.Property("search_entity", th.StringType),
                    th.Property("data_type", th.StringType),
                    th.Property("placeholder", th.StringType),
                    th.Property("value", th.StringType),
                    th.Property("is_dependent_field", th.BooleanType),
                )
            ),
        ),
        th.Property(
            "custom_field_hash",
            th.ObjectType(
                th.Property("cf_projectnummer", th.StringType),
                th.Property("cf_projectnummer_unformatted", th.StringType),
                th.Property("cf_marge", th.StringType),
                th.Property("cf_marge_unformatted", th.NumberType),
            ),
        ),
        th.Property("recurring_invoice_id", th.StringType),
        th.Property("pricebook_id", th.StringType),
        th.Property("pricebook_name", th.StringType),
        th.Property("payment_terms", th.IntegerType),
        th.Property("payment_terms_label", th.StringType),
        th.Property("payment_reminder_enabled", th.BooleanType),
        th.Property("payment_made", th.NumberType),
        th.Property("zcrm_potential_id", th.StringType),
        th.Property("zcrm_potential_name", th.StringType),
        th.Property("reference_number", th.StringType),
        th.Property("is_inventory_valuation_pending", th.BooleanType),
        th.Property("lock_details", th.ObjectType()),
        th.Property("line_items", th.CustomType({"type": ["array", "string"]})),
        th.Property("exchange_rate", th.NumberType),
        th.Property("is_autobill_enabled", th.BooleanType),
        th.Property("inprocess_transaction_present", th.BooleanType),
        th.Property("allow_partial_payments", th.BooleanType),
        th.Property("price_precision", th.IntegerType),
        th.Property("sub_total", th.NumberType),
        th.Property("tax_total", th.NumberType),
        th.Property("discount_total", th.NumberType),
        th.Property("discount_percent", th.StringType),
        th.Property("discount", th.NumberType),
        th.Property("discount_applied_on_amount", th.NumberType),
        th.Property("discount_type", th.StringType),
        th.Property("tax_override_preference", th.StringType),
        th.Property("tds_override_preference", th.StringType),
        th.Property("is_discount_before_tax", th.BooleanType),
        th.Property("adjustment", th.NumberType),
        th.Property("adjustment_description", th.StringType),
        th.Property("shipping_charge_tax_id", th.StringType),
        th.Property("shipping_charge_tax_name", th.StringType),
        th.Property("shipping_charge_tax_type", th.StringType),
        th.Property("shipping_charge_tax_percentage", th.StringType),
        th.Property("shipping_charge_tax", th.StringType),
        th.Property("bcy_shipping_charge_tax", th.StringType),
        th.Property("shipping_charge_exclusive_of_tax", th.NumberType),
        th.Property("shipping_charge_inclusive_of_tax", th.NumberType),
        th.Property("shipping_charge_tax_formatted", th.StringType),
        th.Property("shipping_charge_exclusive_of_tax_formatted", th.StringType),
        th.Property("shipping_charge_inclusive_of_tax_formatted", th.StringType),
        th.Property("shipping_charge", th.NumberType),
        th.Property("bcy_shipping_charge", th.NumberType),
        th.Property("bcy_adjustment", th.NumberType),
        th.Property("bcy_sub_total", th.NumberType),
        th.Property("bcy_discount_total", th.NumberType),
        th.Property("bcy_tax_total", th.NumberType),
        th.Property("bcy_total", th.NumberType),
        th.Property("is_reverse_charge_applied", th.BooleanType),
        th.Property("total", th.NumberType),
        th.Property("balance", th.NumberType),
        th.Property("write_off_amount", th.NumberType),
        th.Property("roundoff_value", th.NumberType),
        th.Property("transaction_rounding_type", th.StringType),
        th.Property("is_inclusive_tax", th.BooleanType),
        th.Property("sub_total_inclusive_of_tax", th.NumberType),
        th.Property("contact_category", th.StringType),
        th.Property("tax_rounding", th.StringType),
        th.Property(
            "taxes",
            th.ArrayType(
                th.ObjectType(
                    th.Property("tax_name", th.StringType),
                    th.Property("tax_amount", th.NumberType),
                )
            ),
        ),
        th.Property("reverse_charge_tax_total", th.NumberType),
        th.Property("tds_calculation_type", th.StringType),
        th.Property("can_send_invoice_sms", th.BooleanType),
        th.Property("payment_expected_date", th.StringType),
        th.Property("payment_discount", th.NumberType),
        th.Property("stop_reminder_until_payment_expected_date", th.BooleanType),
        th.Property("last_payment_date", th.StringType),
        th.Property("ach_supported", th.BooleanType),
        th.Property("ach_payment_initiated", th.BooleanType),
        th.Property(
            "payment_options",
            th.ObjectType(
                th.Property(
                    "payment_gateways",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("configured", th.BooleanType),
                            th.Property("can_show_billing_address", th.BooleanType),
                            th.Property("is_bank_account_applicable", th.BooleanType),
                            th.Property("can_pay_using_new_card", th.BooleanType),
                            th.Property("gateway_name", th.StringType),
                        )
                    ),
                ),
            ),
        ),
        th.Property("reader_offline_payment_initiated", th.BooleanType),
        th.Property("attachment_name", th.StringType),
        th.Property("computation_type", th.StringType),
        th.Property("merchant_id", th.StringType),
        th.Property("merchant_name", th.StringType),
        th.Property("ecomm_operator_id", th.StringType),
        th.Property("ecomm_operator_name", th.StringType),
        th.Property("salesorder_id", th.StringType),
        th.Property("salesorder_number", th.StringType),
        th.Property(
            "salesorders",
            th.ArrayType(
                th.ObjectType(
                    th.Property("salesorder_id", th.StringType),
                    th.Property("salesorder_number", th.StringType),
                    th.Property("reference_number", th.StringType),
                    th.Property("salesorder_order_status", th.StringType),
                    th.Property("total", th.NumberType),
                    th.Property("sub_total", th.NumberType),
                    th.Property("date", th.DateTimeType),
                    th.Property("shipment_date", th.DateTimeType),
                )
            ),
        ),
        th.Property("signed_document_id", th.StringType),
        th.Property("is_digitally_signed", th.BooleanType),
        th.Property("is_edited_after_sign", th.BooleanType),
        th.Property("is_signature_enabled_in_template", th.BooleanType),
        th.Property(
            "contact_persons_details",
            th.ArrayType(
                th.ObjectType(
                    th.Property("contact_person_id", th.StringType),
                    th.Property("first_name", th.StringType),
                    th.Property("last_name", th.StringType),
                    th.Property("email", th.StringType),
                    th.Property("phone", th.StringType),
                    th.Property("mobile", th.StringType),
                    th.Property("is_primary_contact", th.BooleanType),
                    th.Property("photo_url", th.StringType),
                )
            ),
        ),
        th.Property(
            "contact",
            th.ObjectType(
                th.Property("customer_balance", th.NumberType),
                th.Property("credit_limit", th.NumberType),
                th.Property("unused_customer_credits", th.NumberType),
                th.Property("is_credit_limit_migration_completed", th.BooleanType),
            ),
        ),
        th.Property("salesperson_id", th.StringType),
        th.Property("salesperson_name", th.StringType),
        th.Property("is_emailed", th.BooleanType),
        th.Property("reminders_sent", th.IntegerType),
        th.Property("last_reminder_sent_date", th.StringType),
        th.Property("next_reminder_date_formatted", th.StringType),
        th.Property("is_viewed_by_client", th.BooleanType),
        th.Property("client_viewed_time", th.StringType),
        th.Property("is_viewed_in_mail", th.BooleanType),
        th.Property("mail_first_viewed_time", th.DateTimeType),
        th.Property("mail_last_viewed_time", th.DateTimeType),
        th.Property("submitter_id", th.StringType),
        th.Property("approver_id", th.StringType),
        th.Property("submitted_date", th.StringType),
        th.Property("submitted_by", th.StringType),
        th.Property("submitted_by_name", th.StringType),
        th.Property("submitted_by_email", th.StringType),
        th.Property("submitted_by_photo_url", th.StringType),
        th.Property("template_id", th.StringType),
        th.Property("template_name", th.StringType),
        th.Property("template_type", th.StringType),
        th.Property("notes", th.StringType),
        th.Property("terms", th.StringType),
        th.Property(
            "billing_address",
            th.ObjectType(
                th.Property("street", th.StringType),
                th.Property("address", th.StringType),
                th.Property("street2", th.StringType),
                th.Property("city", th.StringType),
                th.Property("state", th.StringType),
                th.Property("zip", th.StringType),
                th.Property("country", th.StringType),
                th.Property("fax", th.StringType),
                th.Property("phone", th.StringType),
                th.Property("attention", th.StringType),
            ),
        ),
        th.Property(
            "shipping_address",
            th.ObjectType(
                th.Property("street", th.StringType),
                th.Property("address", th.StringType),
                th.Property("street2", th.StringType),
                th.Property("city", th.StringType),
                th.Property("state", th.StringType),
                th.Property("zip", th.StringType),
                th.Property("country", th.StringType),
                th.Property("fax", th.StringType),
                th.Property("phone", th.StringType),
                th.Property("attention", th.StringType),
            ),
        ),
        th.Property("invoice_url", th.StringType),
        th.Property("subject_content", th.StringType),
        th.Property("can_send_in_mail", th.BooleanType),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("created_date", th.DateTimeType),
        th.Property("created_by_id", th.StringType),
        th.Property("created_by_name", th.StringType),
        th.Property("last_modified_by_id", th.StringType),
        th.Property("page_width", th.StringType),
        th.Property("page_height", th.StringType),
        th.Property("orientation", th.StringType),
        th.Property("is_backorder", th.BooleanType),
        th.Property("sales_channel", th.StringType),
        th.Property("type", th.StringType),
        th.Property("color_code", th.StringType),
        th.Property("current_sub_status_id", th.StringType),
        th.Property("current_sub_status", th.StringType),
        th.Property("reason_for_debit_note", th.StringType),
        th.Property("estimate_id", th.StringType),
        th.Property("is_client_review_settings_enabled", th.BooleanType),
        th.Property("unused_retainer_payments", th.NumberType),
        th.Property("credits_applied", th.NumberType),
        th.Property("tax_amount_withheld", th.NumberType),
        th.Property("schedule_time", th.StringType),
        th.Property("no_of_copies", th.IntegerType),
        th.Property("show_no_of_copies", th.BooleanType),
        th.Property(
            "customer_default_billing_address",
            th.ObjectType(
                th.Property("zip", th.StringType),
                th.Property("country", th.StringType),
                th.Property("address", th.StringType),
                th.Property("city", th.StringType),
                th.Property("phone", th.StringType),
                th.Property("street2", th.StringType),
                th.Property("state", th.StringType),
                th.Property("fax", th.StringType),
                th.Property("state_code", th.StringType),
            ),
        ),
        th.Property(
            "reference_invoice",
            th.ObjectType(
                th.Property("reference_invoice_id", th.StringType),
            ),
        ),
        th.Property("includes_package_tracking_info", th.BooleanType),
    ).to_dict()


class ContactsStream(ZohoBooksStream):
    name = "contacts"
    path = "/contacts"
    primary_keys = ["contact_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.contacts[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("contact_id", th.StringType),
        th.Property("contact_name", th.StringType),
        th.Property("customer_name", th.StringType),
        th.Property("vendor_name", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("website", th.StringType),
        th.Property("language_code", th.StringType),
        th.Property("language_code_formatted", th.StringType),
        th.Property("contact_type", th.StringType),
        th.Property("contact_type_formatted", th.StringType),
        th.Property("status", th.StringType),
        th.Property("customer_sub_type", th.StringType),
        th.Property("source", th.StringType),
        th.Property("is_linked_with_zohocrm", th.BooleanType),
        th.Property("payment_terms", th.IntegerType),
        th.Property("payment_terms_label", th.StringType),
        th.Property("currency_id", th.StringType),
        th.Property("twitter", th.StringType),
        th.Property("facebook", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("outstanding_receivable_amount", th.NumberType),
        th.Property("outstanding_receivable_amount_bcy", th.NumberType),
        th.Property("outstanding_payable_amount", th.NumberType),
        th.Property("outstanding_payable_amount_bcy", th.NumberType),
        th.Property("unused_credits_receivable_amount", th.NumberType),
        th.Property("unused_credits_receivable_amount_bcy", th.NumberType),
        th.Property("unused_credits_payable_amount", th.NumberType),
        th.Property("unused_credits_payable_amount_bcy", th.NumberType),
        th.Property("first_name", th.StringType),
        th.Property("last_name", th.StringType),
        th.Property("email", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("mobile", th.StringType),
        th.Property("portal_status", th.StringType),
        th.Property("track_1099", th.BooleanType),
        th.Property("created_time", th.DateTimeType),
        th.Property("created_time_formatted", th.DateTimeType),
        th.Property("last_modified_time", th.StringType),
        th.Property("last_modified_time_formatted", th.StringType),
        th.Property("custom_fields", th.CustomType({"type": ["array", "string"]})),
        th.Property("custom_field_hash", th.CustomType({"type": ["array", "object"]})),
        th.Property("ach_supported", th.BooleanType),
        th.Property("has_attachment", th.BooleanType),
    ).to_dict()


class BillsStream(ZohoBooksStream):
    name = "bills"
    path = "/bills"
    primary_keys = ["bill_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.bills[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("bill_id", th.StringType),
        th.Property("vendor_id", th.StringType),
        th.Property("vendor_id", th.StringType),
        th.Property("vendor_name", th.StringType),
        th.Property("status", th.StringType),
        th.Property("bill_number", th.StringType),
        th.Property("reference_number", th.StringType),
        th.Property("date", th.DateType),
        th.Property("due_date", th.DateType),
        th.Property("due_days", th.StringType),
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("price_precision", th.NumberType),
        th.Property("exchange_rate", th.NumberType),
        th.Property("total", th.NumberType),
        th.Property("balance", th.NumberType),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("attachment_name", th.StringType),
        th.Property("has_attachment", th.BooleanType),
        th.Property("is_tds_applied", th.BooleanType),
        th.Property("is_abn_quoted", th.StringType),
    ).to_dict()

    def get_child_context(self, record, context):
        return {
            "bill_id": record["bill_id"],
            "organization_id": context.get("organization_id"),
        }


class BillsDetailsStream(ZohoBooksStream):
    name = "bills_details"
    path = "/bills/{bill_id}"
    primary_keys = ["bill_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.bill[*]"
    parent_stream_type = BillsStream

    schema =schema = th.PropertiesList(
        th.Property("bill_id", th.StringType),
        th.Property("vendor_id", th.StringType),
        th.Property("vendor_name", th.StringType),
        th.Property("source", th.StringType),
        th.Property("contact_category", th.StringType),
        th.Property("unused_credits_payable_amount", th.NumberType),
        th.Property("status", th.StringType),
        th.Property("color_code", th.StringType),
        th.Property("current_sub_status_id", th.StringType),
        th.Property("current_sub_status", th.StringType),
        th.Property("bill_number", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("due_date", th.DateTimeType),
        th.Property("discount_setting", th.StringType),
        th.Property("tds_calculation_type", th.StringType),
        th.Property("payment_terms", th.IntegerType),
        th.Property("payment_terms_label", th.StringType),
        th.Property("payment_expected_date", th.StringType),
        th.Property("reference_number", th.StringType),
        th.Property("recurring_bill_id", th.StringType),
        th.Property("due_by_days", th.StringType),
        th.Property("due_in_days", th.NumberType),
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("currency_symbol", th.StringType),
        th.Property("currency_name_formatted", th.StringType),
        th.Property("documents", th.CustomType({"type": ["array", "string"]})),
        th.Property("subject_content", th.StringType),
        th.Property("price_precision", th.IntegerType),
        th.Property("exchange_rate", th.NumberType),
        th.Property("custom_field_hash", th.ObjectType()),
        th.Property("is_viewed_by_client", th.BooleanType),
        th.Property("client_viewed_time", th.StringType),
        th.Property("is_item_level_tax_calc", th.BooleanType),
        th.Property("is_inclusive_tax", th.BooleanType),
        th.Property("tax_rounding", th.StringType),
        th.Property("is_reverse_charge_applied", th.BooleanType),
        th.Property("is_uber_bill", th.BooleanType),
        th.Property("is_tally_bill", th.BooleanType),
        th.Property("track_discount_in_account", th.BooleanType),
        th.Property("line_items", th.CustomType({"type": ["array", "string"]})),
        th.Property("submitted_date", th.DateTimeType),
        th.Property("submitted_by", th.StringType),
        th.Property("submitted_by_name", th.StringType),
        th.Property("submitted_by_email", th.StringType),
        th.Property("submitted_by_photo_url", th.StringType),
        th.Property("submitter_id", th.StringType),
        th.Property("approver_id", th.StringType),
        th.Property("adjustment", th.NumberType),
        th.Property("adjustment_description", th.StringType),
        th.Property("discount_amount", th.NumberType),
        th.Property("discount", th.StringType),
        th.Property("discount_applied_on_amount", th.NumberType),
        th.Property("is_discount_before_tax", th.BooleanType),
        th.Property("discount_account_id", th.StringType),
        th.Property("discount_account_name", th.StringType),
        th.Property("discount_type", th.StringType),
        th.Property("sub_total", th.NumberType),
        th.Property("sub_total_inclusive_of_tax", th.NumberType),
        th.Property("tax_total", th.NumberType),
        th.Property("total", th.NumberType),
        th.Property("payment_made", th.NumberType),
        th.Property("vendor_credits_applied", th.NumberType),
        th.Property("is_line_item_invoiced", th.BooleanType),
        th.Property(
            "purchaseorders",
            th.ArrayType(
                th.ObjectType(
                    th.Property("purchaseorder_id", th.StringType),
                    th.Property("purchaseorder_number", th.StringType),
                    th.Property("purchaseorder_date", th.StringType),
                    th.Property("date", th.StringType),
                    th.Property("expected_delivery_date", th.StringType),
                    th.Property("purchaseorder_status", th.StringType),
                    th.Property("associated_order_status", th.StringType),
                    th.Property("order_status", th.StringType),
                )
            ),
        ),
        th.Property(
            "taxes",
            th.ArrayType(
                th.ObjectType(
                    th.Property("tax_id", th.StringType),
                    th.Property("tax_name", th.StringType),
                    th.Property("tax_amount", th.NumberType),
                )
            ),
        ),
        th.Property("tax_override_preference", th.StringType),
        th.Property("tds_override_preference", th.StringType),
        th.Property("balance", th.NumberType),
        th.Property(
            "billing_address",
            th.ObjectType(
                th.Property("address", th.StringType),
                th.Property("street2", th.StringType),
                th.Property("city", th.StringType),
                th.Property("state", th.StringType),
                th.Property("zip", th.StringType),
                th.Property("country", th.StringType),
                th.Property("fax", th.StringType),
                th.Property("phone", th.StringType),
                th.Property("attention", th.StringType),
            ),
        ),
        th.Property("created_time", th.DateTimeType),
        th.Property("created_by_id", th.StringType),
        th.Property("last_modified_id", th.StringType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("reference_id", th.StringType),
        th.Property("notes", th.StringType),
        th.Property("terms", th.StringType),
        th.Property("attachment_name", th.StringType),
        th.Property("open_purchaseorders_count", th.IntegerType),
        th.Property("template_id", th.StringType),
        th.Property("template_name", th.StringType),
        th.Property("page_width", th.StringType),
        th.Property("page_height", th.StringType),
        th.Property("orientation", th.StringType),
        th.Property("template_type", th.StringType),
        th.Property("is_approval_required", th.BooleanType),
        th.Property("entity_type", th.StringType),
        th.Property("can_send_in_mail", th.BooleanType),
    ).to_dict()


class SalesOrdersStream(ZohoBooksStream):
    name = "sales_orders"
    path = "/salesorders"
    primary_keys = ["salesorder_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.salesorders[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("salesorder_id", th.StringType),
        th.Property("documents", th.CustomType({"type": ["array", "string"]})),
        th.Property("line_items", th.CustomType({"type": ["array", "string"]})),
        th.Property("shipment_days", th.StringType),
        th.Property("due_by_days", th.NumberType),
        th.Property("due_in_days", th.NumberType),
        th.Property("paid_status", th.StringType),
        th.Property("is_pre_gst", th.BooleanType),
        th.Property("gst_no", th.StringType),
        th.Property("total_invoiced_amount", th.NumberType),
        th.Property("gst_treatment", th.StringType),
        th.Property("place_of_supply", th.StringType),
        th.Property("vat_treatment", th.StringType),
        th.Property("tax_treatment", th.StringType),
        th.Property("zcrm_potential_id", th.StringType),
        th.Property("zcrm_potential_name", th.StringType),
        th.Property("salesorder_number", th.StringType),
        th.Property("date", th.DateType),
        th.Property("delivery_date", th.DateType),
        th.Property("status", th.StringType),
        th.Property("shipment_date", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("reference_number", th.StringType),
        th.Property("customer_id", th.StringType),
        th.Property("customer_name", th.StringType),
        th.Property("contact_persons", th.CustomType({"type": ["array", "string"]})),
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("currency_symbol", th.StringType),
        th.Property("exchange_rate", th.NumberType),
        th.Property("discount_amount", th.NumberType),
        th.Property("discount", th.StringType),
        th.Property("discount_applied_on_amount", th.NumberType),
        th.Property("is_discount_before_tax", th.BooleanType),
        th.Property("discount_type", th.StringType),
        th.Property("estimate_id", th.StringType),
        th.Property("order_status", th.StringType),
        th.Property("email", th.StringType),
        th.Property("delivery_method", th.StringType),
        th.Property("delivery_method_id", th.StringType),
        th.Property("is_inclusive_tax", th.BooleanType),
        th.Property("shipping_charge", th.NumberType),
        th.Property("adjustment", th.NumberType),
        th.Property("adjustment_description", th.StringType),
        th.Property("sub_total", th.NumberType),
        th.Property("tax_total", th.NumberType),
        th.Property("total", th.NumberType),
        th.Property("bcy_total", th.NumberType),
        th.Property("taxes", th.CustomType({"type": ["array", "string"]})),
        th.Property("price_precision", th.NumberType),
        th.Property("is_emailed", th.BooleanType),
        th.Property("billing_address", th.CustomType({"type": ["object", "string"]})),
        th.Property("shipping_address", th.CustomType({"type": ["object", "string"]})),
        th.Property("notes", th.StringType),
        th.Property("terms", th.StringType),
        th.Property("custom_fields", th.CustomType({"type": ["array", "string"]})),
        th.Property("template_id", th.StringType),
        th.Property("template_name", th.StringType),
        th.Property("page_width", th.StringType),
        th.Property("page_height", th.StringType),
        th.Property("orientation", th.StringType),
        th.Property("template_type", th.StringType),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("created_by_id", th.StringType),
        th.Property("attachment_name", th.StringType),
        th.Property("can_send_in_mail", th.BooleanType),
        th.Property("has_attachment", th.BooleanType),
        th.Property("salesperson_id", th.StringType),
        th.Property("salesperson_name", th.StringType),
        th.Property("merchant_id", th.StringType),
        th.Property("merchant_name", th.StringType),
    ).to_dict()


    def get_url_params(self, context, next_page_token):
        return super().get_url_params(context, next_page_token)


    def parse_response(self, response):
        """
        This function works getting all of the data from the Stream
        and adds item details to each item of the stream available.

        It does it using the /salesorders/ details endpoint, chunking with
        100 ids per request.
        """

        if not self.config.get("use_sales_details", False):
            yield from super().parse_response(response)
            return

        # gets organization id from the url
        org_id = response.url.replace(
            self.url_base + "/salesorders?", ""
        ).split("&")[0].replace("organization_id=", "")
        details_base_url = self.url_base + "/salesorders/"
        records = list(extract_jsonpath(self.records_jsonpath, input=response.json()))

        # get all item ids from the records and create a dict with it
        record_ids = OrderedDict((record.get("salesorder_id"), record) for record in records)

        # chunks the request, preserving API quota
        for chunk in self._divide_chunks(list(record_ids.keys())):
            params = {
                "organization_id": org_id,
                "salesorder_ids": ",".join(chunk)
            }
            req = requests.Request(
                "GET",
                details_base_url,
                params=params,
                headers=self.authenticator.auth_headers
            )
            detail_response = self._request(req.prepare())

            sales_details = extract_jsonpath(self.records_jsonpath, input=detail_response.json())

            for sale_detail in sales_details:
                for key in [
                    key for key in sale_detail.keys()
                    if key not in record_ids[sale_detail["salesorder_id"]]
                ]:
                    # Adds data from missing keys
                    record_ids[sale_detail["salesorder_id"]][key] = sale_detail[key]

                yield record_ids[sale_detail["salesorder_id"]]

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "salesorder_id": record["salesorder_id"],
            "organization_id": context.get("organization_id"),
        }


class SalesOrdersDetailsStream(ZohoBooksStream):
    name = "sales_orders_details"
    path = "/salesorders/{salesorder_id}"
    primary_keys = ["salesorder_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.salesorder[*]"
    parent_stream_type = SalesOrdersStream

    schema = th.PropertiesList(
        th.Property("salesorder_id", th.StringType),
        th.Property("documents", th.CustomType({"type": ["array", "string"]})),
        th.Property("line_items", th.CustomType({"type": ["array", "string"]})),
        th.Property("shipment_days", th.StringType),
        th.Property("due_by_days", th.NumberType),
        th.Property("due_in_days", th.NumberType),
        th.Property("paid_status", th.StringType),
        th.Property("is_pre_gst", th.BooleanType),
        th.Property("gst_no", th.StringType),
        th.Property("total_invoiced_amount", th.NumberType),
        th.Property("gst_treatment", th.StringType),
        th.Property("place_of_supply", th.StringType),
        th.Property("vat_treatment", th.StringType),
        th.Property("tax_treatment", th.StringType),
        th.Property("zcrm_potential_id", th.StringType),
        th.Property("zcrm_potential_name", th.StringType),
        th.Property("salesorder_number", th.StringType),
        th.Property("date", th.DateType),
        th.Property("delivery_date", th.DateType),
        th.Property("status", th.StringType),
        th.Property("shipment_date", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("reference_number", th.StringType),
        th.Property("customer_id", th.StringType),
        th.Property("customer_name", th.StringType),
        th.Property("contact_persons", th.CustomType({"type": ["array", "string"]})),
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("currency_symbol", th.StringType),
        th.Property("exchange_rate", th.NumberType),
        th.Property("discount_amount", th.NumberType),
        th.Property("discount", th.NumberType),
        th.Property("discount_applied_on_amount", th.NumberType),
        th.Property("is_discount_before_tax", th.BooleanType),
        th.Property("discount_type", th.StringType),
        th.Property("estimate_id", th.StringType),
        th.Property("order_status", th.StringType),
        th.Property("email", th.StringType),
        th.Property("delivery_method", th.StringType),
        th.Property("delivery_method_id", th.StringType),
        th.Property("is_inclusive_tax", th.BooleanType),
        th.Property("shipping_charge", th.NumberType),
        th.Property("adjustment", th.NumberType),
        th.Property("adjustment_description", th.StringType),
        th.Property("sub_total", th.NumberType),
        th.Property("tax_total", th.NumberType),
        th.Property("total", th.NumberType),
        th.Property("bcy_total", th.NumberType),
        th.Property("taxes", th.CustomType({"type": ["array", "string"]})),
        th.Property("price_precision", th.NumberType),
        th.Property("is_emailed", th.BooleanType),
        th.Property("billing_address", th.CustomType({"type": ["object", "string"]})),
        th.Property("shipping_address", th.CustomType({"type": ["object", "string"]})),
        th.Property("notes", th.StringType),
        th.Property("terms", th.StringType),
        th.Property("custom_fields", th.CustomType({"type": ["array", "string"]})),
        th.Property("template_id", th.StringType),
        th.Property("template_name", th.StringType),
        th.Property("page_width", th.StringType),
        th.Property("page_height", th.StringType),
        th.Property("orientation", th.StringType),
        th.Property("template_type", th.StringType),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("created_by_id", th.StringType),
        th.Property("attachment_name", th.StringType),
        th.Property("can_send_in_mail", th.BooleanType),
        th.Property("has_attachment", th.BooleanType),
        th.Property("salesperson_id", th.StringType),
        th.Property("salesperson_name", th.StringType),
        th.Property("merchant_id", th.StringType),
        th.Property("merchant_name", th.StringType),
    ).to_dict()


class PurchaseOrdersStream(ZohoBooksStream):
    name = "purchase_orders"
    path = "/purchaseorders"
    primary_keys = ["purchaseorder_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.purchaseorders[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("purchaseorder_id", th.StringType),
        th.Property("documents", th.CustomType({"type": ["array", "string"]})),
        th.Property("vat_treatment", th.StringType),
        th.Property("gst_no", th.StringType),
        th.Property("gst_treatment", th.StringType),
        th.Property("tax_treatment", th.StringType),
        th.Property("is_pre_gst", th.BooleanType),
        th.Property("source_of_supply", th.StringType),
        th.Property("destination_of_supply", th.StringType),
        th.Property("place_of_supply", th.StringType),
        th.Property("pricebook_id", th.StringType),
        th.Property("pricebook_name", th.StringType),
        th.Property("is_reverse_charge_applied", th.BooleanType),
        th.Property("purchaseorder_number", th.StringType),
        th.Property("date", th.DateType),
        th.Property("expected_delivery_date", th.StringType),
        th.Property("discount", th.StringType),
        th.Property("discount_account_id", th.StringType),
        th.Property("is_discount_before_tax", th.BooleanType),
        th.Property("reference_number", th.StringType),
        th.Property("status", th.StringType),
        th.Property("vendor_id", th.StringType),
        th.Property("vendor_name", th.StringType),
        th.Property("crm_owner_id", th.StringType),
        th.Property("contact_persons", th.CustomType({"type": ["array", "string"]})),
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("currency_symbol", th.StringType),
        th.Property("exchange_rate", th.NumberType),
        th.Property("delivery_date", th.DateType),
        th.Property("is_emailed", th.BooleanType),
        th.Property("is_inclusive_tax", th.BooleanType),
        th.Property("sub_total", th.NumberType),
        th.Property("tax_total", th.NumberType),
        th.Property("total", th.NumberType),
        th.Property("taxes", th.CustomType({"type": ["array", "string"]})),
        th.Property(
            "acquisition_vat_summary", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "reverse_charge_vat_summary", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property("acquisition_vat_total", th.NumberType),
        th.Property("reverse_charge_vat_total", th.NumberType),
        th.Property("billing_address", th.CustomType({"type": ["object", "string"]})),
        th.Property("notes", th.StringType),
        th.Property("terms", th.StringType),
        th.Property("ship_via", th.StringType),
        th.Property("ship_via_id", th.StringType),
        th.Property("attention", th.StringType),
        th.Property("delivery_org_address_id", th.StringType),
        th.Property("delivery_customer_id", th.StringType),
        th.Property("delivery_address", th.CustomType({"type": ["object", "string"]})),
        th.Property("price_precision", th.NumberType),
        th.Property("custom_fields", th.CustomType({"type": ["array", "string"]})),
        th.Property("attachment_name", th.StringType),
        th.Property("can_send_in_mail", th.BooleanType),
        th.Property("template_id", th.StringType),
        th.Property("template_name", th.StringType),
        th.Property("page_width", th.StringType),
        th.Property("page_height", th.StringType),
        th.Property("orientation", th.StringType),
        th.Property("template_type", th.StringType),
        th.Property("created_time", th.DateTimeType),
        th.Property("created_by_id", th.StringType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("can_mark_as_bill", th.BooleanType),
        th.Property("can_mark_as_unbill", th.BooleanType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "purchaseorder_id": record["purchaseorder_id"],
            "organization_id": context.get("organization_id"),
        }


class PurchaseOrderDetailsStream(ZohoBooksStream):
    name = "purchase_orders_details"
    path = "/purchaseorders/{purchaseorder_id}"
    primary_keys = ["purchaseorder_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.purchaseorder[*]"
    parent_stream_type = PurchaseOrdersStream

    schema = th.PropertiesList(
        th.Property("purchaseorder_id", th.StringType),
        th.Property("documents", th.CustomType({"type": ["array", "string"]})),
        th.Property("line_items", th.CustomType({"type": ["array", "string"]})),
        th.Property("vat_treatment", th.StringType),
        th.Property("gst_no", th.StringType),
        th.Property("gst_treatment", th.StringType),
        th.Property("color_code", th.StringType),
        th.Property("order_status", th.StringType),
        th.Property("current_sub_status_id", th.StringType),
        th.Property("current_sub_status", th.StringType),
        th.Property("pickup_location_id", th.StringType),
        th.Property("source", th.StringType),
        th.Property("tax_treatment", th.StringType),
        th.Property("is_pre_gst", th.BooleanType),
        th.Property("source_of_supply", th.StringType),
        th.Property("destination_of_supply", th.StringType),
        th.Property("place_of_supply", th.StringType),
        th.Property("pricebook_id", th.StringType),
        th.Property("pricebook_name", th.StringType),
        th.Property("is_reverse_charge_applied", th.BooleanType),
        th.Property("purchaseorder_number", th.StringType),
        th.Property("date", th.DateType),
        th.Property("expected_delivery_date", th.StringType),
        th.Property("discount", th.StringType),
        th.Property("discount_account_id", th.StringType),
        th.Property("is_discount_before_tax", th.BooleanType),
        th.Property("reference_number", th.StringType),
        th.Property("status", th.StringType),
        th.Property("vendor_id", th.StringType),
        th.Property("vendor_name", th.StringType),
        th.Property("crm_owner_id", th.StringType),
        th.Property("contact_persons", th.CustomType({"type": ["array", "string"]})),
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("currency_symbol", th.StringType),
        th.Property("exchange_rate", th.NumberType),
        th.Property("delivery_date", th.DateType),
        th.Property("is_emailed", th.BooleanType),
        th.Property("is_inclusive_tax", th.BooleanType),
        th.Property("sub_total", th.NumberType),
        th.Property("tax_total", th.NumberType),
        th.Property("total_invoiced_amount", th.NumberType),
        th.Property("total", th.NumberType),
        th.Property("taxes", th.CustomType({"type": ["array", "string"]})),
        th.Property(
            "acquisition_vat_summary", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "reverse_charge_vat_summary", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property("acquisition_vat_total", th.NumberType),
        th.Property("reverse_charge_vat_total", th.NumberType),
        th.Property("billing_address", th.CustomType({"type": ["object", "string"]})),
        th.Property("notes", th.StringType),
        th.Property("terms", th.StringType),
        th.Property("ship_via", th.StringType),
        th.Property("ship_via_id", th.StringType),
        th.Property("attention", th.StringType),
        th.Property("delivery_org_address_id", th.StringType),
        th.Property("delivery_customer_id", th.StringType),
        th.Property("delivery_address", th.CustomType({"type": ["object", "string"]})),
        th.Property("price_precision", th.NumberType),
        th.Property("custom_fields", th.CustomType({"type": ["array", "string"]})),
        th.Property("attachment_name", th.StringType),
        th.Property("can_send_in_mail", th.BooleanType),
        th.Property("template_id", th.StringType),
        th.Property("template_name", th.StringType),
        th.Property("page_width", th.StringType),
        th.Property("page_height", th.StringType),
        th.Property("orientation", th.StringType),
        th.Property("template_type", th.StringType),
        th.Property("created_time", th.DateTimeType),
        th.Property("created_by_id", th.StringType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("can_mark_as_bill", th.BooleanType),
        th.Property("can_mark_as_unbill", th.BooleanType),
    ).to_dict()


class VendorsStream(ZohoBooksStream):
    name = "vendors"
    path = "/vendors"
    primary_keys = ["contact_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.contacts[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("contact_id", th.StringType),
        th.Property("vendor_id", th.StringType),
        th.Property("contact_name", th.StringType),
        th.Property("vendor_name", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("website", th.StringType),
        th.Property("language_code", th.StringType),
        th.Property("language_code_formatted", th.StringType),
        th.Property("contact_type", th.StringType),
        th.Property("contact_type_formatted", th.StringType),
        th.Property("status", th.StringType),
        th.Property("customer_sub_type", th.StringType),
        th.Property("source", th.StringType),
        th.Property("is_linked_with_zohocrm", th.BooleanType),
        th.Property("payment_terms", th.IntegerType),
        th.Property("payment_terms_label", th.StringType),
        th.Property("currency_id", th.StringType),
        th.Property("twitter", th.StringType),
        th.Property("facebook", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("outstanding_payable_amount", th.NumberType),
        th.Property("outstanding_payable_amount_bcy", th.NumberType),
        th.Property("unused_credits_payable_amount", th.NumberType),
        th.Property("unused_credits_payable_amount_bcy", th.NumberType),
        th.Property("first_name", th.StringType),
        th.Property("last_name", th.StringType),
        th.Property("email", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("mobile", th.StringType),
        th.Property("portal_status", th.StringType),
        th.Property("created_time", th.DateTimeType),
        th.Property("created_time_formatted", th.DateTimeType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("last_modified_time_formatted", th.DateTimeType),
        th.Property("custom_fields", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
        th.Property("custom_field_hash", th.CustomType({"type": ["object", "string"]})),
        th.Property("ach_supported", th.BooleanType),
        th.Property("has_attachment", th.BooleanType),
    ).to_dict()


class EstimatesStream(ZohoBooksStream):
    name = "estimates"
    path = "/estimates"
    primary_keys = ["estimate_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.estimates[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("estimate_id", th.StringType),
        th.Property("zcrm_potential_id", th.StringType),
        th.Property("zcrm_potential_name", th.StringType),
        th.Property("customer_name", th.StringType),
        th.Property("customer_id", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("status", th.StringType),
        th.Property("is_digitally_signed", th.BooleanType),
        th.Property("is_edited_after_sign", th.BooleanType),
        th.Property("color_code", th.StringType),
        th.Property("current_sub_status_id", th.StringType),
        th.Property("current_sub_status", th.StringType),
        th.Property("estimate_number", th.StringType),
        th.Property("reference_number", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("is_signed_and_accepted", th.BooleanType),
        th.Property("total", th.NumberType),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("accepted_date", th.DateTimeType),
        th.Property("declined_date", th.DateTimeType),
        th.Property("expiry_date", th.DateTimeType),
        th.Property("has_attachment", th.BooleanType),
        th.Property("is_viewed_by_client", th.BooleanType),
        th.Property("client_viewed_time", th.StringType),
        th.Property("is_emailed", th.BooleanType),
        th.Property("template_type", th.StringType),
        th.Property("template_id", th.StringType),
        th.Property("is_signature_enabled_in_template", th.BooleanType),
        th.Property("salesperson_id", th.StringType),
        th.Property("salesperson_name", th.StringType),
    ).to_dict()

    def get_child_context(self, record, context):
        return {
            "estimate_id": record["estimate_id"],
            "organization_id": context.get("organization_id"),
        }


class EstimatesDetailsStream(ZohoBooksStream):
    name = "estimates_details"
    path = "/estimates/{estimate_id}"
    primary_keys = ["estimate_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.estimate[*]"
    parent_stream_type = EstimatesStream

    schema = th.PropertiesList(
        th.Property("estimate_id", th.StringType),
        th.Property("estimate_number", th.StringType),
        th.Property("zcrm_potential_id", th.StringType),
        th.Property("zcrm_potential_name", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("created_date", th.DateTimeType),
        th.Property("reference_number", th.StringType),
        th.Property("status", th.StringType),
        th.Property("color_code", th.StringType),
        th.Property("current_sub_status_id", th.StringType),
        th.Property("current_sub_status", th.StringType),
        th.Property("customer_id", th.StringType),
        th.Property("customer_name", th.StringType),
        th.Property("is_signed_and_accepted", th.BooleanType),
        th.Property("is_transaction_created", th.BooleanType),
        th.Property("is_converted_to_open", th.BooleanType),
        th.Property("contact_category", th.StringType),
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("currency_symbol", th.StringType),
        th.Property("exchange_rate", th.NumberType),
        th.Property("expiry_date", th.DateTimeType),
        th.Property("discount", th.NumberType),
        th.Property("discount_applied_on_amount", th.NumberType),
        th.Property("is_discount_before_tax", th.BooleanType),
        th.Property("discount_type", th.StringType),
        th.Property("is_viewed_by_client", th.BooleanType),
        th.Property("client_viewed_time", th.StringType),
        th.Property("is_inclusive_tax", th.BooleanType),
        th.Property("tax_rounding", th.StringType),
        th.Property("is_digitally_signed", th.BooleanType),
        th.Property("is_edited_after_sign", th.BooleanType),
        th.Property("tds_calculation_type", th.StringType),
        th.Property("estimate_url", th.StringType),
        th.Property("is_reverse_charge_applied", th.BooleanType),
        th.Property("submitter_id", th.StringType),
        th.Property("submitted_date", th.StringType),
        th.Property("submitted_by", th.StringType),
        th.Property("submitted_by_name", th.StringType),
        th.Property("submitted_by_email", th.StringType),
        th.Property("submitted_by_photo_url", th.StringType),
        th.Property("approver_id", th.StringType),
        th.Property("shipping_charge_tax_id", th.StringType),
        th.Property("shipping_charge_tax_name", th.StringType),
        th.Property("shipping_charge_tax_type", th.StringType),
        th.Property("shipping_charge_tax_percentage", th.StringType),
        th.Property("shipping_charge_tax", th.StringType),
        th.Property("bcy_shipping_charge_tax", th.StringType),
        th.Property("shipping_charge_exclusive_of_tax", th.NumberType),
        th.Property("shipping_charge_inclusive_of_tax", th.NumberType),
        th.Property("shipping_charge_tax_formatted", th.StringType),
        th.Property("shipping_charge_exclusive_of_tax_formatted", th.StringType),
        th.Property("shipping_charge_inclusive_of_tax_formatted", th.StringType),
        th.Property("shipping_charge", th.NumberType),
        th.Property("bcy_shipping_charge", th.NumberType),
        th.Property("adjustment", th.NumberType),
        th.Property("bcy_adjustment", th.NumberType),
        th.Property("adjustment_description", th.StringType),
        th.Property("roundoff_value", th.NumberType),
        th.Property("transaction_rounding_type", th.StringType),
        th.Property("sub_total", th.NumberType),
        th.Property("bcy_sub_total", th.NumberType),
        th.Property("sub_total_inclusive_of_tax", th.NumberType),
        th.Property("sub_total_exclusive_of_discount", th.NumberType),
        th.Property("discount_total", th.NumberType),
        th.Property("bcy_discount_total", th.NumberType),
        th.Property("discount_percent", th.StringType),
        th.Property("total", th.NumberType),
        th.Property("bcy_total", th.NumberType),
        th.Property("tax_total", th.NumberType),
        th.Property("bcy_tax_total", th.NumberType),
        th.Property("reverse_charge_tax_total", th.NumberType),
        th.Property("price_precision", th.IntegerType),
        th.Property("tax_override_preference", th.StringType),
        th.Property("tds_override_preference", th.StringType),
        th.Property("billing_address", th.ObjectType(
            th.Property("address", th.StringType),
            th.Property("street2", th.StringType),
            th.Property("city", th.StringType),
            th.Property("state", th.StringType),
            th.Property("zip", th.StringType),
            th.Property("country", th.StringType),
            th.Property("fax", th.StringType),
            th.Property("phone", th.StringType),
            th.Property("attention", th.StringType),
        )),
        th.Property("shipping_address", th.ObjectType(
            th.Property("address", th.StringType),
            th.Property("street2", th.StringType),
            th.Property("city", th.StringType),
            th.Property("state", th.StringType),
            th.Property("zip", th.StringType),
            th.Property("country", th.StringType),
            th.Property("fax", th.StringType),
            th.Property("phone", th.StringType),
            th.Property("attention", th.StringType),
        )),
        th.Property("customer_default_billing_address", th.ObjectType(
            th.Property("zip", th.StringType),
            th.Property("country", th.StringType),
            th.Property("address", th.StringType),
            th.Property("city", th.StringType),
            th.Property("phone", th.StringType),
            th.Property("street2", th.StringType),
            th.Property("state", th.StringType),
            th.Property("fax", th.StringType),
            th.Property("state_code", th.StringType),
        )),
        th.Property("notes", th.StringType),
        th.Property("terms", th.StringType),
        th.Property("custom_field_hash", th.CustomType({"type": ["object", "string"]})),
        th.Property("template_id", th.StringType),
        th.Property("template_name", th.StringType),
        th.Property("template_type", th.StringType),
        th.Property("is_signature_enabled_in_template", th.BooleanType),
        th.Property("page_width", th.StringType),
        th.Property("page_height", th.StringType),
        th.Property("orientation", th.StringType),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("created_by_id", th.StringType),
        th.Property("last_modified_by_id", th.StringType),
        th.Property("salesperson_id", th.StringType),
        th.Property("salesperson_name", th.StringType),
        th.Property("attachment_name", th.StringType),
        th.Property("can_send_in_mail", th.BooleanType),
        th.Property("can_send_estimate_sms", th.BooleanType),
        th.Property("allow_partial_payments", th.BooleanType),
        th.Property("payment_options", th.CustomType({"type": ["object", "string"]})),
        th.Property("estimate_type", th.StringType),
        th.Property("accept_retainer", th.BooleanType),
        th.Property("retainer_percentage", th.StringType),
        th.Property("subject_content", th.StringType),
        th.Property("line_items", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()


class AccountTransactionsStream(ZohoBooksStream):
    name = "account_transactions"
    path = "/chartofaccounts/transactions"
    primary_keys = ["transaction_id","account_id"]
    replication_key = "transaction_date"
    records_jsonpath: str = "$.transactions[*]"
    parent_stream_type = ChartOfAccountsStream

    schema = th.PropertiesList(
        th.Property("transaction_id", th.StringType),
        th.Property("transaction_date", th.DateType),
        th.Property("categorized_transaction_id", th.StringType),
        th.Property("transaction_type", th.StringType),
        th.Property("transaction_type_formatted", th.StringType),
        th.Property("account_id", th.StringType),
        th.Property("customer_id", th.StringType),
        th.Property("payee", th.StringType),
        th.Property("description", th.StringType),
        th.Property("entry_number", th.StringType),
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("debit_or_credit", th.StringType),
        th.Property("offset_account_name", th.StringType),
        th.Property("reference_number", th.StringType),
        th.Property("credit_amount", th.NumberType),
        th.Property("fcy_credit_amount", th.NumberType),
        th.Property("fcy_debit_amount", th.NumberType),
        th.Property("debit_amount", th.StringType),
    ).to_dict()

    def post_process(self, record, context):
        """
        Need to overwrite the post_process because zoho sometimes returns transactions
        without a transaction_id, and it fails on export.
        """
        record = super().post_process(record, context)
        
        if record and record.get("transaction_id"):
            return record


class ExpensesStream(ZohoBooksStream):
    name = "expenses"
    path = "/expenses"
    primary_keys = ["expense_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.expenses[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("expense_id", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("paid_through_account_name", th.StringType),
        th.Property("account_name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("bcy_total", th.NumberType),
        th.Property("bcy_total_without_tax", th.NumberType),
        th.Property("total", th.NumberType),
        th.Property("total_without_tax", th.NumberType),
        th.Property("is_billable", th.BooleanType),
        th.Property("reference_number", th.StringType),
        th.Property("customer_id", th.StringType),
        th.Property("is_personal", th.BooleanType),
        th.Property("customer_name", th.StringType),
        th.Property("vendor_id", th.StringType),
        th.Property("vendor_name", th.StringType),
        th.Property("status", th.StringType),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("expense_receipt_name", th.StringType),
        th.Property("exchange_rate", th.NumberType),
        th.Property("distance", th.NumberType),
        th.Property("mileage_rate", th.NumberType),
        th.Property("mileage_unit", th.StringType),
        th.Property("mileage_type", th.StringType),
        th.Property("expense_type", th.StringType),
        th.Property("report_id", th.StringType),
        th.Property("start_reading", th.StringType),
        th.Property("end_reading", th.StringType),
        th.Property("report_name", th.StringType),
        th.Property("report_number", th.StringType),
        th.Property("has_attachment", th.BooleanType),
        th.Property("custom_fields_list", th.CustomType({"type": ["array", "object", "string"]})),
    ).to_dict()

    def get_child_context(self, record, context):
        return {
            "expense_id": record["expense_id"],
            "organization_id": context.get("organization_id"),
            "parent_last_modified_time": record.get("last_modified_time"),
        }


class ExpensesDetailsStream(ZohoBooksStream):
    name = "expenses_details"
    path = "/expenses/{expense_id}"
    primary_keys = ["expense_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.expense[*]"
    parent_stream_type = ExpensesStream

    schema = th.PropertiesList(
        th.Property("expense_id", th.StringType),
        th.Property("transaction_type", th.StringType),
        th.Property("transaction_type_formatted", th.StringType),
        th.Property("expense_item_id", th.StringType),
        th.Property("account_id", th.StringType),
        th.Property("account_name", th.StringType),
        th.Property("paid_through_account_id", th.StringType),
        th.Property("paid_through_account_name", th.StringType),
        th.Property("vendor_id", th.StringType),
        th.Property("vendor_name", th.StringType),
        th.Property("is_reverse_charge_applied", th.BooleanType),
        th.Property("reverse_charge_tax_id", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("markup_percent", th.StringType),
        th.Property("tax_id", th.StringType),
        th.Property("tax_name", th.StringType),
        th.Property("tax_percentage", th.StringType),
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("exchange_rate", th.NumberType),
        th.Property("tax_amount", th.NumberType),
        th.Property("sub_total", th.NumberType),
        th.Property("total", th.NumberType),
        th.Property("bcy_total", th.NumberType),
        th.Property("amount", th.NumberType),
        th.Property("is_inclusive_tax", th.BooleanType),
        th.Property("reference_number", th.StringType),
        th.Property("description", th.StringType),
        th.Property("is_billable", th.BooleanType),
        th.Property("is_personal", th.BooleanType),
        th.Property("customer_id", th.StringType),
        th.Property("customer_name", th.StringType),
        th.Property("expense_receipt_name", th.StringType),
        th.Property("expense_receipt_type", th.StringType),
        th.Property("created_time", th.DateTimeType),
        th.Property("created_by_id", th.StringType),
        th.Property("last_modified_by_id", th.StringType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("employee_id", th.StringType),
        th.Property("employee_name", th.StringType),
        th.Property("employee_email", th.StringType),
        th.Property("vehicle_id", th.StringType),
        th.Property("vehicle_name", th.StringType),
        th.Property("mileage_rate", th.NumberType),
        th.Property("mileage_unit", th.StringType),
        th.Property("mileage_type", th.StringType),
        th.Property("expense_type", th.StringType),
        th.Property("start_reading", th.StringType),
        th.Property("end_reading", th.StringType),
        th.Property("status", th.StringType),
        th.Property("invoice_id", th.StringType),
        th.Property("invoice_number", th.StringType),
        th.Property("report_id", th.StringType),
        th.Property("report_name", th.StringType),
        th.Property("report_number", th.StringType),
        th.Property("user_id", th.StringType),
        th.Property("user_name", th.StringType),
        th.Property("user_email", th.StringType),
        th.Property("approver_id", th.StringType),
        th.Property("approver_name", th.StringType),
        th.Property("approver_email", th.StringType),
        th.Property("report_status", th.StringType),
        th.Property("is_reimbursable", th.BooleanType),
        th.Property("trip_id", th.StringType),
        th.Property("trip_number", th.StringType),
        th.Property("location", th.StringType),
        th.Property("merchant_id", th.StringType),
        th.Property("merchant_name", th.StringType),
        th.Property("payment_mode", th.StringType),
        th.Property("project_id", th.StringType),
        th.Property("project_name", th.StringType),
        th.Property("custom_field_hash", th.ObjectType()),
        th.Property("is_recurring_applicable", th.BooleanType),
        th.Property("line_items", th.CustomType({"type": ["array", "string"]})),
        th.Property("is_surcharge_applicable", th.BooleanType),
        th.Property("fcy_surcharge_amount", th.NumberType),
        th.Property("bcy_surcharge_amount", th.NumberType),
        th.Property("zcrm_potential_id", th.StringType),
        th.Property("zcrm_potential_name", th.StringType),
        th.Property(
            "imported_transactions",
            th.ArrayType(
                th.ObjectType(
                    th.Property("imported_transaction_id", th.StringType),
                    th.Property("date", th.DateTimeType),
                    th.Property("amount", th.NumberType),
                    th.Property("payee", th.StringType),
                    th.Property("description", th.StringType),
                    th.Property("reference_number", th.StringType),
                    th.Property("status", th.StringType),
                    th.Property("account_id", th.StringType),
                )
            ),
        ),
    ).to_dict()


class VendorPaymentsStream(ZohoBooksStream):
    name = "vendorpayments"
    path = "/vendorpayments"
    primary_keys = ["payment_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.vendorpayments[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("payment_id", th.StringType),
        th.Property("vendor_id", th.StringType),
        th.Property("vendor_name", th.StringType),
        th.Property("payment_number", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("payment_mode", th.StringType),
        th.Property("paid_through_account_id", th.StringType),
        th.Property("paid_through_account_name", th.StringType),
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("exchange_rate", th.NumberType),
        th.Property("amount", th.NumberType),
        th.Property("bcy_amount", th.NumberType),
        th.Property("unused_amount", th.NumberType),
        th.Property("description", th.StringType),
        th.Property("reference_number", th.StringType),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("bills", th.CustomType({"type": ["array", "string"]})),
        th.Property("documents", th.CustomType({"type": ["array", "string"]})),
        th.Property("custom_fields", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()


class BankTransactionsStream(ZohoBooksStream):
    name = "banktransactions"
    path = "/banktransactions"
    primary_keys = ["transaction_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.banktransactions[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("transaction_id", th.StringType),
        th.Property("bank_account_id", th.StringType),
        th.Property("account_id", th.StringType),
        th.Property("account_name", th.StringType),
        th.Property("transaction_type", th.StringType),
        th.Property("transaction_type_formatted", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("debit_or_credit", th.StringType),
        th.Property("amount", th.NumberType),
        th.Property("bcy_amount", th.NumberType),
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("exchange_rate", th.NumberType),
        th.Property("payee", th.StringType),
        th.Property("description", th.StringType),
        th.Property("reference_number", th.StringType),
        th.Property("status", th.StringType),
        th.Property("is_categorized", th.BooleanType),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_modified_time", th.DateTimeType),
    ).to_dict()


class BankAccountsStream(ZohoBooksStream):
    name = "bankaccounts"
    path = "/bankaccounts"
    primary_keys = ["account_id"]
    replication_key = None
    records_jsonpath: str = "$.bankaccounts[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("account_id", th.StringType),
        th.Property("account_name", th.StringType),
        th.Property("bank_name", th.StringType),
        th.Property("account_number", th.StringType),
        th.Property("account_type", th.StringType),
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("current_balance", th.NumberType),
        th.Property("bcy_current_balance", th.NumberType),
        th.Property("is_active", th.BooleanType),
        th.Property("is_primary_account", th.BooleanType),
        th.Property("created_time", th.DateTimeType),
        th.Property("last_modified_time", th.DateTimeType),
    ).to_dict()


class CreditNotesIDStream(ZohoBooksStream):
    name = "credit_notes_id"
    path = "/creditnotes"
    primary_keys = ["creditnote_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.creditnotes[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("creditnote_id", th.StringType),
        th.Property("last_modified_time", th.DateTimeType),
    ).to_dict()

    def get_child_context(self, record, context={}):
        return {
            "creditnote_id": record["creditnote_id"],
            "organization_id": context.get("organization_id"),
        }


class CreditNoteDetailsStream(ZohoBooksStream):
    name = "credit_notes_details"
    path = "/creditnotes/{creditnote_id}"
    primary_keys = ["creditnote_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.creditnote[*]"
    parent_stream_type = CreditNotesIDStream

    schema = th.PropertiesList(
        th.Property("creditnote_id", th.StringType),
        th.Property("creditnote_number", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("is_pre_gst", th.BooleanType),
        th.Property("place_of_supply", th.StringType),
        th.Property("vat_treatment", th.StringType),
        th.Property("vat_reg_no", th.StringType),
        th.Property("gst_no", th.StringType),
        th.Property("cfdi_usage", th.StringType),
        th.Property("cfdi_reference_type", th.StringType),
        th.Property("gst_treatment", th.StringType),
        th.Property("tax_treatment", th.StringType),
        th.Property("status", th.StringType),
        th.Property("customer_id", th.StringType),
        th.Property("customer_name", th.StringType),
        th.Property("reference_number", th.StringType),
        th.Property("email", th.StringType),
        th.Property("total", th.NumberType),
        th.Property("balance", th.NumberType),
        th.Property("currency_code", th.StringType),
        th.Property("currency_symbol", th.StringType),
        th.Property("billing_address", th.ObjectType()),
        th.Property("shipping_address", th.ObjectType()),
        th.Property("created_time", th.DateTimeType),
        th.Property("updated_time", th.DateTimeType),
        th.Property("template_id", th.StringType),
        th.Property("template_name", th.StringType),
        th.Property("notes", th.StringType),
        th.Property("terms", th.StringType),
        th.Property("line_items", th.CustomType({"type": ["array", "string"]})),
        th.Property("last_modified_time", th.DateTimeType),
    ).to_dict()




class VendorCreditIDSStream(ZohoBooksStream):
    name = "vendor_credit_ids_stream"
    path = "/vendorcredits"
    primary_keys = ["vendor_credit_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.vendor_credits[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("vendor_credit_id", th.StringType),
        th.Property("last_modified_time", th.DateTimeType),
    ).to_dict()

    def get_child_context(self, record, context):
        return {
            "vendor_credit_id": record["vendor_credit_id"],
            "organization_id": context.get("organization_id"),
            "parent_last_modified_time": record.get("last_modified_time"),
        }


class VendorCreditsStream(VendorCreditIDSStream):
    """Zoho endpoint-name alias for vendor credits."""

    name = "vendorcredits"


class VendorCreditDetailsStream(ZohoBooksStream):
    name = "vendor_credit_details"
    path = "/vendorcredits/{vendor_credit_id}"
    primary_keys = ["vendor_credit_id"]
    replication_key = "last_modified_time"
    records_jsonpath: str = "$.vendor_credit[*]"
    parent_stream_type = VendorCreditIDSStream

    schema = th.PropertiesList(
        th.Property("vendor_credit_id", th.StringType),
        th.Property("vendor_id", th.StringType),
        th.Property("currency_id", th.StringType),
        th.Property("vat_treatment", th.StringType),
        th.Property("vendor_credit_number", th.StringType),
        th.Property("gst_treatment", th.StringType),
        th.Property("tax_treatment", th.StringType),
        th.Property("gst_no", th.StringType),
        th.Property("source_of_supply", th.StringType),
        th.Property("destination_of_supply", th.StringType),
        th.Property("place_of_supply", th.StringType),
        th.Property("pricebook_id", th.StringType),
        th.Property("reference_number", th.StringType),
        th.Property("is_update_customer", th.BooleanType),
        th.Property("date", th.DateTimeType),
        th.Property("last_modified_time", th.DateTimeType),
        th.Property("exchange_rate", th.NumberType),
        th.Property("is_inclusive_tax", th.BooleanType),
        th.Property("notes", th.StringType),
        th.Property("line_items", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

class ProfitAndLossStream(ZohoBooksStream):
    name = "profit_and_loss"
    path = "/reports/profitandloss"
    primary_keys = None
    replication_key = None
    records_jsonpath: str = "$.profit_and_loss[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("total", th.NumberType),
        th.Property("previous_values", th.CustomType({"type": ["array", "string"]})),
        th.Property("account_transactions", th.CustomType({"type": ["array", "string"]})),
        th.Property("name", th.StringType),
        th.Property("previous_total", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()


class ReportAccountTransactionsStream(ZohoBooksStream):
    name = "report_account_transactions"
    path = "/reports/accounttransaction"
    primary_keys = None
    replication_key = None
    records_jsonpath: str = "$.account_transactions[:1].account_transactions[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("total", th.NumberType),
        th.Property("date", th.DateTimeType),
        th.Property("account_name", th.StringType),
        th.Property("transaction_details", th.StringType),
        th.Property("transaction_id", th.StringType),
        th.Property("reference_transaction_id", th.StringType),
        th.Property("offset_account_id", th.StringType),
        th.Property("offset_account_type", th.StringType),
        th.Property("transaction_type", th.StringType),
        th.Property("reference_number", th.StringType),
        th.Property("entity_number", th.StringType),
        th.Property("debit", th.NumberType),
        th.Property("credit", th.NumberType),
        th.Property("net_amount", th.StringType),
        th.Property("contact_id", th.StringType),
        th.Property("account_id", th.StringType),
        th.Property("project_ids", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("account", th.ObjectType(
            th.Property("account_group", th.StringType),
            th.Property("account_type", th.StringType),
        )),
        th.Property("reporting_tag", th.StringType),
        th.Property("branch", th.CustomType({"type": ["object", "string"]})),
    ).to_dict()

class ProfitAndLossCashStream(ProfitAndLossStream):
    name = "profit_and_loss_cash_based"

class ReportAccountTransactionsCashStream(ReportAccountTransactionsStream):
    name = "report_account_transactions_cash_based"

class ReportAgingDetailStream(ZohoBooksStream):
    name = "ar_aging_detail_report"
    path = "/reports/aragingdetails"
    primary_keys = None
    replication_key = None
    records_jsonpath: str = "$.invoiceaging[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("amount", th.NumberType),
        th.Property("group_list", th.CustomType({"type": ["object", "array"]})),
    ).to_dict()
    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["per_page"] = 500
        params["sort_order"] = "A"
        params["aging_by"] = "invoiceduedate"
        params["interval_range"] = self.config.get("report_interval_range",15)
        params["number_of_columns"] = self.config.get("number_of_columns", 4)
        params["interval_type"] = self.config.get("report_interval_type", "days")
        params["group_by"] = self.config.get("report_group_by", "none")
        params["include_credit_notes"] = self.config.get(
            "report_include_credit_notes", "false"
        )
        params["include_manual_journals"] = self.config.get(
            "report_include_manual_journals", "false"
        )
        params["response_option"]  = 1
        #@TODO apply report date ranges once we figure out parameter names
        params["report_date"] = self.config.get("report_date",datetime.today().strftime("%Y-%m-%d"))  # noqa: F821
        
        if "last_modified_time" in params:
            del params["last_modified_time"]
        return params
class ReportAgingSummaryStream(ZohoBooksStream):
    name = "ar_aging_summary_report"
    path = "/reports/aragingsummary"
    primary_keys = None
    replication_key = None
    records_jsonpath: str = "$.ar_aging_summary[:1].invoiceagingsummary[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("customer_id", th.StringType),
        th.Property("customer_name", th.StringType),
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("current", th.NumberType),
        th.Property("intervals", th.CustomType({"type": ["object", "array"]})),
    ).to_dict()
    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["per_page"] = 500
        params["sort_order"] = "A"
        params["sort_column"] = "customer_name"
        params["aging_by"] = "invoiceduedate"
        params["show_by"] = "overdueamount"
        params["group_by"] = "customer_name"
        params["include_credit_notes"] = self.config.get(
            "report_include_credit_notes", "false"
        )
        params["interval_type"] = self.config.get("report_interval_type", "days")
        params["number_of_columns"] = self.config.get("number_of_columns", 4)
        params["interval_range"] = self.config.get("report_interval_range", 15)
        params["include_manual_journals"] = self.config.get(
            "report_include_manual_journals", "false"
        )
        params["is_new_group_by"] = self.config.get("report_is_new_group_by", "true")
        params["response_option"] = 1
        #@TODO apply report date ranges once we figure out parameter names
        params["report_date"] = self.config.get("report_date",datetime.today().strftime("%Y-%m-%d"))  # noqa: F821
        if "last_modified_time" in params:
            del params["last_modified_time"]
        return params

class CurrencyStream(ZohoBooksStream):
    name = "currencies"
    path = "/settings/currencies"
    replication_key = None
    primary_keys = ["currency_id"]
    records_jsonpath: str = "$.currencies[*]"
    parent_stream_type = OrganizationIdStream

    schema = th.PropertiesList(
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("currency_name", th.StringType),
        th.Property("currency_name_formatted", th.StringType),
        th.Property("currency_symbol", th.StringType),
        th.Property("price_precision", th.IntegerType),
        th.Property("currency_format", th.StringType),
        th.Property("is_base_currency", th.BooleanType),
        th.Property("exchange_rate", th.NumberType),
        th.Property("effective_date", th.DateTimeType),
    ).to_dict()

    def get_child_context(self, record, context) -> dict:
        return {
            "currency_id": record["currency_id"],
            "organization_id": context.get("organization_id"),
        }


class ExchangeRatesStream(ZohoBooksStream):
    name = "exchangerates"
    path = "/settings/currencies/{currency_id}/exchangerates"
    replication_key = None
    primary_keys = ["effective_date", "currency_id"]
    records_jsonpath: str = "$.exchange_rates[*]"
    parent_stream_type = CurrencyStream

    schema = th.PropertiesList(
        th.Property("currency_id", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("currency_name", th.StringType),
        th.Property("exchange_rate", th.NumberType),
        th.Property("effective_date", th.DateTimeType),
    ).to_dict()

    def post_process(self, record, context):
        record = super().post_process(record, context)
        if record and context is not None:
            record.setdefault("currency_id", context.get("currency_id"))
        return record
