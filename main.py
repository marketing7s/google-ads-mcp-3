"""
Google Ads MCP Server
A remote MCP server for Claude.ai to interact with the Google Ads API.
"""

import json
import os
from typing import Optional
import httpx
from pydantic import BaseModel, Field, ConfigDict
from mcp.server.fastmcp import FastMCP

# ── Server Init ──────────────────────────────────────────────────────────────
mcp = FastMCP("google_ads_mcp")

# ── Google Ads API Config ─────────────────────────────────────────────────────
DEVELOPER_TOKEN   = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", "")
CLIENT_ID         = os.environ.get("GOOGLE_ADS_CLIENT_ID", "")
CLIENT_SECRET     = os.environ.get("GOOGLE_ADS_CLIENT_SECRET", "")
REFRESH_TOKEN     = os.environ.get("GOOGLE_ADS_REFRESH_TOKEN", "")
CUSTOMER_ID       = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", "").replace("-", "")
LOGIN_CUSTOMER_ID = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "").replace("-", "")

TOKEN_URL      = "https://oauth2.googleapis.com/token"
ADS_API_BASE   = "https://googleads.googleapis.com/v17"
API_VERSION    = "v17"


# ── Auth Helpers ──────────────────────────────────────────────────────────────

async def get_access_token() -> str:
    """Exchange refresh token for a short-lived access token."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(TOKEN_URL, data={
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": REFRESH_TOKEN,
            "grant_type":    "refresh_token",
        })
        resp.raise_for_status()
        return resp.json()["access_token"]


def build_headers(access_token: str, customer_id: str) -> dict:
    """Build standard Google Ads API request headers."""
    headers = {
        "Authorization":         f"Bearer {access_token}",
        "developer-token":       DEVELOPER_TOKEN,
        "Content-Type":          "application/json",
    }
    if LOGIN_CUSTOMER_ID:
        headers["login-customer-id"] = LOGIN_CUSTOMER_ID
    return headers


async def ads_query(customer_id: str, gaql: str) -> list:
    """Run a GAQL query against the Google Ads API and return rows."""
    cid = customer_id.replace("-", "") or CUSTOMER_ID
    token = await get_access_token()
    headers = build_headers(token, cid)
    url = f"{ADS_API_BASE}/customers/{cid}/googleAds:searchStream"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, headers=headers, json={"query": gaql})
        resp.raise_for_status()
        # searchStream returns newline-delimited JSON objects
        rows = []
        for line in resp.text.strip().splitlines():
            obj = json.loads(line)
            rows.extend(obj.get("results", []))
        return rows


def _handle_error(e: Exception) -> str:
    if isinstance(e, httpx.HTTPStatusError):
        try:
            detail = e.response.json()
        except Exception:
            detail = e.response.text
        return f"API Error {e.response.status_code}: {json.dumps(detail, indent=2)}"
    return f"Error: {type(e).__name__}: {e}"


# ── Input Models ──────────────────────────────────────────────────────────────

class CustomerIdInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    customer_id: Optional[str] = Field(
        default=None,
        description="Google Ads customer ID (e.g. '1234567890'). Defaults to env var if omitted."
    )


class CampaignQueryInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    customer_id: Optional[str] = Field(default=None, description="Google Ads customer ID.")
    status_filter: Optional[str] = Field(
        default="ENABLED",
        description="Filter by status: ENABLED, PAUSED, REMOVED, or ALL."
    )


class DateRangeInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    customer_id: Optional[str] = Field(default=None, description="Google Ads customer ID.")
    start_date: str = Field(..., description="Start date in YYYY-MM-DD format.")
    end_date:   str = Field(..., description="End date in YYYY-MM-DD format.")


class CampaignIdInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    customer_id: Optional[str] = Field(default=None, description="Google Ads customer ID.")
    campaign_id: str = Field(..., description="The campaign resource ID (numeric).")


# ── Tools ─────────────────────────────────────────────────────────────────────

@mcp.tool(
    name="list_accessible_customers",
    annotations={"readOnlyHint": True, "destructiveHint": False}
)
async def list_accessible_customers() -> str:
    """List all Google Ads customer accounts accessible with the current credentials.

    Returns:
        str: JSON list of accessible customer IDs and descriptive names.
    """
    try:
        token = await get_access_token()
        headers = {
            "Authorization":   f"Bearer {token}",
            "developer-token": DEVELOPER_TOKEN,
        }
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{ADS_API_BASE}/customers:listAccessibleCustomers",
                headers=headers
            )
            resp.raise_for_status()
            data = resp.json()
        return json.dumps(data, indent=2)
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="list_campaigns",
    annotations={"readOnlyHint": True, "destructiveHint": False}
)
async def list_campaigns(params: CampaignQueryInput) -> str:
    """List all campaigns in a Google Ads account.

    Args:
        params: customer_id and optional status_filter (ENABLED/PAUSED/REMOVED/ALL)

    Returns:
        str: JSON array of campaigns with id, name, status, budget, bidding strategy.
    """
    try:
        status_clause = ""
        if params.status_filter and params.status_filter.upper() != "ALL":
            status_clause = f"WHERE campaign.status = '{params.status_filter.upper()}'"
        gaql = f"""
            SELECT
              campaign.id,
              campaign.name,
              campaign.status,
              campaign.advertising_channel_type,
              campaign_budget.amount_micros,
              campaign.bidding_strategy_type
            FROM campaign
            {status_clause}
            ORDER BY campaign.name
        """
        rows = await ads_query(params.customer_id or CUSTOMER_ID, gaql)
        campaigns = []
        for row in rows:
            c = row.get("campaign", {})
            b = row.get("campaignBudget", {})
            campaigns.append({
                "id":             c.get("id"),
                "name":           c.get("name"),
                "status":         c.get("status"),
                "channel_type":   c.get("advertisingChannelType"),
                "daily_budget":   round(int(b.get("amountMicros", 0)) / 1_000_000, 2),
                "bidding_strategy": c.get("biddingStrategyType"),
            })
        return json.dumps({"count": len(campaigns), "campaigns": campaigns}, indent=2)
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="get_campaign_performance",
    annotations={"readOnlyHint": True, "destructiveHint": False}
)
async def get_campaign_performance(params: DateRangeInput) -> str:
    """Get campaign performance metrics (impressions, clicks, cost, conversions) for a date range.

    Args:
        params: customer_id, start_date (YYYY-MM-DD), end_date (YYYY-MM-DD)

    Returns:
        str: JSON array of campaigns with clicks, impressions, CTR, CPC, cost, conversions.
    """
    try:
        gaql = f"""
            SELECT
              campaign.id,
              campaign.name,
              campaign.status,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              metrics.conversions,
              metrics.ctr,
              metrics.average_cpc
            FROM campaign
            WHERE segments.date BETWEEN '{params.start_date}' AND '{params.end_date}'
            ORDER BY metrics.cost_micros DESC
        """
        rows = await ads_query(params.customer_id or CUSTOMER_ID, gaql)
        results = []
        for row in rows:
            c = row.get("campaign", {})
            m = row.get("metrics", {})
            results.append({
                "campaign_id":   c.get("id"),
                "campaign_name": c.get("name"),
                "status":        c.get("status"),
                "impressions":   m.get("impressions", 0),
                "clicks":        m.get("clicks", 0),
                "cost":          round(int(m.get("costMicros", 0)) / 1_000_000, 2),
                "conversions":   round(float(m.get("conversions", 0)), 2),
                "ctr_pct":       round(float(m.get("ctr", 0)) * 100, 2),
                "avg_cpc":       round(int(m.get("averageCpc", 0)) / 1_000_000, 2),
            })
        return json.dumps({"period": f"{params.start_date} to {params.end_date}", "campaigns": results}, indent=2)
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="get_ad_group_performance",
    annotations={"readOnlyHint": True, "destructiveHint": False}
)
async def get_ad_group_performance(params: DateRangeInput) -> str:
    """Get ad group performance metrics for a date range.

    Args:
        params: customer_id, start_date (YYYY-MM-DD), end_date (YYYY-MM-DD)

    Returns:
        str: JSON array of ad groups with clicks, impressions, cost, conversions.
    """
    try:
        gaql = f"""
            SELECT
              campaign.name,
              ad_group.id,
              ad_group.name,
              ad_group.status,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              metrics.conversions
            FROM ad_group
            WHERE segments.date BETWEEN '{params.start_date}' AND '{params.end_date}'
            ORDER BY metrics.cost_micros DESC
            LIMIT 100
        """
        rows = await ads_query(params.customer_id or CUSTOMER_ID, gaql)
        results = []
        for row in rows:
            ag = row.get("adGroup", {})
            c  = row.get("campaign", {})
            m  = row.get("metrics", {})
            results.append({
                "campaign_name":  c.get("name"),
                "ad_group_id":    ag.get("id"),
                "ad_group_name":  ag.get("name"),
                "status":         ag.get("status"),
                "impressions":    m.get("impressions", 0),
                "clicks":         m.get("clicks", 0),
                "cost":           round(int(m.get("costMicros", 0)) / 1_000_000, 2),
                "conversions":    round(float(m.get("conversions", 0)), 2),
            })
        return json.dumps({"period": f"{params.start_date} to {params.end_date}", "ad_groups": results}, indent=2)
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="get_keyword_performance",
    annotations={"readOnlyHint": True, "destructiveHint": False}
)
async def get_keyword_performance(params: DateRangeInput) -> str:
    """Get keyword performance metrics for a date range.

    Args:
        params: customer_id, start_date (YYYY-MM-DD), end_date (YYYY-MM-DD)

    Returns:
        str: JSON array of keywords with match type, clicks, impressions, cost, quality score.
    """
    try:
        gaql = f"""
            SELECT
              campaign.name,
              ad_group.name,
              ad_group_criterion.keyword.text,
              ad_group_criterion.keyword.match_type,
              ad_group_criterion.quality_info.quality_score,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              metrics.conversions
            FROM keyword_view
            WHERE segments.date BETWEEN '{params.start_date}' AND '{params.end_date}'
              AND ad_group_criterion.status != 'REMOVED'
            ORDER BY metrics.cost_micros DESC
            LIMIT 200
        """
        rows = await ads_query(params.customer_id or CUSTOMER_ID, gaql)
        results = []
        for row in rows:
            kw = row.get("adGroupCriterion", {}).get("keyword", {})
            qi = row.get("adGroupCriterion", {}).get("qualityInfo", {})
            c  = row.get("campaign", {})
            ag = row.get("adGroup", {})
            m  = row.get("metrics", {})
            results.append({
                "campaign":      c.get("name"),
                "ad_group":      ag.get("name"),
                "keyword":       kw.get("text"),
                "match_type":    kw.get("matchType"),
                "quality_score": qi.get("qualityScore"),
                "impressions":   m.get("impressions", 0),
                "clicks":        m.get("clicks", 0),
                "cost":          round(int(m.get("costMicros", 0)) / 1_000_000, 2),
                "conversions":   round(float(m.get("conversions", 0)), 2),
            })
        return json.dumps({"period": f"{params.start_date} to {params.end_date}", "keywords": results}, indent=2)
    except Exception as e:
        return _handle_error(e)


@mcp.tool(
    name="get_account_summary",
    annotations={"readOnlyHint": True, "destructiveHint": False}
)
async def get_account_summary(params: DateRangeInput) -> str:
    """Get a high-level account summary: total spend, clicks, impressions, conversions for a date range.

    Args:
        params: customer_id, start_date (YYYY-MM-DD), end_date (YYYY-MM-DD)

    Returns:
        str: JSON summary with totals and top-level metrics.
    """
    try:
        gaql = f"""
            SELECT
              customer.id,
              customer.descriptive_name,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              metrics.conversions
            FROM customer
            WHERE segments.date BETWEEN '{params.start_date}' AND '{params.end_date}'
        """
        rows = await ads_query(params.customer_id or CUSTOMER_ID, gaql)
        totals = {"impressions": 0, "clicks": 0, "cost": 0.0, "conversions": 0.0}
        account_name = ""
        for row in rows:
            cu = row.get("customer", {})
            m  = row.get("metrics", {})
            account_name = cu.get("descriptiveName", "")
            totals["impressions"]  += int(m.get("impressions", 0))
            totals["clicks"]       += int(m.get("clicks", 0))
            totals["cost"]         += int(m.get("costMicros", 0)) / 1_000_000
            totals["conversions"]  += float(m.get("conversions", 0))
        totals["cost"]        = round(totals["cost"], 2)
        totals["conversions"] = round(totals["conversions"], 2)
        totals["ctr_pct"]     = round(totals["clicks"] / totals["impressions"] * 100, 2) if totals["impressions"] else 0
        totals["cost_per_conversion"] = round(totals["cost"] / totals["conversions"], 2) if totals["conversions"] else None
        return json.dumps({
            "account":  account_name,
            "period":   f"{params.start_date} to {params.end_date}",
            "summary":  totals,
        }, indent=2)
    except Exception as e:
        return _handle_error(e)


# ── Entry Point ───────────────────────────────────────────────────────────────


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    app = mcp.streamable_http_app()
    uvicorn.run(app, host="0.0.0.0", port=port)
```
