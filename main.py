"""
Google Ads MCP Server - Claude.ai compatible (MCP spec 2025-06-18)
"""

import json
import os
from typing import Optional
import httpx
import uvicorn
from pydantic import BaseModel, Field, ConfigDict
from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from starlette.routing import Route, Mount

# Server Init
mcp = FastMCP("google_ads_mcp")

# Google Ads API Config
DEVELOPER_TOKEN   = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", "")
CLIENT_ID         = os.environ.get("GOOGLE_ADS_CLIENT_ID", "")
CLIENT_SECRET     = os.environ.get("GOOGLE_ADS_CLIENT_SECRET", "")
REFRESH_TOKEN     = os.environ.get("GOOGLE_ADS_REFRESH_TOKEN", "")
CUSTOMER_ID       = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", "").replace("-", "")
LOGIN_CUSTOMER_ID = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "").replace("-", "")

TOKEN_URL    = "https://oauth2.googleapis.com/token"
ADS_API_BASE = "https://googleads.googleapis.com/v17"
MCP_VERSION  = "2025-06-18"


# Auth Helpers

async def get_access_token() -> str:
    async with httpx.AsyncClient() as client:
        resp = await client.post(TOKEN_URL, data={
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": REFRESH_TOKEN,
            "grant_type":    "refresh_token",
        })
        resp.raise_for_status()
        return resp.json()["access_token"]


def build_headers(access_token: str) -> dict:
    headers = {
        "Authorization":   f"Bearer {access_token}",
        "developer-token": DEVELOPER_TOKEN,
        "Content-Type":    "application/json",
    }
    if LOGIN_CUSTOMER_ID:
        headers["login-customer-id"] = LOGIN_CUSTOMER_ID
    return headers


async def ads_query(customer_id: str, gaql: str) -> list:
    cid = customer_id.replace("-", "") or CUSTOMER_ID
    token = await get_access_token()
    headers = build_headers(token)
    url = f"{ADS_API_BASE}/customers/{cid}/googleAds:searchStream"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, headers=headers, json={"query": gaql})
        resp.raise_for_status()
        rows = []
        for line in resp.text.strip().splitlines():
            obj = json.loads(line)
            rows.extend(obj.get("results", []))
        return rows


def handle_error(e: Exception) -> str:
    if isinstance(e, httpx.HTTPStatusError):
        try:
            detail = e.response.json()
        except Exception:
            detail = e.response.text
        return f"API Error {e.response.status_code}: {json.dumps(detail, indent=2)}"
    return f"Error: {type(e).__name__}: {e}"


# Input Models

class CampaignQueryInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    customer_id: Optional[str] = Field(default=None, description="Google Ads customer ID.")
    status_filter: Optional[str] = Field(default="ENABLED", description="ENABLED, PAUSED, REMOVED, or ALL.")


class DateRangeInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    customer_id: Optional[str] = Field(default=None, description="Google Ads customer ID.")
    start_date: str = Field(..., description="Start date YYYY-MM-DD.")
    end_date:   str = Field(..., description="End date YYYY-MM-DD.")


# Tools

@mcp.tool(name="list_accessible_customers", annotations={"readOnlyHint": True, "destructiveHint": False})
async def list_accessible_customers() -> str:
    """List all Google Ads customer accounts accessible with the current credentials."""
    try:
        token = await get_access_token()
        headers = {"Authorization": f"Bearer {token}", "developer-token": DEVELOPER_TOKEN}
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"{ADS_API_BASE}/customers:listAccessibleCustomers", headers=headers)
            resp.raise_for_status()
        return json.dumps(resp.json(), indent=2)
    except Exception as e:
        return handle_error(e)


@mcp.tool(name="list_campaigns", annotations={"readOnlyHint": True, "destructiveHint": False})
async def list_campaigns(params: CampaignQueryInput) -> str:
    """List all campaigns in a Google Ads account with budget and bidding strategy."""
    try:
        status_clause = ""
        if params.status_filter and params.status_filter.upper() != "ALL":
            status_clause = f"WHERE campaign.status = '{params.status_filter.upper()}'"
        gaql = f"""
            SELECT campaign.id, campaign.name, campaign.status,
              campaign.advertising_channel_type, campaign_budget.amount_micros,
              campaign.bidding_strategy_type
            FROM campaign {status_clause} ORDER BY campaign.name
        """
        rows = await ads_query(params.customer_id or CUSTOMER_ID, gaql)
        campaigns = [{
            "id": r.get("campaign", {}).get("id"),
            "name": r.get("campaign", {}).get("name"),
            "status": r.get("campaign", {}).get("status"),
            "channel_type": r.get("campaign", {}).get("advertisingChannelType"),
            "daily_budget": round(int(r.get("campaignBudget", {}).get("amountMicros", 0)) / 1_000_000, 2),
            "bidding_strategy": r.get("campaign", {}).get("biddingStrategyType"),
        } for r in rows]
        return json.dumps({"count": len(campaigns), "campaigns": campaigns}, indent=2)
    except Exception as e:
        return handle_error(e)


@mcp.tool(name="get_campaign_performance", annotations={"readOnlyHint": True, "destructiveHint": False})
async def get_campaign_performance(params: DateRangeInput) -> str:
    """Get campaign performance metrics (clicks, impressions, cost, conversions) for a date range."""
    try:
        gaql = f"""
            SELECT campaign.id, campaign.name, campaign.status,
              metrics.impressions, metrics.clicks, metrics.cost_micros,
              metrics.conversions, metrics.ctr, metrics.average_cpc
            FROM campaign
            WHERE segments.date BETWEEN '{params.start_date}' AND '{params.end_date}'
            ORDER BY metrics.cost_micros DESC
        """
        rows = await ads_query(params.customer_id or CUSTOMER_ID, gaql)
        results = [{
            "campaign_id": r.get("campaign", {}).get("id"),
            "campaign_name": r.get("campaign", {}).get("name"),
            "status": r.get("campaign", {}).get("status"),
            "impressions": r.get("metrics", {}).get("impressions", 0),
            "clicks": r.get("metrics", {}).get("clicks", 0),
            "cost": round(int(r.get("metrics", {}).get("costMicros", 0)) / 1_000_000, 2),
            "conversions": round(float(r.get("metrics", {}).get("conversions", 0)), 2),
            "ctr_pct": round(float(r.get("metrics", {}).get("ctr", 0)) * 100, 2),
            "avg_cpc": round(int(r.get("metrics", {}).get("averageCpc", 0)) / 1_000_000, 2),
        } for r in rows]
        return json.dumps({"period": f"{params.start_date} to {params.end_date}", "campaigns": results}, indent=2)
    except Exception as e:
        return handle_error(e)


@mcp.tool(name="get_keyword_performance", annotations={"readOnlyHint": True, "destructiveHint": False})
async def get_keyword_performance(params: DateRangeInput) -> str:
    """Get keyword performance metrics including quality scores for a date range."""
    try:
        gaql = f"""
            SELECT campaign.name, ad_group.name,
              ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type,
              ad_group_criterion.quality_info.quality_score,
              metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions
            FROM keyword_view
            WHERE segments.date BETWEEN '{params.start_date}' AND '{params.end_date}'
              AND ad_group_criterion.status != 'REMOVED'
            ORDER BY metrics.cost_micros DESC LIMIT 200
        """
        rows = await ads_query(params.customer_id or CUSTOMER_ID, gaql)
        results = [{
            "campaign": r.get("campaign", {}).get("name"),
            "ad_group": r.get("adGroup", {}).get("name"),
            "keyword": r.get("adGroupCriterion", {}).get("keyword", {}).get("text"),
            "match_type": r.get("adGroupCriterion", {}).get("keyword", {}).get("matchType"),
            "quality_score": r.get("adGroupCriterion", {}).get("qualityInfo", {}).get("qualityScore"),
            "impressions": r.get("metrics", {}).get("impressions", 0),
            "clicks": r.get("metrics", {}).get("clicks", 0),
            "cost": round(int(r.get("metrics", {}).get("costMicros", 0)) / 1_000_000, 2),
            "conversions": round(float(r.get("metrics", {}).get("conversions", 0)), 2),
        } for r in rows]
        return json.dumps({"period": f"{params.start_date} to {params.end_date}", "keywords": results}, indent=2)
    except Exception as e:
        return handle_error(e)


@mcp.tool(name="get_account_summary", annotations={"readOnlyHint": True, "destructiveHint": False})
async def get_account_summary(params: DateRangeInput) -> str:
    """Get high-level account totals: spend, clicks, impressions, conversions for a date range."""
    try:
        gaql = f"""
            SELECT customer.id, customer.descriptive_name,
              metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions
            FROM customer
            WHERE segments.date BETWEEN '{params.start_date}' AND '{params.end_date}'
        """
        rows = await ads_query(params.customer_id or CUSTOMER_ID, gaql)
        totals = {"impressions": 0, "clicks": 0, "cost": 0.0, "conversions": 0.0}
        account_name = ""
        for row in rows:
            account_name = row.get("customer", {}).get("descriptiveName", "")
            m = row.get("metrics", {})
            totals["impressions"] += int(m.get("impressions", 0))
            totals["clicks"]      += int(m.get("clicks", 0))
            totals["cost"]        += int(m.get("costMicros", 0)) / 1_000_000
            totals["conversions"] += float(m.get("conversions", 0))
        totals["cost"]        = round(totals["cost"], 2)
        totals["conversions"] = round(totals["conversions"], 2)
        totals["ctr_pct"]     = round(totals["clicks"] / totals["impressions"] * 100, 2) if totals["impressions"] else 0
        totals["cost_per_conversion"] = round(totals["cost"] / totals["conversions"], 2) if totals["conversions"] else None
        return json.dumps({"account": account_name, "period": f"{params.start_date} to {params.end_date}", "summary": totals}, indent=2)
    except Exception as e:
        return handle_error(e)


# Protocol discovery endpoints required by Claude.ai (MCP spec 2025-06-18)

async def handle_head(request: Request) -> Response:
    """HEAD / - Claude uses this for protocol discovery."""
    return Response(
        status_code=200,
        headers={
            "MCP-Protocol-Version": MCP_VERSION,
            "Allow": "GET, HEAD, POST, OPTIONS",
        }
    )

async def handle_get_root(request: Request) -> Response:
    """GET / - Return server info."""
    return JSONResponse({
        "name": "google_ads_mcp",
        "version": "1.0.0",
        "protocol_version": MCP_VERSION,
        "description": "Google Ads MCP Server for Claude.ai",
    }, headers={"MCP-Protocol-Version": MCP_VERSION})

async def handle_options(request: Request) -> Response:
    """OPTIONS - CORS preflight."""
    return Response(
        status_code=200,
        headers={
            "Allow": "GET, HEAD, POST, OPTIONS",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization, Mcp-Session-Id",
            "MCP-Protocol-Version": MCP_VERSION,
        }
    )


# Build the combined ASGI app

def create_app():
    mcp_app = mcp.streamable_http_app()

    async def root_handler(scope, receive, send):
        request = Request(scope, receive)
        method = request.method.upper()
        if method == "HEAD":
            response = await handle_head(request)
        elif method == "GET":
            response = await handle_get_root(request)
        elif method == "OPTIONS":
            response = await handle_options(request)
        else:
            response = Response(status_code=405, headers={"Allow": "GET, HEAD, POST, OPTIONS"})
        await response(scope, receive, send)

    async def app(scope, receive, send):
        if scope["type"] == "http":
            path = scope.get("path", "/")
            # Route / to our discovery handler, everything else to MCP
            if path == "/" or path == "":
                await root_handler(scope, receive, send)
            else:
                await mcp_app(scope, receive, send)
        else:
            await mcp_app(scope, receive, send)

    return app


# Entry Point

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=port)
