import asyncio
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any
from datetime import date, timedelta

import httpx
from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field


app = FastAPI(title="OpenBB Platform CLI Gateway")
SEEN_FEISHU_MESSAGE_IDS: set[str] = set()
SEEN_FEISHU_CARD_ACTION_IDS: set[str] = set()

API_TOKEN = os.getenv("API_TOKEN", "")
OPENBB_COMMAND = os.getenv("OPENBB_CLI_COMMAND", "openbb")
OPENBB_TIMEOUT_SECONDS = int(os.getenv("OPENBB_TIMEOUT_SECONDS", "120"))
OPENBB_ALLOWED_PREFIXES = tuple(
    prefix.strip()
    for prefix in os.getenv(
        "OPENBB_ALLOWED_PREFIXES",
        "/equity,/economy,/index,/crypto,/etf,/currency,/commodity,/fixedincome,/derivatives,/news,/technical",
    ).split(",")
    if prefix.strip()
)
MAX_OUTPUT_CHARS = int(os.getenv("MAX_OUTPUT_CHARS", "12000"))

FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")
FEISHU_VERIFICATION_TOKEN = os.getenv("FEISHU_VERIFICATION_TOKEN", "")
FEISHU_ACK_REACTION = os.getenv("FEISHU_ACK_REACTION", "RaiseHand")
FEISHU_OPEN_REACTION = os.getenv("FEISHU_OPEN_REACTION", FEISHU_ACK_REACTION)
FEISHU_RUN_REACTION = os.getenv("FEISHU_RUN_REACTION", "OnIt")

MIKOTO_BASE_URL = os.getenv("MIKOTO_BASE_URL", "").rstrip("/")
MIKOTO_API_KEY = os.getenv("MIKOTO_API_KEY", "")
MIKOTO_MODEL = os.getenv("MIKOTO_MODEL", "gpt-5.5")
FMP_API_KEY = os.getenv("FMP_API_KEY", "")

TICKER_DIRECTORY = [
    {"symbol": "AAPL", "name": "Apple Inc.", "aliases": ["apple", "\u82f9\u679c", "iphone"]},
    {"symbol": "NVDA", "name": "NVIDIA Corporation", "aliases": ["nvidia", "\u82f1\u4f1f\u8fbe", "\u82f1\u4f1f\u8fbe"]},
    {"symbol": "MSFT", "name": "Microsoft Corporation", "aliases": ["microsoft", "\u5fae\u8f6f"]},
    {"symbol": "GOOGL", "name": "Alphabet Inc. Class A", "aliases": ["google", "alphabet", "\u8c37\u6b4c"]},
    {"symbol": "AMZN", "name": "Amazon.com Inc.", "aliases": ["amazon", "\u4e9a\u9a6c\u900a"]},
    {"symbol": "TSLA", "name": "Tesla Inc.", "aliases": ["tesla", "\u7279\u65af\u62c9"]},
    {"symbol": "META", "name": "Meta Platforms Inc.", "aliases": ["meta", "facebook", "\u8138\u4e66"]},
    {"symbol": "AMD", "name": "Advanced Micro Devices Inc.", "aliases": ["amd", "\u8d85\u5a01"]},
    {"symbol": "TSM", "name": "Taiwan Semiconductor Manufacturing", "aliases": ["tsmc", "\u53f0\u79ef\u7535", "\u53f0\u7a4d\u96fb"]},
    {"symbol": "ASML", "name": "ASML Holding N.V.", "aliases": ["asml", "\u963f\u65af\u9ea6"]},
    {"symbol": "AVGO", "name": "Broadcom Inc.", "aliases": ["broadcom", "\u535a\u901a"]},
    {"symbol": "NFLX", "name": "Netflix Inc.", "aliases": ["netflix", "\u5948\u98de"]},
    {"symbol": "BABA", "name": "Alibaba Group Holding", "aliases": ["alibaba", "\u963f\u91cc", "\u963f\u91cc\u5df4\u5df4"]},
    {"symbol": "AA", "name": "Alcoa Corporation", "aliases": ["alcoa", "\u7f8e\u94dd"]},
]

SECTION_LABELS = {
    "price": "\u8fd1\u4e00\u5e74\u80a1\u4ef7",
    "valuation": "\u4f30\u503c",
    "growth": "\u6536\u5165\u589e\u957f",
    "margin": "\u5229\u6da6\u7387",
}

OPENBB_CREDENTIAL_ENV = {
    "fmp_api_key": "FMP_API_KEY",
    "polygon_api_key": "POLYGON_API_KEY",
    "benzinga_api_key": "BENZINGA_API_KEY",
    "fred_api_key": "FRED_API_KEY",
    "nasdaq_api_key": "NASDAQ_API_KEY",
    "intrinio_api_key": "INTRINIO_API_KEY",
    "alpha_vantage_api_key": "ALPHA_VANTAGE_API_KEY",
    "biztoc_api_key": "BIZTOC_API_KEY",
    "tradier_api_key": "TRADIER_API_KEY",
    "tradier_account_type": "TRADIER_ACCOUNT_TYPE",
    "tradingeconomics_api_key": "TRADINGECONOMICS_API_KEY",
    "tiingo_token": "TIINGO_TOKEN",
}


class RoutineRequest(BaseModel):
    routine: str | None = Field(default=None, description="OpenBB Platform CLI routine text.")
    commands: list[str] | None = Field(default=None, description="One OpenBB CLI command per item.")
    timeout_seconds: int | None = Field(default=None, ge=1, le=600)


class ChatRequest(BaseModel):
    message: str
    timeout_seconds: int | None = Field(default=None, ge=1, le=600)


def extract_symbol(message: str) -> str | None:
    blocked = {"API", "CLI", "GDP", "CPI", "FRED", "FMP", "ETF", "USD", "PE", "PS"}
    for match in re.findall(r"(?<![A-Z0-9])[A-Z]{1,6}(?![A-Z0-9])", message.upper()):
        if match not in blocked:
            return match
    return None


def compact_message(message: str) -> str:
    return re.sub(r"[\s,.;:!?\u3001\uff0c\u3002\uff1b\uff1a\uff01\uff1f]+", "", message).lower()


def is_template_menu_request(message: str) -> bool:
    normalized = compact_message(message)
    return normalized in {
        "help",
        "menu",
        "template",
        "templates",
        "\u5e2e\u52a9",
        "\u83dc\u5355",
        "\u6a21\u677f",
        "\u9009\u9879",
        "\u600e\u4e48\u67e5",
        "\u5982\u4f55\u4f7f\u7528",
    }


def build_template_menu() -> str:
    return (
        "OpenBB \u6295\u7814\u67e5\u8be2\u6a21\u677f\n\n"
        "1. \u80a1\u7968\u5feb\u7167\uff08\u63a8\u8350\uff09\n"
        "\u7528\u9014\uff1a\u67e5\u4f30\u503c\u3001\u6536\u5165\u589e\u957f\u3001\u5229\u6da6\u7387\u3001\u8fd1\u4e00\u5e74\u80a1\u4ef7\u3002\n"
        "\u5feb\u6377\u5199\u6cd5\uff1a\u80a1\u7968\u5feb\u7167 NVDA\n"
        "\u5b57\u6bb5\u6a21\u677f\uff1a\n"
        "\u7c7b\u578b\uff1a\u80a1\u7968\u5feb\u7167\n"
        "\u4ee3\u7801\uff1aNVDA\n"
        "\u5e02\u573a\uff1aUS\n"
        "\u6307\u6807\uff1a\u4f30\u503c\uff0c\u6536\u5165\u589e\u957f\uff0c\u5229\u6da6\u7387\uff0c\u8fd1\u4e00\u5e74\u80a1\u4ef7\n\n"
        "2. \u81ea\u7136\u8bed\u8a00\u5199\u6cd5\n"
        "\u67e5 AAPL \u7684\u4f30\u503c\u3001\u6536\u5165\u589e\u957f\u3001\u5229\u6da6\u7387\u548c\u6700\u8fd1\u4e00\u5e74\u80a1\u4ef7\n\n"
        "\u5efa\u8bae\uff1a\u5e38\u7528\u67e5\u8be2\u5c3d\u91cf\u7528\u201c\u80a1\u7968\u5feb\u7167 \u4ee3\u7801\u201d\u6216\u5b57\u6bb5\u6a21\u677f\uff0c"
        "\u4f1a\u76f4\u63a5\u8d70 FMP \u5feb\u901f API\uff0c\u4e0d\u7b49 CLI\u3002"
    )


def normalize_search_text(text: str) -> str:
    return compact_message(re.sub(r"^(search|find|\u9009\u80a1|\u627e|\u641c|\u641c\u7d22)", "", text.strip(), flags=re.IGNORECASE))


def search_ticker_candidates(query: str, limit: int = 6) -> list[dict[str, Any]]:
    normalized = normalize_search_text(query)
    if not normalized:
        return []

    scored: list[tuple[int, dict[str, Any]]] = []
    for item in TICKER_DIRECTORY:
        symbol = item["symbol"].lower()
        aliases = [str(alias).lower() for alias in item.get("aliases", [])]
        name = str(item["name"]).lower()
        score = 0
        if normalized == symbol:
            score = 100
        elif symbol.startswith(normalized):
            score = 80
        elif normalized in aliases:
            score = 75
        elif any(alias.startswith(normalized) or normalized in alias for alias in aliases):
            score = 65
        elif normalized in name:
            score = 55
        if score:
            scored.append((score, item))

    scored.sort(key=lambda row: (-row[0], row[1]["symbol"]))
    return [item for _, item in scored[:limit]]


def plain_text(content: str) -> dict[str, str]:
    return {"tag": "plain_text", "content": content}


def markdown_text(content: str) -> dict[str, str]:
    return {"tag": "lark_md", "content": content}


def card_button(label: str, value: dict[str, Any], button_type: str = "default") -> dict[str, Any]:
    return {
        "tag": "button",
        "text": plain_text(label),
        "type": button_type,
        "value": value,
    }


def build_interactive_card(title: str, content: str, actions: list[dict[str, Any]], template: str = "blue") -> dict[str, Any]:
    elements: list[dict[str, Any]] = [{"tag": "div", "text": markdown_text(content)}]
    for i in range(0, len(actions), 3):
        elements.append({"tag": "action", "actions": actions[i : i + 3]})
    return {
        "config": {"wide_screen_mode": True},
        "header": {"template": template, "title": plain_text(title)},
        "elements": elements,
    }


def build_query_builder_card() -> dict[str, Any]:
    content = (
        "**OpenBB Platform \u6295\u7814\u603b\u63a7\u53f0**\n"
        "\u5148\u9009\u8d44\u4ea7/\u6570\u636e\u7c7b\u578b\uff0c\u518d\u9009\u6807\u7684\u3001\u6307\u6807\u3001\u5468\u671f\u548c\u8f93\u51fa\u683c\u5f0f\u3002\n\n"
        "\u6700\u540e\u540e\u7aef\u4f1a\u751f\u6210\u7ed3\u6784\u5316\u6307\u4ee4\uff0c\u518d\u8c03\u7528 OpenBB / FMP / FRED / Tiingo \u7b49\u6570\u636e\u6e90\u3002"
    )
    actions = [
        card_button("\u80a1\u7968", {"action": "asset", "asset": "equity"}, "primary"),
        card_button("ETF", {"action": "asset", "asset": "etf"}),
        card_button("\u6307\u6570", {"action": "asset", "asset": "index"}),
        card_button("\u5b8f\u89c2", {"action": "asset", "asset": "macro"}),
        card_button("\u5916\u6c47", {"action": "asset", "asset": "fx"}),
        card_button("\u52a0\u5bc6", {"action": "asset", "asset": "crypto"}),
        card_button("\u5927\u5b97\u5546\u54c1", {"action": "asset", "asset": "commodity"}),
        card_button("\u65b0\u95fb", {"action": "asset", "asset": "news"}),
    ]
    return build_interactive_card("OpenBB \u6295\u7814\u603b\u63a7\u53f0", content, actions)


def build_equity_console_card() -> dict[str, Any]:
    content = (
        "**\u80a1\u7968\u67e5\u8be2**\n"
        "\u5148\u9009\u6807\u7684\uff0c\u518d\u9009\u4f60\u8981\u7684\u529f\u80fd/\u6307\u6807\u3002\n\n"
        "\u5982\u679c\u5019\u9009\u91cc\u6ca1\u6709\uff0c\u4f60\u4e5f\u53ef\u4ee5\u76f4\u63a5\u53d1\uff1a`\u9009\u80a1 \u82f9\u679c`\u3001`\u9009\u80a1 AA`\u3001`\u9009\u80a1 NVDA`\u3002"
    )
    actions = [
        card_button("\u82f9\u679c AAPL", {"action": "select_symbol", "symbol": "AAPL", "name": "Apple Inc."}, "primary"),
        card_button("\u82f1\u4f1f\u8fbe NVDA", {"action": "select_symbol", "symbol": "NVDA", "name": "NVIDIA Corporation"}),
        card_button("\u5fae\u8f6f MSFT", {"action": "select_symbol", "symbol": "MSFT", "name": "Microsoft Corporation"}),
        card_button("\u8c37\u6b4c GOOGL", {"action": "select_symbol", "symbol": "GOOGL", "name": "Alphabet Inc. Class A"}),
        card_button("\u7279\u65af\u62c9 TSLA", {"action": "select_symbol", "symbol": "TSLA", "name": "Tesla Inc."}),
        card_button("\u4e9a\u9a6c\u900a AMZN", {"action": "select_symbol", "symbol": "AMZN", "name": "Amazon.com Inc."}),
        card_button("AA Alcoa", {"action": "select_symbol", "symbol": "AA", "name": "Alcoa Corporation"}),
        card_button("\u8fd4\u56de\u603b\u63a7\u53f0", {"action": "home"}),
    ]
    return build_interactive_card("\u80a1\u7968\u67e5\u8be2", content, actions, "blue")


def build_asset_coming_card(asset: str) -> dict[str, Any]:
    labels = {
        "etf": "ETF",
        "index": "\u6307\u6570",
        "macro": "\u5b8f\u89c2",
        "fx": "\u5916\u6c47",
        "crypto": "\u52a0\u5bc6",
        "commodity": "\u5927\u5b97\u5546\u54c1",
        "news": "\u65b0\u95fb",
    }
    label = labels.get(asset, asset)
    content = (
        f"**{label} \u6a21\u5757**\n"
        "\u8fd9\u4e2a\u5165\u53e3\u5df2\u7ecf\u4fdd\u7559\u5728\u603b\u63a7\u53f0\u91cc\uff0c\u4e0b\u4e00\u6b65\u53ef\u4ee5\u63a5\u5bf9\u5e94 OpenBB \u8def\u5f84\u3002\n\n"
        "\u8ba1\u5212\u6307\u4ee4\u683c\u5f0f\uff1a\n"
        f"`OPENBB_QUERY asset={asset} target=<symbol_or_series> metric=<metric> period=<period> format=<format>`"
    )
    actions = [
        card_button("\u8fd4\u56de\u603b\u63a7\u53f0", {"action": "home"}, "primary"),
        card_button("\u8fdb\u5165\u80a1\u7968\u6a21\u5757", {"action": "asset", "asset": "equity"}),
    ]
    return build_interactive_card(f"{label} \u6a21\u5757", content, actions, "purple")


def build_symbol_candidates_card(query: str, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    content = (
        f"\u641c\u7d22\uff1a`{query}`\n"
        "\u8bf7\u9009\u62e9\u6b63\u786e\u6807\u7684\uff0c\u4e0b\u4e00\u6b65\u518d\u9009\u8981\u67e5\u7684\u529f\u80fd\u548c\u6307\u6807\u3002"
    )
    actions = [
        card_button(
            f"{item['symbol']} {item['name'][:20]}",
            {"action": "select_symbol", "symbol": item["symbol"], "name": item["name"]},
            "primary" if idx == 0 else "default",
        )
        for idx, item in enumerate(candidates)
    ]
    return build_interactive_card("\u9009\u62e9\u6807\u7684", content, actions)


def build_metric_picker_card(symbol: str, name: str = "") -> dict[str, Any]:
    display_name = f"{symbol} {name}".strip()
    content = (
        f"\u5df2\u9009\u6807\u7684\uff1a**{display_name}**\n\n"
        "\u9009\u62e9\u8981\u67e5\u7684\u5185\u5bb9\uff0c\u540e\u7aef\u4f1a\u751f\u6210\u7ed3\u6784\u5316\u6307\u4ee4\uff1a\n"
        f"`EQUITY_SNAPSHOT symbol={symbol} sections=<selected> provider=FMP`"
    )
    actions = [
        card_button(
            "\u5168\u91cf\u5feb\u7167",
            {"action": "run_equity", "symbol": symbol, "sections": ["price", "valuation", "growth", "margin"]},
            "primary",
        ),
        card_button(
            "\u4f30\u503c+\u5229\u6da6\u7387",
            {"action": "run_equity", "symbol": symbol, "sections": ["valuation", "margin"]},
        ),
        card_button(
            "\u6536\u5165\u589e\u957f+\u5229\u6da6\u7387",
            {"action": "run_equity", "symbol": symbol, "sections": ["growth", "margin"]},
        ),
        card_button(
            "\u8fd1\u4e00\u5e74\u80a1\u4ef7",
            {"action": "run_equity", "symbol": symbol, "sections": ["price"]},
        ),
    ]
    return build_interactive_card("\u9009\u62e9\u67e5\u8be2\u5185\u5bb9", content, actions, "green")


def is_equity_research_request(message: str) -> bool:
    if not extract_symbol(message):
        return False
    keywords = (
        "\u4f30\u503c",
        "\u6536\u5165",
        "\u5229\u6da6",
        "\u80a1\u4ef7",
        "\u8d22\u62a5",
        "\u57fa\u672c\u9762",
        "\u8425\u6536",
        "\u6bdb\u5229",
        "\u51c0\u5229",
        "\u80a1\u7968\u5feb\u7167",
        "\u6295\u7814\u5feb\u7167",
        "EQUITY_SNAPSHOT",
        "SNAPSHOT",
        "PE",
        "PS",
    )
    return any(keyword in message.upper() for keyword in keywords)


def is_smalltalk_message(message: str) -> bool:
    normalized = re.sub(r"[\s,.;:!?，。！？、~～]+", "", message).lower()
    return normalized in {
        "hi",
        "hello",
        "hey",
        "test",
        "ping",
        "\u54c8\u55bd",
        "\u4f60\u597d",
        "\u5728\u5417",
        "\u6d4b\u8bd5",
    }


def fmt_number(value: Any, digits: int = 2) -> str:
    if value in (None, ""):
        return "N/A"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    abs_number = abs(number)
    if abs_number >= 1_000_000_000_000:
        return f"{number / 1_000_000_000_000:.{digits}f}T"
    if abs_number >= 1_000_000_000:
        return f"{number / 1_000_000_000:.{digits}f}B"
    if abs_number >= 1_000_000:
        return f"{number / 1_000_000:.{digits}f}M"
    return f"{number:.{digits}f}"


def fmt_percent(value: Any, digits: int = 2, ratio: bool = False) -> str:
    if value in (None, ""):
        return "N/A"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if ratio:
        number *= 100
    return f"{number:.{digits}f}%"


def safe_ratio(numerator: Any, denominator: Any) -> float | None:
    try:
        num = float(numerator)
        den = float(denominator)
    except (TypeError, ValueError):
        return None
    if den == 0:
        return None
    return num / den


async def fetch_fmp_json(client: httpx.AsyncClient, path: str, params: dict[str, str] | None = None) -> Any:
    if not FMP_API_KEY:
        raise HTTPException(status_code=500, detail="FMP_API_KEY is not configured")
    request_params = dict(params or {})
    request_params["apikey"] = FMP_API_KEY
    response = await client.get(f"https://financialmodelingprep.com/stable/{path}", params=request_params)
    response.raise_for_status()
    return response.json()


async def answer_equity_snapshot(message: str) -> str | None:
    symbol = extract_symbol(message)
    if not symbol or not FMP_API_KEY:
        return None

    today = date.today()
    start_date = today - timedelta(days=370)
    print(f"Fast equity snapshot path: symbol={symbol}", flush=True)
    async with httpx.AsyncClient(timeout=45) as client:
        quote_task = fetch_fmp_json(client, "quote", {"symbol": symbol})
        metrics_task = fetch_fmp_json(client, "key-metrics-ttm", {"symbol": symbol})
        income_task = fetch_fmp_json(client, "income-statement", {"symbol": symbol, "period": "annual", "limit": "5"})
        history_task = fetch_fmp_json(
            client,
            "historical-price-eod/full",
            {"symbol": symbol, "from": start_date.isoformat(), "to": today.isoformat()},
        )
        quote, metrics, income, history = await asyncio.gather(
            quote_task,
            metrics_task,
            income_task,
            history_task,
        )

    quote_row = quote[0] if isinstance(quote, list) and quote else {}
    metrics_row = metrics[0] if isinstance(metrics, list) and metrics else {}
    income_rows = income if isinstance(income, list) else []
    latest_income = income_rows[0] if income_rows else {}
    previous_income = income_rows[1] if len(income_rows) > 1 else {}

    historical = history.get("historical", []) if isinstance(history, dict) else history if isinstance(history, list) else []
    latest_close = historical[0].get("close") if historical else quote_row.get("price")
    first_close = historical[-1].get("close") if historical else None
    one_year_return = None
    if latest_close and first_close:
        one_year_return = (float(latest_close) / float(first_close) - 1) * 100

    revenue_growth = None
    if latest_income.get("revenue") and previous_income.get("revenue"):
        revenue_growth = (float(latest_income["revenue"]) / float(previous_income["revenue"]) - 1) * 100

    pe_value = (
        quote_row.get("pe")
        or metrics_row.get("peRatioTTM")
        or (1 / float(metrics_row["earningsYieldTTM"]) if metrics_row.get("earningsYieldTTM") else None)
        or safe_ratio(quote_row.get("price"), latest_income.get("epsDiluted") or latest_income.get("eps"))
    )
    ps_value = metrics_row.get("priceToSalesRatioTTM") or safe_ratio(quote_row.get("marketCap"), latest_income.get("revenue"))
    ev_ebitda_value = metrics_row.get("enterpriseValueOverEBITDATTM") or metrics_row.get("evToEBITDATTM")
    gross_margin = latest_income.get("grossProfitRatio") or safe_ratio(latest_income.get("grossProfit"), latest_income.get("revenue"))
    operating_margin = latest_income.get("operatingIncomeRatio") or safe_ratio(
        latest_income.get("operatingIncome"),
        latest_income.get("revenue"),
    )
    net_margin = latest_income.get("netIncomeRatio") or safe_ratio(latest_income.get("netIncome"), latest_income.get("revenue"))

    company_name = quote_row.get("name") or symbol
    lines = [
        f"{symbol} {company_name} 投研快照",
        "",
        "数据源：FMP，走快速 API 路径，未走交互式 CLI。",
        "",
        "股价",
        f"- 最新价格：{fmt_number(quote_row.get('price') or latest_close)}",
        f"- 近一年涨跌幅：{fmt_percent(one_year_return)}",
        f"- 市值：{fmt_number(quote_row.get('marketCap'))}",
        "",
        "估值",
        f"- PE：{fmt_number(pe_value)}",
        f"- PS：{fmt_number(ps_value)}",
        f"- EV/EBITDA：{fmt_number(ev_ebitda_value)}",
        "",
        "收入与利润率",
        f"- 最近年度收入：{fmt_number(latest_income.get('revenue'))}",
        f"- 收入同比增长：{fmt_percent(revenue_growth)}",
        f"- 毛利率：{fmt_percent(gross_margin, ratio=True)}",
        f"- 营业利润率：{fmt_percent(operating_margin, ratio=True)}",
        f"- 净利率：{fmt_percent(net_margin, ratio=True)}",
        "",
        "简评",
        "这是一版快速数据摘要；后续可以再加同行对比、历史估值分位和图表卡片。",
    ]
    return "\n".join(lines)


async def answer_equity_snapshot_by_symbol(symbol: str, sections: list[str] | set[str] | None = None) -> str:
    selected_sections = set(sections or ["price", "valuation", "growth", "margin"])
    if not FMP_API_KEY:
        raise HTTPException(status_code=500, detail="FMP_API_KEY is not configured")

    today = date.today()
    start_date = today - timedelta(days=370)
    symbol = symbol.upper()
    print(f"Card equity snapshot path: symbol={symbol}, sections={','.join(sorted(selected_sections))}", flush=True)
    async with httpx.AsyncClient(timeout=45) as client:
        quote_task = fetch_fmp_json(client, "quote", {"symbol": symbol})
        metrics_task = fetch_fmp_json(client, "key-metrics-ttm", {"symbol": symbol})
        income_task = fetch_fmp_json(client, "income-statement", {"symbol": symbol, "period": "annual", "limit": "5"})
        history_task = fetch_fmp_json(
            client,
            "historical-price-eod/full",
            {"symbol": symbol, "from": start_date.isoformat(), "to": today.isoformat()},
        )
        quote, metrics, income, history = await asyncio.gather(
            quote_task,
            metrics_task,
            income_task,
            history_task,
        )

    quote_row = quote[0] if isinstance(quote, list) and quote else {}
    metrics_row = metrics[0] if isinstance(metrics, list) and metrics else {}
    income_rows = income if isinstance(income, list) else []
    latest_income = income_rows[0] if income_rows else {}
    previous_income = income_rows[1] if len(income_rows) > 1 else {}
    historical = history.get("historical", []) if isinstance(history, dict) else history if isinstance(history, list) else []
    latest_close = historical[0].get("close") if historical else quote_row.get("price")
    first_close = historical[-1].get("close") if historical else None

    one_year_return = None
    if latest_close and first_close:
        one_year_return = (float(latest_close) / float(first_close) - 1) * 100

    revenue_growth = None
    if latest_income.get("revenue") and previous_income.get("revenue"):
        revenue_growth = (float(latest_income["revenue"]) / float(previous_income["revenue"]) - 1) * 100

    pe_value = (
        quote_row.get("pe")
        or metrics_row.get("peRatioTTM")
        or (1 / float(metrics_row["earningsYieldTTM"]) if metrics_row.get("earningsYieldTTM") else None)
        or safe_ratio(quote_row.get("price"), latest_income.get("epsDiluted") or latest_income.get("eps"))
    )
    ps_value = metrics_row.get("priceToSalesRatioTTM") or safe_ratio(quote_row.get("marketCap"), latest_income.get("revenue"))
    ev_ebitda_value = metrics_row.get("enterpriseValueOverEBITDATTM") or metrics_row.get("evToEBITDATTM")
    gross_margin = latest_income.get("grossProfitRatio") or safe_ratio(latest_income.get("grossProfit"), latest_income.get("revenue"))
    operating_margin = latest_income.get("operatingIncomeRatio") or safe_ratio(
        latest_income.get("operatingIncome"),
        latest_income.get("revenue"),
    )
    net_margin = latest_income.get("netIncomeRatio") or safe_ratio(latest_income.get("netIncome"), latest_income.get("revenue"))

    company_name = quote_row.get("name") or symbol
    section_names = "\uff0c".join(SECTION_LABELS.get(section, section) for section in selected_sections)
    lines = [
        f"{symbol} {company_name} \u6295\u7814\u67e5\u8be2",
        "",
        f"\u5185\u90e8\u6307\u4ee4\uff1aEQUITY_SNAPSHOT symbol={symbol} sections={','.join(sorted(selected_sections))} provider=FMP",
        f"\u5df2\u9009\u5185\u5bb9\uff1a{section_names}",
        "",
        "\u6570\u636e\u6e90\uff1aFMP\uff0c\u8d70\u5feb\u901f API \u8def\u5f84\uff0c\u672a\u8d70\u4ea4\u4e92\u5f0f CLI\u3002",
        "",
    ]
    if "price" in selected_sections:
        lines.extend([
            "\u80a1\u4ef7",
            f"- \u6700\u65b0\u4ef7\u683c\uff1a{fmt_number(quote_row.get('price') or latest_close)}",
            f"- \u8fd1\u4e00\u5e74\u6da8\u8dcc\u5e45\uff1a{fmt_percent(one_year_return)}",
            f"- \u5e02\u503c\uff1a{fmt_number(quote_row.get('marketCap'))}",
            "",
        ])
    if "valuation" in selected_sections:
        lines.extend([
            "\u4f30\u503c",
            f"- PE\uff1a{fmt_number(pe_value)}",
            f"- PS\uff1a{fmt_number(ps_value)}",
            f"- EV/EBITDA\uff1a{fmt_number(ev_ebitda_value)}",
            "",
        ])
    if {"growth", "margin"} & selected_sections:
        lines.append("\u6536\u5165\u4e0e\u5229\u6da6\u7387")
        lines.append(f"- \u6700\u8fd1\u5e74\u5ea6\u6536\u5165\uff1a{fmt_number(latest_income.get('revenue'))}")
        if "growth" in selected_sections:
            lines.append(f"- \u6536\u5165\u540c\u6bd4\u589e\u957f\uff1a{fmt_percent(revenue_growth)}")
        if "margin" in selected_sections:
            lines.extend([
                f"- \u6bdb\u5229\u7387\uff1a{fmt_percent(gross_margin, ratio=True)}",
                f"- \u8425\u4e1a\u5229\u6da6\u7387\uff1a{fmt_percent(operating_margin, ratio=True)}",
                f"- \u51c0\u5229\u7387\uff1a{fmt_percent(net_margin, ratio=True)}",
            ])

    return "\n".join(lines).strip()


def write_openbb_user_settings() -> None:
    credentials = {
        key: value
        for key, env_name in OPENBB_CREDENTIAL_ENV.items()
        if (value := os.getenv(env_name, "").strip())
    }
    if not credentials:
        return

    settings_dir = Path.home() / ".openbb_platform"
    settings_dir.mkdir(parents=True, exist_ok=True)
    settings_path = settings_dir / "user_settings.json"
    settings_path.write_text(
        json.dumps({"credentials": credentials}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def require_api_token(authorization: str | None) -> None:
    if not API_TOKEN:
        return
    expected = f"Bearer {API_TOKEN}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="Invalid API token")


def normalize_routine(payload: RoutineRequest) -> str:
    if payload.routine and payload.commands:
        raise HTTPException(status_code=400, detail="Use either routine or commands, not both")

    if payload.routine:
        lines = payload.routine.splitlines()
    elif payload.commands:
        lines = payload.commands
    else:
        raise HTTPException(status_code=400, detail="Missing routine or commands")

    cleaned = []
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if not line.startswith(OPENBB_ALLOWED_PREFIXES):
            raise HTTPException(status_code=400, detail=f"Command is not allow-listed: {line}")
        cleaned.append(line)

    if not cleaned:
        raise HTTPException(status_code=400, detail="Routine contains no executable commands")
    return "\n".join(cleaned) + "\n"


async def run_openbb_routine(routine: str, timeout_seconds: int) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as temp_dir:
        routine_path = Path(temp_dir) / "routine.openbb"
        routine_path.write_text(routine, encoding="utf-8")

        proc = await asyncio.create_subprocess_exec(
            OPENBB_COMMAND,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        cli_input = f"/exe --file {routine_path}\nexit\n".encode("utf-8")
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(cli_input),
                timeout=timeout_seconds,
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise HTTPException(status_code=504, detail="OpenBB CLI routine timed out")

    stdout_text = stdout.decode("utf-8", errors="replace")
    stderr_text = stderr.decode("utf-8", errors="replace")
    return {
        "returncode": proc.returncode,
        "stdout": stdout_text[-MAX_OUTPUT_CHARS:],
        "stderr": stderr_text[-MAX_OUTPUT_CHARS:],
    }


async def call_model(messages: list[dict[str, str]], max_tokens: int = 1200) -> str:
    if not MIKOTO_BASE_URL or not MIKOTO_API_KEY:
        raise HTTPException(status_code=500, detail="Model endpoint is not configured")

    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(
            f"{MIKOTO_BASE_URL}/v1/chat/completions",
            headers={"Authorization": f"Bearer {MIKOTO_API_KEY}"},
            json={
                "model": MIKOTO_MODEL,
                "messages": messages,
                "max_tokens": max_tokens,
            },
        )
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"].strip()


def extract_commands(model_text: str) -> list[str]:
    lines = []
    for raw_line in model_text.splitlines():
        line = raw_line.strip().strip("`")
        if line.startswith(tuple(OPENBB_ALLOWED_PREFIXES)):
            lines.append(line)
    return lines[:5]


async def answer_with_openbb(message: str, timeout_seconds: int) -> str:
    if is_template_menu_request(message):
        return build_template_menu()

    if is_equity_research_request(message):
        try:
            equity_answer = await answer_equity_snapshot(message)
            if equity_answer:
                return equity_answer
        except Exception as exc:
            print(f"Fast equity snapshot failed: {type(exc).__name__}: {exc}", flush=True)
            return f"股票快照查询失败：{type(exc).__name__}: {exc}"
        return "股票快照查询失败：没有识别到可用股票代码或 FMP_API_KEY 未配置。"

    command_text = await call_model(
        [
            {
                "role": "system",
                "content": (
                    "Convert the user request into OpenBB Platform CLI routine commands. "
                    "Return only commands, one per line. Use current OpenBB Platform paths. "
                    "Do not include shell commands or explanations."
                ),
            },
            {"role": "user", "content": message},
        ],
        max_tokens=600,
    )
    commands = extract_commands(command_text)
    if not commands:
        return (
            "我收到了，但这句话没有被识别成可执行的 OpenBB 数据查询。\n"
            "可以这样问：查 AAPL 的估值、收入增长、利润率和最近一年股价。"
        )

    result = await run_openbb_routine("\n".join(commands) + "\n", timeout_seconds)
    summary = await call_model(
        [
            {
                "role": "system",
                "content": "Summarize the OpenBB CLI output for an investment research chat. Be concise.",
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "question": message,
                        "commands": commands,
                        "returncode": result["returncode"],
                        "stdout": result["stdout"],
                        "stderr": result["stderr"],
                    },
                    ensure_ascii=False,
                ),
            },
        ],
        max_tokens=1200,
    )
    return summary


async def get_feishu_tenant_token() -> str:
    if not FEISHU_APP_ID or not FEISHU_APP_SECRET:
        raise HTTPException(status_code=500, detail="Feishu credentials are not configured")
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET},
        )
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise HTTPException(status_code=502, detail=data)
        return data["tenant_access_token"]


async def reply_feishu_message(message_id: str, text: str) -> None:
    token = await get_feishu_tenant_token()
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reply",
            headers={"Authorization": f"Bearer {token}"},
            json={"msg_type": "text", "content": json.dumps({"text": text}, ensure_ascii=False)},
        )
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise HTTPException(status_code=502, detail=data)


async def reply_feishu_card(message_id: str, card: dict[str, Any]) -> None:
    token = await get_feishu_tenant_token()
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reply",
            headers={"Authorization": f"Bearer {token}"},
            json={"msg_type": "interactive", "content": json.dumps(card, ensure_ascii=False)},
        )
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise HTTPException(status_code=502, detail=data)


async def add_feishu_reaction(message_id: str, emoji_type: str = FEISHU_ACK_REACTION) -> None:
    token = await get_feishu_tenant_token()
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reactions",
            headers={"Authorization": f"Bearer {token}"},
            json={"reaction_type": {"emoji_type": emoji_type}},
        )
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise HTTPException(status_code=502, detail=data)


async def process_feishu_query(message_id: str, text: str) -> None:
    try:
        answer = await answer_with_openbb(text, OPENBB_TIMEOUT_SECONDS)
    except Exception as exc:
        answer = f"查询过程中出错了：{type(exc).__name__}: {exc}"
    await reply_feishu_message(message_id, answer[:3000])


async def process_equity_card_query(message_id: str, symbol: str, sections: list[str]) -> None:
    try:
        await add_feishu_reaction(message_id, FEISHU_RUN_REACTION)
    except Exception as exc:
        print(f"Failed to add Feishu run reaction: {type(exc).__name__}: {exc}", flush=True)
    try:
        answer = await answer_equity_snapshot_by_symbol(symbol, sections)
    except Exception as exc:
        answer = f"\u67e5\u8be2\u8fc7\u7a0b\u4e2d\u51fa\u9519\u4e86\uff1a{type(exc).__name__}: {exc}"
    await reply_feishu_message(message_id, answer[:3000])


def recursive_find_key(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        if key in value:
            return value[key]
        for child in value.values():
            found = recursive_find_key(child, key)
            if found:
                return found
    elif isinstance(value, list):
        for item in value:
            found = recursive_find_key(item, key)
            if found:
                return found
    return None


def extract_card_action_value(body: dict[str, Any]) -> dict[str, Any]:
    value = recursive_find_key(body, "value")
    return value if isinstance(value, dict) else {}


def extract_action_message_id(body: dict[str, Any]) -> str:
    for key in ("open_message_id", "message_id"):
        value = recursive_find_key(body, key)
        if isinstance(value, str) and value:
            return value
    return ""


async def handle_feishu_card_action(body: dict[str, Any], background_tasks: BackgroundTasks) -> dict[str, Any]:
    action_value = extract_card_action_value(body)
    action = action_value.get("action")
    message_id = extract_action_message_id(body)
    action_id = recursive_find_key(body, "event_id")
    if action_id:
        action_id = str(action_id)
        if action_id in SEEN_FEISHU_CARD_ACTION_IDS:
            return {"toast": {"type": "info", "content": "\u5df2\u6536\u5230\uff0c\u6b63\u5728\u5904\u7406"}}
        SEEN_FEISHU_CARD_ACTION_IDS.add(action_id)

    if not message_id:
        print(f"Card action missing message id: {json.dumps(body, ensure_ascii=False)[:1000]}", flush=True)
        return {"toast": {"type": "warning", "content": "\u672a\u627e\u5230\u53ef\u56de\u590d\u7684\u6d88\u606f"}}

    if action == "home":
        await reply_feishu_card(message_id, build_query_builder_card())
        return {"toast": {"type": "success", "content": "\u5df2\u8fd4\u56de\u603b\u63a7\u53f0"}}

    if action == "asset":
        asset = str(action_value.get("asset", ""))
        card = build_equity_console_card() if asset == "equity" else build_asset_coming_card(asset)
        await reply_feishu_card(message_id, card)
        return {"toast": {"type": "success", "content": "\u5df2\u6253\u5f00\u6a21\u5757"}}

    if action == "select_symbol":
        symbol = str(action_value.get("symbol", "")).upper()
        name = str(action_value.get("name", ""))
        if not symbol:
            return {"toast": {"type": "warning", "content": "\u672a\u8bc6\u522b\u6807\u7684"}}
        await reply_feishu_card(message_id, build_metric_picker_card(symbol, name))
        return {"toast": {"type": "success", "content": f"\u5df2\u9009 {symbol}"}}

    if action == "run_equity":
        symbol = str(action_value.get("symbol", "")).upper()
        sections = action_value.get("sections") or ["price", "valuation", "growth", "margin"]
        if not symbol:
            return {"toast": {"type": "warning", "content": "\u672a\u8bc6\u522b\u6807\u7684"}}
        if not isinstance(sections, list):
            sections = ["price", "valuation", "growth", "margin"]
        background_tasks.add_task(process_equity_card_query, message_id, symbol, sections)
        return {"toast": {"type": "success", "content": "\u5df2\u751f\u6210\u6307\u4ee4\u5e76\u5f00\u59cb\u67e5\u8be2"}}

    return {"toast": {"type": "warning", "content": "\u672a\u8bc6\u522b\u7684\u5361\u7247\u64cd\u4f5c"}}


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "runtime": "openbb-platform-cli"}


@app.on_event("startup")
async def startup() -> None:
    write_openbb_user_settings()


@app.post("/routine")
async def routine_endpoint(payload: RoutineRequest, authorization: str | None = Header(default=None)) -> dict[str, Any]:
    require_api_token(authorization)
    routine = normalize_routine(payload)
    return await run_openbb_routine(routine, payload.timeout_seconds or OPENBB_TIMEOUT_SECONDS)


@app.post("/chat")
async def chat_endpoint(payload: ChatRequest, authorization: str | None = Header(default=None)) -> dict[str, str]:
    require_api_token(authorization)
    text = await answer_with_openbb(payload.message, payload.timeout_seconds or OPENBB_TIMEOUT_SECONDS)
    return {"text": text}


@app.post("/feishu/events")
async def feishu_events(request: Request, background_tasks: BackgroundTasks) -> dict[str, Any]:
    body = await request.json()
    if "challenge" in body:
        return {"challenge": body["challenge"]}

    request_token = body.get("token") or body.get("header", {}).get("token")
    if FEISHU_VERIFICATION_TOKEN and request_token != FEISHU_VERIFICATION_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid Feishu verification token")

    if extract_card_action_value(body).get("action"):
        return await handle_feishu_card_action(body, background_tasks)

    event = body.get("event", {})
    message = event.get("message", {})
    message_id = message.get("message_id", "")
    if not message_id:
        return {"status": "ignored"}
    if message_id in SEEN_FEISHU_MESSAGE_IDS:
        return {"status": "duplicate"}
    SEEN_FEISHU_MESSAGE_IDS.add(message_id)

    content = message.get("content", "{}")
    try:
        text = json.loads(content).get("text", "")
    except json.JSONDecodeError:
        text = content
    text = re.sub(r"@\S+", "", text).strip()

    try:
        await add_feishu_reaction(message_id, FEISHU_OPEN_REACTION)
    except Exception as exc:
        print(f"Failed to add Feishu reaction: {type(exc).__name__}: {exc}", flush=True)

    if re.match(r"^(search|find|\u9009\u80a1|\u627e|\u641c|\u641c\u7d22)\s+", text, flags=re.IGNORECASE):
        candidates = search_ticker_candidates(text)
        if candidates:
            await reply_feishu_card(message_id, build_symbol_candidates_card(text, candidates))
            return {"status": "candidate_card_sent"}

    await reply_feishu_card(message_id, build_query_builder_card())
    return {"status": "console_card_sent"}
