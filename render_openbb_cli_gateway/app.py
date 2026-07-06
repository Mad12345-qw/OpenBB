import asyncio
import inspect
import json
import os
import re
import shlex
import tempfile
from pathlib import Path
from typing import Any
from datetime import date, datetime, time, timedelta, timezone

import httpx
from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field


app = FastAPI(title="OpenBB Platform CLI Gateway")
SEEN_FEISHU_MESSAGE_IDS: set[str] = set()
SEEN_FEISHU_CARD_ACTION_IDS: set[str] = set()
RECENT_FEISHU_TASKS: dict[str, dict[str, Any]] = {}
MAX_RECENT_FEISHU_TASKS = 50
TASK_STATE_PATH = Path(
    os.getenv(
        "OPENBB_TASK_STATE_PATH",
        str(Path(tempfile.gettempdir()) / "openbb_feishu_tasks.json"),
    )
)
INTERRUPTED_TASK_STATUSES = {
    "running",
    "translating",
    "running_cli",
    "summarizing",
    "replying",
}

API_TOKEN = os.getenv("API_TOKEN", "")
OPENBB_COMMAND = os.getenv("OPENBB_CLI_COMMAND", "openbb")
OPENBB_TIMEOUT_SECONDS = int(os.getenv("OPENBB_TIMEOUT_SECONDS", "120"))
OPENBB_EXECUTION_MODE = os.getenv("OPENBB_EXECUTION_MODE", "platform_api").strip().lower()
OPENBB_PLATFORM_PREWARM = os.getenv("OPENBB_PLATFORM_PREWARM", "1").strip().lower() in {"1", "true", "yes", "on"}
OPENBB_PLATFORM_MAX_ROWS = int(os.getenv("OPENBB_PLATFORM_MAX_ROWS", "80"))
OPENBB_FAST_EQUITY_SNAPSHOT = os.getenv("OPENBB_FAST_EQUITY_SNAPSHOT", "0").strip().lower() in {"1", "true", "yes", "on"}
OPENBB_ALLOWED_PREFIXES = tuple(
    prefix.strip()
    for prefix in os.getenv(
        "OPENBB_ALLOWED_PREFIXES",
        "/equity,/economy,/index,/crypto,/etf,/currency,/commodity,/fixedincome,/derivatives,/news,/technical",
    ).split(",")
    if prefix.strip()
)
OPENBB_REQUIRE_ALLOWED_PREFIX = os.getenv("OPENBB_REQUIRE_ALLOWED_PREFIX", "0").strip().lower() in {"1", "true", "yes", "on"}
MAX_OUTPUT_CHARS = int(os.getenv("MAX_OUTPUT_CHARS", "12000"))

FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")
FEISHU_VERIFICATION_TOKEN = os.getenv("FEISHU_VERIFICATION_TOKEN", "")
FEISHU_ACK_REACTION = os.getenv("FEISHU_ACK_REACTION", "OneSecond")
FEISHU_OPEN_REACTION = os.getenv("FEISHU_OPEN_REACTION", FEISHU_ACK_REACTION)
FEISHU_RUN_REACTION = os.getenv("FEISHU_RUN_REACTION", "OnIt")

MIKOTO_BASE_URL = os.getenv("MIKOTO_BASE_URL", "").rstrip("/")
MIKOTO_API_KEY = os.getenv("MIKOTO_API_KEY", "")
MIKOTO_MODEL = os.getenv("MIKOTO_MODEL", "gpt-5.5")
FMP_API_KEY = os.getenv("FMP_API_KEY", "")

OPENBB_OBB: Any | None = None
OPENBB_IMPORT_LOCK: asyncio.Lock | None = None

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
    {"symbol": "0700.HK", "name": "Tencent Holdings Limited", "aliases": ["tencent", "\u817e\u8baf", "\u9a30\u8a0a", "0700"]},
    {"symbol": "7203.T", "name": "Toyota Motor Corp.", "aliases": ["toyota", "\u4e30\u7530", "\u8c50\u7530", "7203"]},
    {"symbol": "9988.HK", "name": "Alibaba Group Holding Limited", "aliases": ["alibaba hk", "\u963f\u91cc\u6e2f\u80a1", "9988"]},
    {"symbol": "3690.HK", "name": "Meituan", "aliases": ["meituan", "\u7f8e\u56e2", "\u7f8e\u5718", "3690"]},
    {"symbol": "1810.HK", "name": "Xiaomi Corporation", "aliases": ["xiaomi", "\u5c0f\u7c73", "1810"]},
]

SECTION_LABELS = {
    "price": "\u8fd1\u4e00\u5e74\u80a1\u4ef7",
    "valuation": "\u4f30\u503c",
    "growth": "\u6536\u5165\u589e\u957f",
    "margin": "\u5229\u6da6\u7387",
}

INDEX_DIRECTORY = [
    {"symbol": "^GSPC", "name": "S&P 500"},
    {"symbol": "^IXIC", "name": "NASDAQ Composite"},
    {"symbol": "^DJI", "name": "Dow Jones Industrial Average"},
]

CRYPTO_DIRECTORY = [
    {"symbol": "BTCUSD", "name": "Bitcoin USD"},
    {"symbol": "ETHUSD", "name": "Ethereum USD"},
]

FRED_SERIES = [
    {"series_id": "GDP", "name": "US GDP"},
    {"series_id": "CPIAUCSL", "name": "US CPI"},
    {"series_id": "UNRATE", "name": "US Unemployment Rate"},
    {"series_id": "FEDFUNDS", "name": "Federal Funds Rate"},
    {"series_id": "DGS10", "name": "10-Year Treasury Yield"},
]

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


class UnsupportedOpenBBPlatformCommand(Exception):
    pass


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


def parse_search_command(text: str) -> tuple[str, str] | None:
    raw = (text or "").strip()
    if not raw:
        return None

    english_match = re.match(r"^(?:search|find)\s+(?:(stock|equity|etf)\s+)?(.+)$", raw, flags=re.IGNORECASE)
    if english_match:
        asset_word = (english_match.group(1) or "").lower()
        query = english_match.group(2).strip()
        asset = "etf" if asset_word == "etf" else "equity"
        return (asset, query) if query else None

    patterns: list[tuple[str, str]] = [
        (r"^(?:\u641c\u7d22ETF|\u641cETF|ETF\u641c\u7d22|ETF)\s*(.+)$", "etf"),
        (r"^(?:\u641c\u7d22\u80a1\u7968|\u641c\u80a1\u7968|\u9009\u80a1|\u80a1\u7968\u641c\u7d22|\u80a1\u7968)\s*(.+)$", "equity"),
        (r"^(?:\u641c\u7d22|\u641c|\u627e)\s+(.+)$", "equity"),
    ]
    for pattern, asset in patterns:
        match = re.match(pattern, raw, flags=re.IGNORECASE)
        if match:
            query = match.group(1).strip()
            return (asset, query) if query else None
    return None


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
        "**OpenBB Platform CLI \u76f4\u8fde\u6a21\u5f0f**\n"
        "\u73b0\u5728\u98de\u4e66\u53ea\u505a\u4fe1\u606f\u4f20\u9012\uff1a\u4f60\u76f4\u63a5\u8bf4\u8981\u67e5\u4ec0\u4e48\uff0c"
        "\u540e\u7aef\u4f1a\u628a\u8bf7\u6c42\u8f6c\u6210 OpenBB Platform CLI routine\uff0c\u5728 Render \u4e0a\u7528\u5b8c\u6574 CLI \u6267\u884c\uff0c\u7136\u540e\u628a CLI \u7ed3\u679c\u56de\u5230\u98de\u4e66\u3002\n\n"
        "\u4e0d\u518d\u7528\u81ea\u5b9a\u4e49 FMP/Yahoo \u5feb\u7167\u66ff\u4ee3 OpenBB CLI\u3002"
    )
    actions = [
        card_button("AAPL \u5b8c\u6574\u516c\u53f8\u4fe1\u606f", {"action": "cli_query", "query": "\u67e5 AAPL \u7684\u516c\u53f8\u6982\u51b5\u3001\u884c\u60c5\u3001\u4f30\u503c\u3001\u8d22\u52a1\u3001\u6536\u5165\u589e\u957f\u3001\u5229\u6da6\u7387\u3001\u7ba1\u7406\u5c42\u548c\u62c6\u80a1"}, "primary"),
        card_button("\u817e\u8baf\u5b8c\u6574\u516c\u53f8\u4fe1\u606f", {"action": "cli_query", "query": "\u67e5 0700.HK \u7684\u516c\u53f8\u6982\u51b5\u3001\u884c\u60c5\u3001\u4f30\u503c\u3001\u8d22\u52a1\u3001\u6536\u5165\u589e\u957f\u3001\u5229\u6da6\u7387\u3001\u7ba1\u7406\u5c42\u548c\u62c6\u80a1"}),
    ]
    return build_interactive_card("OpenBB CLI \u76f4\u8fde", content, actions)


def build_equity_console_card() -> dict[str, Any]:
    content = (
        "**\u5168\u5e02\u573a\u80a1\u7968\u641c\u7d22**\n"
        "\u8fd9\u91cc\u4e0d\u662f\u56fa\u5b9a\u80a1\u7968\u5217\u8868\uff0c\u800c\u662f\u5168\u5e02\u573a\u641c\u7d22\u5165\u53e3\u3002\n\n"
        "\u8bf7\u76f4\u63a5\u5728\u804a\u5929\u6846\u53d1\u9001\uff1a\n"
        "`\u641c\u80a1\u7968 \u4efb\u610f\u516c\u53f8\u540d/\u4ee3\u7801`\uff0c\u4e2d\u95f4\u6709\u6ca1\u6709\u7a7a\u683c\u90fd\u53ef\u4ee5\u3002\n\n"
        "\u793a\u4f8b\uff1a\n"
        "`\u641c\u80a1\u7968 AAPL`\n"
        "`\u641c\u80a1\u7968\u817e\u8baf`\n"
        "`\u641c\u80a1\u7968 \u82f9\u679c`\n"
        "`\u641c\u80a1\u7968 0700`\n"
        "`\u641c\u80a1\u7968 Toyota`\n"
        "`\u641c\u80a1\u7968 7203`\n\n"
        "\u6211\u4f1a\u8fd4\u56de\u5019\u9009\u6807\u7684\u5361\u7247\uff0c\u4f60\u70b9\u9009\u540e\uff0c\u540e\u7aef\u518d\u68c0\u6d4b\u8be5\u6807\u7684\u54ea\u4e9b\u6307\u6807\u771f\u5b9e\u53ef\u7528\u3002"
    )
    actions = [
        card_button("\u8fd4\u56de\u603b\u63a7\u53f0", {"action": "home"}),
    ]
    return build_interactive_card("\u80a1\u7968\u67e5\u8be2", content, actions, "blue")


def build_etf_console_card() -> dict[str, Any]:
    content = (
        "**ETF \u641c\u7d22**\n"
        "\u8fd9\u91cc\u4e0d\u662f\u56fa\u5b9a ETF \u5217\u8868\uff0c\u800c\u662f ETF \u641c\u7d22\u5165\u53e3\u3002\n\n"
        "\u8bf7\u76f4\u63a5\u5728\u804a\u5929\u6846\u53d1\u9001\uff1a\n"
        "`\u641cETF \u4efb\u610f ETF \u4ee3\u7801/\u540d\u79f0`\uff0c\u4e2d\u95f4\u6709\u6ca1\u6709\u7a7a\u683c\u90fd\u53ef\u4ee5\u3002\n\n"
        "\u793a\u4f8b\uff1a\n"
        "`\u641cETF SPY`\n"
        "`\u641cETFSPY`\n"
        "`\u641cETF QQQ`\n"
        "`\u641cETF VOO`\n\n"
        "\u70b9\u9009\u5019\u9009 ETF \u540e\uff0c\u6211\u4f1a\u751f\u6210 ETF \u884c\u60c5\u6307\u4ee4\u5e76\u8fd4\u56de\u4ef7\u683c/\u8fd1\u4e00\u5e74\u8868\u73b0\u3002"
    )
    actions = [
        card_button("\u8fd4\u56de\u603b\u63a7\u53f0", {"action": "home"}),
    ]
    return build_interactive_card("ETF \u641c\u7d22", content, actions, "blue")


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


def build_symbol_candidates_card(query: str, candidates: list[dict[str, Any]], asset: str = "equity") -> dict[str, Any]:
    content = (
        f"\u641c\u7d22\uff1a`{query}`\n"
        "\u8bf7\u9009\u62e9\u6b63\u786e\u6807\u7684\uff0c\u4e0b\u4e00\u6b65\u518d\u9009\u8981\u67e5\u7684\u529f\u80fd\u548c\u6307\u6807\u3002"
    )
    actions = [
        card_button(
            f"{item['symbol']} {item.get('exchange', '')} {item['name'][:18]}",
            {"action": "select_symbol", "asset": asset, "symbol": item["symbol"], "name": item["name"]},
            "primary" if idx == 0 else "default",
        )
        for idx, item in enumerate(candidates)
    ]
    return build_interactive_card("\u9009\u62e9\u6807\u7684", content, actions)


def build_search_empty_card(query: str, asset: str = "equity") -> dict[str, Any]:
    asset_label = "ETF" if asset == "etf" else "\u80a1\u7968"
    example = "\u641cETF SPY" if asset == "etf" else "\u641c\u80a1\u7968 0700.HK"
    content = (
        f"**{asset_label}\u641c\u7d22\u6ca1\u6709\u8fd4\u56de\u5019\u9009**\n"
        f"\u5df2\u8bc6\u522b\u5230\u4f60\u8981\u641c\u7d22\uff1a`{query}`\n\n"
        "\u8fd9\u6b21\u6ca1\u6709\u8fd4\u56de\u5019\u9009\uff0c\u5e38\u89c1\u539f\u56e0\u662f FMP \u641c\u7d22\u63a5\u53e3\u4e34\u65f6\u9650\u6d41\uff0c"
        "\u6216\u516c\u53f8\u540d\u9700\u8981\u66f4\u660e\u786e\u7684\u4ee3\u7801/\u82f1\u6587\u540d\u3002\n\n"
        f"\u53ef\u4ee5\u6539\u53d1\uff1a`{example}`\uff0c\u6216\u7a0d\u540e\u91cd\u8bd5\u540c\u4e00\u4e2a\u641c\u7d22\u3002"
    )
    actions = [
        card_button("\u8fd4\u56de\u603b\u63a7\u53f0", {"action": "home"}),
    ]
    return build_interactive_card("\u641c\u7d22\u672a\u547d\u4e2d", content, actions, "yellow")


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


def build_price_picker_card(symbol: str, name: str = "", asset: str = "instrument") -> dict[str, Any]:
    display_name = f"{symbol} {name}".strip()
    content = (
        f"\u5df2\u9009\u6807\u7684\uff1a**{display_name}**\n\n"
        "\u8be5\u6807\u7684\u5df2\u9a8c\u8bc1\u53ef\u7528\u529f\u80fd\uff1a\u884c\u60c5/\u8fd1\u4e00\u5e74\u8868\u73b0\u3002\n"
        f"`PRICE_SNAPSHOT asset={asset} symbol={symbol} provider=FMP`"
    )
    actions = [
        card_button(
            "\u884c\u60c5\u5feb\u7167",
            {"action": "run_price", "asset": asset, "symbol": symbol},
            "primary",
        ),
        card_button("\u8fd4\u56de\u603b\u63a7\u53f0", {"action": "home"}),
    ]
    return build_interactive_card("\u9009\u62e9\u67e5\u8be2\u5185\u5bb9", content, actions, "green")


def build_index_card() -> dict[str, Any]:
    actions = [
        card_button(item["name"], {"action": "run_price", "asset": "index", "symbol": item["symbol"]}, "primary" if idx == 0 else "default")
        for idx, item in enumerate(INDEX_DIRECTORY)
    ]
    actions.append(card_button("\u8fd4\u56de\u603b\u63a7\u53f0", {"action": "home"}))
    return build_interactive_card("\u6307\u6570\u884c\u60c5", "\u9009\u62e9\u8981\u67e5\u7684\u6307\u6570\uff1a", actions, "blue")


def build_crypto_card() -> dict[str, Any]:
    actions = [
        card_button(item["name"], {"action": "run_price", "asset": "crypto", "symbol": item["symbol"]}, "primary" if idx == 0 else "default")
        for idx, item in enumerate(CRYPTO_DIRECTORY)
    ]
    actions.append(card_button("\u8fd4\u56de\u603b\u63a7\u53f0", {"action": "home"}))
    return build_interactive_card("\u52a0\u5bc6\u884c\u60c5", "\u9009\u62e9\u8981\u67e5\u7684\u52a0\u5bc6\u8d44\u4ea7\uff1a", actions, "blue")


def build_macro_card() -> dict[str, Any]:
    actions = [
        card_button(item["name"], {"action": "run_macro", "series_id": item["series_id"], "name": item["name"]}, "primary" if idx == 0 else "default")
        for idx, item in enumerate(FRED_SERIES)
    ]
    actions.append(card_button("\u8fd4\u56de\u603b\u63a7\u53f0", {"action": "home"}))
    return build_interactive_card("\u5b8f\u89c2\u6570\u636e", "\u9009\u62e9 FRED \u5b8f\u89c2\u6307\u6807\uff1a", actions, "blue")


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


async def fetch_yahoo_chart(symbol: str) -> dict[str, Any]:
    url_symbol = symbol.replace("^", "%5E")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
    }
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{url_symbol}",
            params={"range": "1y", "interval": "1d"},
            headers=headers,
        )
        response.raise_for_status()
        data = response.json()
    result = (data.get("chart", {}).get("result") or [None])[0]
    if not result:
        raise HTTPException(status_code=502, detail=f"Yahoo chart returned no data for {symbol}")
    return result


def raw_value(value: Any) -> Any:
    if isinstance(value, dict) and "raw" in value:
        return value.get("raw")
    return value


def nested_raw(data: dict[str, Any], *path: str) -> Any:
    current: Any = data
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return raw_value(current)


async def fetch_yahoo_quote_summary(symbol: str) -> dict[str, Any]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
    }
    modules = "price,summaryDetail,defaultKeyStatistics,financialData"
    async with httpx.AsyncClient(timeout=20, headers=headers, follow_redirects=True) as client:
        try:
            await client.get("https://fc.yahoo.com")
        except Exception:
            pass
        crumb_response = await client.get("https://query1.finance.yahoo.com/v1/test/getcrumb")
        crumb_response.raise_for_status()
        crumb = crumb_response.text.strip()
        response = await client.get(
            f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}",
            params={"modules": modules, "crumb": crumb},
        )
        response.raise_for_status()
        data = response.json()
    result = (data.get("quoteSummary", {}).get("result") or [None])[0]
    if not result:
        raise HTTPException(status_code=502, detail=f"Yahoo summary returned no data for {symbol}")
    return result


async def fetch_yahoo_fundamentals(symbol: str) -> dict[str, list[dict[str, Any]]]:
    types = [
        "annualTotalRevenue",
        "annualGrossProfit",
        "annualOperatingIncome",
        "annualNetIncome",
        "trailingTotalRevenue",
        "trailingGrossProfit",
        "trailingOperatingIncome",
        "trailingNetIncome",
    ]
    period2 = int(datetime.combine(date.today() + timedelta(days=2), time.min, tzinfo=timezone.utc).timestamp())
    period1 = int(datetime.combine(date.today() - timedelta(days=365 * 6), time.min, tzinfo=timezone.utc).timestamp())
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
    }
    async with httpx.AsyncClient(timeout=20, headers=headers) as client:
        response = await client.get(
            f"https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{symbol}",
            params={"type": ",".join(types), "period1": str(period1), "period2": str(period2)},
        )
        response.raise_for_status()
        data = response.json()
    rows = data.get("timeseries", {}).get("result") or []
    output: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        row_type = ((row.get("meta") or {}).get("type") or [None])[0]
        if row_type and isinstance(row.get(row_type), list):
            output[row_type] = row[row_type]
    return output


async def yahoo_price_available(symbol: str) -> bool:
    try:
        result = await fetch_yahoo_chart(symbol)
        return bool(result.get("timestamp"))
    except Exception as exc:
        print(f"Yahoo price unavailable: {symbol}: {type(exc).__name__}: {exc}", flush=True)
        return False


async def search_fmp_candidates(query: str, asset: str = "equity", limit: int = 8) -> list[dict[str, Any]]:
    if not FMP_API_KEY:
        return []
    query = query.strip()
    if not query:
        return []
    async with httpx.AsyncClient(timeout=20) as client:
        endpoint = "search-symbol" if re.search(r"[0-9.^]", query) or (query == query.upper() and re.fullmatch(r"[A-Z.\-]{1,12}", query)) else "search-name"
        try:
            rows = await fetch_fmp_json(client, endpoint, {"query": query})
        except Exception as exc:
            print(f"FMP search failed: {type(exc).__name__}: {exc}", flush=True)
            return search_ticker_candidates(query, limit)
    candidates = rows if isinstance(rows, list) else []
    if asset == "etf":
        candidates = [row for row in candidates if "ETF" in str(row.get("name", "")).upper() or row.get("symbol") in {"SPY", "QQQ", "VOO"}]
    return candidates[:limit]


async def fmp_available(path: str, params: dict[str, str]) -> bool:
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            data = await fetch_fmp_json(client, path, params)
        return bool(data)
    except Exception as exc:
        print(f"FMP capability unavailable: {path} {params.get('symbol', '')}: {type(exc).__name__}: {exc}", flush=True)
        return False


async def build_verified_metric_picker_card(symbol: str, name: str = "", asset: str = "equity") -> dict[str, Any]:
    symbol = symbol.upper()
    if asset != "equity":
        return build_price_picker_card(symbol, name, asset)

    actions: list[dict[str, Any]] = [
        card_button("\u6295\u7814\u5feb\u7167", {"action": "run_equity", "symbol": symbol, "sections": ["price", "valuation", "growth", "margin"]}, "primary"),
        card_button("\u4f30\u503c+\u5229\u6da6\u7387", {"action": "run_equity", "symbol": symbol, "sections": ["valuation", "margin"]}),
        card_button("\u6536\u5165\u589e\u957f+\u5229\u6da6\u7387", {"action": "run_equity", "symbol": symbol, "sections": ["growth", "margin"]}),
        card_button("\u8fd1\u4e00\u5e74\u80a1\u4ef7", {"action": "run_equity", "symbol": symbol, "sections": ["price"]}),
        card_button("\u8fd4\u56de\u603b\u63a7\u53f0", {"action": "home"}),
    ]

    display_name = f"{symbol} {name}".strip()
    content = (
        f"\u5df2\u9009\u6807\u7684\uff1a**{display_name}**\n\n"
        "\u76f4\u63a5\u9009\u8981\u67e5\u7684\u6295\u7814\u5185\u5bb9\u3002\u540e\u7aef\u4f1a\u6309\u6570\u636e\u6e90\u9010\u9879\u515c\u5e95\uff1a"
        "\u4ef7\u683c\u4f18\u5148 FMP\uff0c\u5931\u8d25\u5219\u7528 Yahoo Finance\uff1b\u8d22\u52a1/\u4f30\u503c\u5b57\u6bb5\u82e5\u6570\u636e\u6e90\u6ca1\u6709\uff0c\u4f1a\u5728\u7ed3\u679c\u91cc\u660e\u793a\uff0c\u4e0d\u518d\u628a\u529f\u80fd\u85cf\u6389\u3002"
    )
    return build_interactive_card("\u9009\u62e9\u67e5\u8be2\u5185\u5bb9", content, actions, "green")


async def answer_equity_snapshot(message: str) -> str | None:
    symbol = extract_symbol(message)
    if not symbol:
        return None
    return await answer_equity_snapshot_by_symbol(symbol, ["price", "valuation", "growth", "margin"])


async def answer_equity_snapshot_by_symbol(symbol: str, sections: list[str] | set[str] | None = None) -> str:
    selected_sections = set(sections or ["price", "valuation", "growth", "margin"])
    today = date.today()
    start_date = today - timedelta(days=370)
    symbol = symbol.upper()
    print(f"Card equity snapshot path: symbol={symbol}, sections={','.join(sorted(selected_sections))}", flush=True)

    needs_price = "price" in selected_sections
    needs_valuation = "valuation" in selected_sections
    needs_income = bool({"valuation", "growth", "margin"} & selected_sections)
    results: dict[str, Any] = {}
    failures: list[str] = []

    async def fmp_optional(client: httpx.AsyncClient, key: str, path: str, params: dict[str, str]) -> None:
        try:
            results[key] = await fetch_fmp_json(client, path, params)
        except Exception as exc:
            failures.append(f"FMP {key}: {type(exc).__name__}")
            status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else ""
            print(f"FMP optional field failed: {symbol} {key}: {type(exc).__name__} {status}", flush=True)

    if FMP_API_KEY:
        async with httpx.AsyncClient(timeout=45) as client:
            tasks: list[Any] = []
            if needs_price or needs_valuation:
                tasks.append(fmp_optional(client, "quote", "quote", {"symbol": symbol}))
            if needs_valuation:
                tasks.append(fmp_optional(client, "metrics", "key-metrics-ttm", {"symbol": symbol}))
            if needs_income:
                tasks.append(fmp_optional(client, "income", "income-statement", {"symbol": symbol, "period": "annual", "limit": "5"}))
            if needs_price:
                tasks.append(
                    fmp_optional(
                        client,
                        "history",
                        "historical-price-eod/full",
                        {"symbol": symbol, "from": start_date.isoformat(), "to": today.isoformat()},
                    )
                )
            if tasks:
                await asyncio.gather(*tasks)
    else:
        failures.append("FMP: api key not configured")

    quote = results.get("quote", [])
    metrics = results.get("metrics", [])
    income = results.get("income", [])
    history = results.get("history", [])

    quote_row = quote[0] if isinstance(quote, list) and quote else {}
    metrics_row = metrics[0] if isinstance(metrics, list) and metrics else {}
    income_rows = income if isinstance(income, list) else []
    latest_income = income_rows[0] if income_rows else {}
    previous_income = income_rows[1] if len(income_rows) > 1 else {}
    historical = history.get("historical", []) if isinstance(history, dict) else history if isinstance(history, list) else []
    latest_close = historical[0].get("close") if historical else quote_row.get("price")
    first_close = historical[-1].get("close") if historical else None
    year_high = quote_row.get("yearHigh")
    year_low = quote_row.get("yearLow")
    price_source = "FMP" if latest_close else ""
    yahoo_meta: dict[str, Any] = {}

    if needs_price and (not latest_close or not first_close):
        try:
            yahoo_result = await fetch_yahoo_chart(symbol)
            yahoo_meta = yahoo_result.get("meta", {})
            closes = yahoo_result.get("indicators", {}).get("quote", [{}])[0].get("close") or []
            valid_closes = [close for close in closes if close is not None]
            if valid_closes:
                latest_close = yahoo_meta.get("regularMarketPrice") or valid_closes[-1]
                first_close = first_close or valid_closes[0]
                year_high = year_high or max(valid_closes)
                year_low = year_low or min(valid_closes)
                price_source = "Yahoo Finance" if not price_source else f"{price_source} + Yahoo Finance"
            if not quote_row:
                quote_row = {
                    "name": yahoo_meta.get("shortName") or yahoo_meta.get("longName") or symbol,
                    "price": latest_close,
                    "marketCap": yahoo_meta.get("marketCap"),
                }
        except Exception as exc:
            failures.append(f"Yahoo price: {type(exc).__name__}")
            print(f"Yahoo equity fallback failed: {symbol}: {type(exc).__name__}: {exc}", flush=True)

    yahoo_summary: dict[str, Any] = {}
    yahoo_fundamentals: dict[str, list[dict[str, Any]]] = {}
    if needs_valuation or needs_income:
        try:
            yahoo_summary = await fetch_yahoo_quote_summary(symbol)
            yahoo_price = yahoo_summary.get("price", {})
            if yahoo_price:
                quote_row.setdefault("name", nested_raw(yahoo_summary, "price", "shortName") or nested_raw(yahoo_summary, "price", "longName"))
                quote_row["marketCap"] = quote_row.get("marketCap") or nested_raw(yahoo_summary, "price", "marketCap")
        except Exception as exc:
            failures.append(f"Yahoo summary: {type(exc).__name__}")
            print(f"Yahoo summary fallback failed: {symbol}: {type(exc).__name__}: {exc}", flush=True)
        try:
            yahoo_fundamentals = await fetch_yahoo_fundamentals(symbol)
        except Exception as exc:
            failures.append(f"Yahoo fundamentals: {type(exc).__name__}")
            print(f"Yahoo fundamentals fallback failed: {symbol}: {type(exc).__name__}: {exc}", flush=True)

    one_year_return = None
    if latest_close and first_close:
        one_year_return = (float(latest_close) / float(first_close) - 1) * 100

    def series_raw(series_name: str, offset: int = -1) -> Any:
        rows = yahoo_fundamentals.get(series_name) or []
        if not rows:
            return None
        rows = sorted(rows, key=lambda row: row.get("asOfDate", ""))
        try:
            row = rows[offset]
        except IndexError:
            return None
        reported = row.get("reportedValue") or {}
        return reported.get("raw")

    if not latest_income and yahoo_fundamentals:
        latest_income = {
            "revenue": series_raw("annualTotalRevenue"),
            "grossProfit": series_raw("annualGrossProfit"),
            "operatingIncome": series_raw("annualOperatingIncome"),
            "netIncome": series_raw("annualNetIncome"),
        }
        previous_income = {
            "revenue": series_raw("annualTotalRevenue", -2),
        }

    revenue_growth = None
    if latest_income.get("revenue") and previous_income.get("revenue"):
        revenue_growth = (float(latest_income["revenue"]) / float(previous_income["revenue"]) - 1) * 100
    elif nested_raw(yahoo_summary, "financialData", "revenueGrowth") is not None:
        revenue_growth = float(nested_raw(yahoo_summary, "financialData", "revenueGrowth")) * 100

    pe_value = (
        quote_row.get("pe")
        or metrics_row.get("peRatioTTM")
        or nested_raw(yahoo_summary, "summaryDetail", "trailingPE")
        or nested_raw(yahoo_summary, "defaultKeyStatistics", "trailingPE")
        or nested_raw(yahoo_summary, "summaryDetail", "forwardPE")
        or (1 / float(metrics_row["earningsYieldTTM"]) if metrics_row.get("earningsYieldTTM") else None)
        or safe_ratio(quote_row.get("price"), latest_income.get("epsDiluted") or latest_income.get("eps"))
    )
    ps_value = (
        metrics_row.get("priceToSalesRatioTTM")
        or nested_raw(yahoo_summary, "summaryDetail", "priceToSalesTrailing12Months")
        or nested_raw(yahoo_summary, "defaultKeyStatistics", "priceToSalesTrailing12Months")
        or safe_ratio(quote_row.get("marketCap"), latest_income.get("revenue"))
    )
    ev_ebitda_value = (
        metrics_row.get("enterpriseValueOverEBITDATTM")
        or metrics_row.get("evToEBITDATTM")
        or nested_raw(yahoo_summary, "defaultKeyStatistics", "enterpriseToEbitda")
    )
    gross_margin = (
        latest_income.get("grossProfitRatio")
        or nested_raw(yahoo_summary, "financialData", "grossMargins")
        or safe_ratio(latest_income.get("grossProfit"), latest_income.get("revenue"))
    )
    operating_margin = latest_income.get("operatingIncomeRatio") or safe_ratio(
        latest_income.get("operatingIncome"),
        latest_income.get("revenue"),
    )
    operating_margin = operating_margin or nested_raw(yahoo_summary, "financialData", "operatingMargins")
    net_margin = (
        latest_income.get("netIncomeRatio")
        or nested_raw(yahoo_summary, "financialData", "profitMargins")
        or safe_ratio(latest_income.get("netIncome"), latest_income.get("revenue"))
    )

    company_name = quote_row.get("name") or yahoo_meta.get("shortName") or yahoo_meta.get("longName") or symbol
    section_names = "\uff0c".join(SECTION_LABELS.get(section, section) for section in selected_sections)
    data_sources = []
    if any(results.get(key) for key in ("quote", "metrics", "income", "history")):
        data_sources.append("FMP")
    if price_source and "Yahoo Finance" in price_source:
        data_sources.append("Yahoo Finance")
    if yahoo_summary or yahoo_fundamentals:
        data_sources.append("Yahoo Finance")
    source_label = " + ".join(dict.fromkeys(data_sources)) if data_sources else "unavailable"

    lines = [
        f"{symbol} {company_name} \u6295\u7814\u67e5\u8be2",
        "",
        f"\u5185\u90e8\u6307\u4ee4\uff1aEQUITY_SNAPSHOT symbol={symbol} sections={','.join(sorted(selected_sections))} provider={source_label}",
        f"\u5df2\u9009\u5185\u5bb9\uff1a{section_names}",
        "",
        f"\u6570\u636e\u6e90\uff1a{source_label}\uff0c\u8d70\u5feb\u901f API \u8def\u5f84\uff0c\u672a\u8d70\u4ea4\u4e92\u5f0f CLI\u3002",
        "",
    ]
    if "price" in selected_sections:
        lines.extend([
            "\u80a1\u4ef7",
            f"- \u6700\u65b0\u4ef7\u683c\uff1a{fmt_number(quote_row.get('price') or latest_close)}",
            f"- \u8fd1\u4e00\u5e74\u6da8\u8dcc\u5e45\uff1a{fmt_percent(one_year_return)}",
            f"- \u5e74\u5185\u9ad8\u70b9\uff1a{fmt_number(year_high)}",
            f"- \u5e74\u5185\u4f4e\u70b9\uff1a{fmt_number(year_low)}",
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
        lines.append("")

    lines.extend([
        "\u6570\u636e\u5b8c\u6574\u6027",
        f"- \u884c\u60c5\uff1a{'\u5df2\u83b7\u53d6' if latest_close else '\u672a\u83b7\u53d6'}",
        f"- \u4f30\u503c\uff1a{'\u5df2\u83b7\u53d6' if (pe_value or ps_value or ev_ebitda_value) else '\u672a\u83b7\u53d6'}",
        f"- \u6536\u5165/\u5229\u6da6\u7387\uff1a{'\u5df2\u83b7\u53d6' if latest_income else '\u672a\u83b7\u53d6'}",
    ])
    if failures:
        lines.append(f"- \u5931\u8d25\u515c\u5e95\uff1a{'; '.join(dict.fromkeys(failures[:4]))}")

    lines.extend([
        "",
        "\u7b80\u8bc4",
        "\u8fd9\u662f\u6570\u636e\u5e95\u5ea7\u5feb\u7167\uff1a\u5148\u7ed9\u4f60\u53ef\u7528\u6570\u636e\uff0c\u518d\u628a\u7f3a\u5931\u5b57\u6bb5\u660e\u786e\u6807\u51fa\u3002\u4e0b\u4e00\u6b65\u53ef\u7ee7\u7eed\u52a0\u5165\u540c\u884c\u5bf9\u6bd4\u3001\u5386\u53f2\u4f30\u503c\u5206\u4f4d\u548c\u56fe\u8868\u5361\u7247\u3002",
    ])

    return "\n".join(lines).strip()


async def answer_price_snapshot(symbol: str, asset: str = "instrument") -> str:
    symbol = symbol.upper()
    today = date.today()
    start_date = today - timedelta(days=370)
    source = "FMP"
    quote_row: dict[str, Any] = {}
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            quote_task = fetch_fmp_json(client, "quote", {"symbol": symbol})
            history_task = fetch_fmp_json(
                client,
                "historical-price-eod/full",
                {"symbol": symbol, "from": start_date.isoformat(), "to": today.isoformat()},
            )
            quote, history = await asyncio.gather(quote_task, history_task)

        quote_row = quote[0] if isinstance(quote, list) and quote else {}
        historical = history.get("historical", []) if isinstance(history, dict) else history if isinstance(history, list) else []
        latest_close = historical[0].get("close") if historical else quote_row.get("price")
        first_close = historical[-1].get("close") if historical else None
    except Exception as exc:
        status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else ""
        print(f"FMP price snapshot failed, falling back to Yahoo: {symbol}: {type(exc).__name__} {status}", flush=True)
        try:
            source = "Yahoo Finance"
            result = await fetch_yahoo_chart(symbol)
            meta = result.get("meta", {})
            closes = result.get("indicators", {}).get("quote", [{}])[0].get("close") or []
            valid_closes = [close for close in closes if close is not None]
            latest_close = meta.get("regularMarketPrice") or (valid_closes[-1] if valid_closes else None)
            first_close = valid_closes[0] if valid_closes else None
            quote_row = {
                "name": meta.get("shortName") or meta.get("longName") or symbol,
                "price": latest_close,
                "yearHigh": max(valid_closes) if valid_closes else None,
                "yearLow": min(valid_closes) if valid_closes else None,
                "marketCap": None,
            }
        except Exception as yahoo_exc:
            status = yahoo_exc.response.status_code if isinstance(yahoo_exc, httpx.HTTPStatusError) else ""
            print(f"Yahoo price snapshot failed, falling back to FMP quote only: {symbol}: {type(yahoo_exc).__name__} {status}", flush=True)
            source = "FMP quote"
            try:
                async with httpx.AsyncClient(timeout=20) as client:
                    quote = await fetch_fmp_json(client, "quote", {"symbol": symbol})
                quote_row = quote[0] if isinstance(quote, list) and quote else {}
            except Exception as quote_exc:
                status = quote_exc.response.status_code if isinstance(quote_exc, httpx.HTTPStatusError) else ""
                print(f"FMP quote-only fallback failed: {symbol}: {type(quote_exc).__name__} {status}", flush=True)
                source = "unavailable"
                quote_row = {"name": symbol, "price": None, "yearHigh": None, "yearLow": None, "marketCap": None}
            latest_close = quote_row.get("price")
            first_close = None
    one_year_return = None
    if latest_close and first_close:
        one_year_return = (float(latest_close) / float(first_close) - 1) * 100

    return "\n".join([
        f"{symbol} {quote_row.get('name') or ''} \u884c\u60c5\u5feb\u7167".strip(),
        "",
        f"\u5185\u90e8\u6307\u4ee4\uff1aPRICE_SNAPSHOT asset={asset} symbol={symbol} provider={source}",
        f"\u6570\u636e\u6e90\uff1a{source}",
        "",
        f"- \u6700\u65b0\u4ef7\u683c\uff1a{fmt_number(quote_row.get('price') or latest_close)}",
        f"- \u8fd1\u4e00\u5e74\u6da8\u8dcc\u5e45\uff1a{fmt_percent(one_year_return)}",
        f"- \u5e74\u5185\u9ad8\u70b9\uff1a{fmt_number(quote_row.get('yearHigh'))}",
        f"- \u5e74\u5185\u4f4e\u70b9\uff1a{fmt_number(quote_row.get('yearLow'))}",
        f"- \u5e02\u503c\uff1a{fmt_number(quote_row.get('marketCap'))}",
    ])


async def answer_macro_snapshot(series_id: str, name: str = "") -> str:
    api_key = os.getenv("FRED_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=500, detail="FRED_API_KEY is not configured")
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": "3",
    }
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get("https://api.stlouisfed.org/fred/series/observations", params=params)
        response.raise_for_status()
        data = response.json()
    observations = data.get("observations", [])
    lines = [
        f"{name or series_id} \u5b8f\u89c2\u6570\u636e",
        "",
        f"\u5185\u90e8\u6307\u4ee4\uff1aMACRO_SERIES series_id={series_id} provider=FRED",
        "\u6570\u636e\u6e90\uff1aFRED",
        "",
    ]
    for row in observations[:3]:
        lines.append(f"- {row.get('date')}: {row.get('value')}")
    return "\n".join(lines)


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
        if OPENBB_REQUIRE_ALLOWED_PREFIX and not line.startswith(OPENBB_ALLOWED_PREFIXES):
            raise HTTPException(status_code=400, detail=f"Command is not allow-listed: {line}")
        cleaned.append(line)

    if not cleaned:
        raise HTTPException(status_code=400, detail="Routine contains no executable commands")
    return "\n".join(cleaned) + "\n"


def parse_cli_value(value: str) -> Any:
    lowered = value.strip().lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"none", "null"}:
        return None
    if "," in value and not value.startswith("{") and not value.startswith("["):
        return value
    try:
        if re.fullmatch(r"-?\d+", value):
            return int(value)
        if re.fullmatch(r"-?\d+\.\d+", value):
            return float(value)
    except ValueError:
        return value
    return value


def parse_openbb_command(command: str) -> dict[str, Any]:
    try:
        tokens = shlex.split(command, posix=True)
    except ValueError as exc:
        raise UnsupportedOpenBBPlatformCommand(f"Cannot parse command: {command}") from exc
    if not tokens:
        raise UnsupportedOpenBBPlatformCommand("Empty command")

    route = tokens[0].strip()
    if not route.startswith("/"):
        raise UnsupportedOpenBBPlatformCommand(f"Only slash OpenBB routes are supported by the platform engine: {route}")

    kwargs: dict[str, Any] = {}
    positional: list[Any] = []
    idx = 1
    while idx < len(tokens):
        token = tokens[idx]
        if token.startswith("--"):
            key = token[2:].replace("-", "_")
            if not key:
                idx += 1
                continue
            if idx + 1 < len(tokens) and not tokens[idx + 1].startswith("--"):
                kwargs[key] = parse_cli_value(tokens[idx + 1])
                idx += 2
            else:
                kwargs[key] = True
                idx += 1
        elif token.startswith("-") and len(token) > 1:
            key = token[1:].replace("-", "_")
            kwargs[key] = True
            idx += 1
        else:
            positional.append(parse_cli_value(token))
            idx += 1

    return {"route": route, "positional": positional, "kwargs": kwargs}


def get_openbb_import_lock() -> asyncio.Lock:
    global OPENBB_IMPORT_LOCK
    if OPENBB_IMPORT_LOCK is None:
        OPENBB_IMPORT_LOCK = asyncio.Lock()
    return OPENBB_IMPORT_LOCK


def import_openbb_obb() -> Any:
    from openbb import obb  # pylint: disable=import-outside-toplevel

    return obb


async def get_openbb_obb() -> Any:
    global OPENBB_OBB
    if OPENBB_OBB is not None:
        return OPENBB_OBB
    async with get_openbb_import_lock():
        if OPENBB_OBB is not None:
            return OPENBB_OBB
        print("OpenBB Platform import start", flush=True)
        start = datetime.now(timezone.utc)
        OPENBB_OBB = import_openbb_obb()
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        print(f"OpenBB Platform import finished seconds={elapsed:.1f}", flush=True)
        return OPENBB_OBB


async def prewarm_openbb_platform() -> None:
    if not OPENBB_PLATFORM_PREWARM:
        return
    try:
        await get_openbb_obb()
    except Exception as exc:
        print(f"OpenBB Platform prewarm failed: {type(exc).__name__}: {exc}", flush=True)


def resolve_openbb_callable(obb: Any, route: str) -> Any:
    target = obb
    for raw_part in route.strip("/").split("/"):
        part = raw_part.strip().replace("-", "_")
        if not part:
            continue
        if not hasattr(target, part):
            raise UnsupportedOpenBBPlatformCommand(f"OpenBB Platform route not found: {route}")
        target = getattr(target, part)
    if not callable(target):
        raise UnsupportedOpenBBPlatformCommand(f"OpenBB Platform route is not executable: {route}")
    return target


def fit_positionals_to_signature(func: Any, positional: list[Any], kwargs: dict[str, Any]) -> tuple[list[Any], dict[str, Any]]:
    if not positional:
        return [], kwargs
    try:
        parameters = list(inspect.signature(func).parameters.values())
    except (TypeError, ValueError):
        return positional, kwargs

    fitted_args: list[Any] = []
    fitted_kwargs = dict(kwargs)
    remaining = list(positional)
    for param in parameters:
        if not remaining:
            break
        if param.kind not in {inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD}:
            continue
        if param.name.startswith("__") or param.name in fitted_kwargs:
            continue
        fitted_args.append(remaining.pop(0))
    if remaining:
        fitted_args.extend(remaining)
    return fitted_args, fitted_kwargs


def dataframe_preview(df: Any) -> dict[str, Any]:
    total_rows = int(getattr(df, "shape", [0])[0])
    preview = df.head(OPENBB_PLATFORM_MAX_ROWS)
    records = preview.to_dict(orient="records")
    return {
        "type": "dataframe",
        "row_count": total_rows,
        "truncated": total_rows > OPENBB_PLATFORM_MAX_ROWS,
        "rows": records,
    }


def serialize_openbb_output(value: Any) -> dict[str, Any]:
    if hasattr(value, "to_dataframe"):
        try:
            return dataframe_preview(value.to_dataframe(index=None))
        except Exception:
            pass
    if hasattr(value, "to_dict"):
        try:
            data = value.to_dict("records")
            if isinstance(data, list):
                return {
                    "type": value.__class__.__name__,
                    "row_count": len(data),
                    "truncated": len(data) > OPENBB_PLATFORM_MAX_ROWS,
                    "rows": data[:OPENBB_PLATFORM_MAX_ROWS],
                }
            return {"type": value.__class__.__name__, "data": data}
        except Exception:
            pass
    if hasattr(value, "model_dump"):
        try:
            return {"type": value.__class__.__name__, "data": value.model_dump(exclude_none=True)}
        except Exception:
            pass
    if isinstance(value, list):
        return {
            "type": "list",
            "row_count": len(value),
            "truncated": len(value) > OPENBB_PLATFORM_MAX_ROWS,
            "rows": value[:OPENBB_PLATFORM_MAX_ROWS],
        }
    if isinstance(value, dict):
        return {"type": "dict", "data": value}
    return {"type": value.__class__.__name__, "data": str(value)}


def execute_openbb_callable(func: Any, positional: list[Any], kwargs: dict[str, Any]) -> Any:
    args, fitted_kwargs = fit_positionals_to_signature(func, positional, kwargs)
    return func(*args, **fitted_kwargs)


async def execute_openbb_platform_command(command: str) -> dict[str, Any]:
    parsed = parse_openbb_command(command)
    obb = await get_openbb_obb()
    func = resolve_openbb_callable(obb, parsed["route"])
    start = datetime.now(timezone.utc)
    result = execute_openbb_callable(func, parsed["positional"], parsed["kwargs"])
    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    return {
        "command": command,
        "route": parsed["route"],
        "params": {"positional": parsed["positional"], "kwargs": parsed["kwargs"]},
        "seconds": round(elapsed, 3),
        "output": serialize_openbb_output(result),
    }


async def run_openbb_platform_routine(commands: list[str], timeout_seconds: int) -> dict[str, Any]:
    async def run_all() -> list[dict[str, Any]]:
        outputs = []
        for command in commands:
            print(f"OpenBB Platform command start command={command[:300]}", flush=True)
            try:
                output = await execute_openbb_platform_command(command)
            except Exception as exc:
                output = {
                    "command": command,
                    "error": f"{type(exc).__name__}: {exc}",
                }
                print(f"OpenBB Platform command failed command={command[:300]} error={output['error']}", flush=True)
            else:
                print(
                    "OpenBB Platform command finished "
                    f"command={command[:300]} seconds={output.get('seconds')}",
                    flush=True,
                )
            outputs.append(output)
        return outputs

    try:
        started = datetime.now(timezone.utc)
        results = await asyncio.wait_for(run_all(), timeout=timeout_seconds)
    except asyncio.TimeoutError as exc:
        raise HTTPException(status_code=504, detail="OpenBB Platform routine timed out") from exc
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    return {
        "execution_mode": "platform_api",
        "returncode": 0 if all("error" not in item for item in results) else 1,
        "seconds": round(elapsed, 3),
        "results": results,
    }


async def run_openbb_routine(routine: str, timeout_seconds: int) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as temp_dir:
        routine_path = Path(temp_dir) / "routine.openbb"
        routine_path.write_text(routine, encoding="utf-8")
        print(
            "OpenBB routine start "
            f"timeout={timeout_seconds} "
            f"routine={routine[:500].replace(chr(10), ' | ')}",
            flush=True,
        )

        proc = await asyncio.create_subprocess_exec(
            OPENBB_COMMAND,
            "--file",
            "routine.openbb",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=temp_dir,
        )

        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=timeout_seconds,
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            print("OpenBB routine timed out", flush=True)
            raise HTTPException(status_code=504, detail="OpenBB CLI routine timed out")

    stdout_text = stdout.decode("utf-8", errors="replace")
    stderr_text = stderr.decode("utf-8", errors="replace")
    print(
        "OpenBB routine finished "
        f"returncode={proc.returncode} stdout_len={len(stdout_text)} stderr_len={len(stderr_text)}",
        flush=True,
    )
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
        if line and not line.startswith("#") and line not in {"```", "```text"}:
            if OPENBB_REQUIRE_ALLOWED_PREFIX and not line.startswith(tuple(OPENBB_ALLOWED_PREFIXES)):
                continue
            lines.append(line)
    return lines[:12]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def persist_feishu_tasks() -> None:
    try:
        TASK_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = TASK_STATE_PATH.with_suffix(TASK_STATE_PATH.suffix + ".tmp")
        tmp_path.write_text(
            json.dumps(list(RECENT_FEISHU_TASKS.values()), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(TASK_STATE_PATH)
    except Exception as exc:
        print(f"Failed to persist Feishu task state: {type(exc).__name__}: {exc}", flush=True)


def load_feishu_tasks() -> None:
    if not TASK_STATE_PATH.exists():
        return
    try:
        data = json.loads(TASK_STATE_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"Failed to load Feishu task state: {type(exc).__name__}: {exc}", flush=True)
        return
    if not isinstance(data, list):
        return
    for item in data[-MAX_RECENT_FEISHU_TASKS:]:
        if isinstance(item, dict) and isinstance(item.get("message_id"), str):
            RECENT_FEISHU_TASKS[item["message_id"]] = item


def looks_like_openbb_routine_line(line: str) -> bool:
    if not line or any(ord(char) > 127 for char in line):
        return False
    command = line.split()[0].strip().lower()
    if "/" in command:
        return True
    if command in {"home", "help", "exit", "clear", "about", "exe"}:
        return True
    if command.startswith(("/", "?")):
        return True
    return False


def commands_from_user_message(message: str) -> list[str]:
    lines = []
    for raw_line in message.splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#"):
            if OPENBB_REQUIRE_ALLOWED_PREFIX and not line.startswith(tuple(OPENBB_ALLOWED_PREFIXES)):
                continue
            lines.append(line)
    if not lines:
        return []
    if any(any(ord(char) > 127 for char in line) for line in lines):
        return []
    if any(looks_like_openbb_routine_line(line) for line in lines):
        return lines
    return []


def remember_feishu_task(message_id: str, **updates: Any) -> None:
    task = RECENT_FEISHU_TASKS.setdefault(
        message_id,
        {
            "message_id": message_id,
            "created_at": now_iso(),
            "status": "queued",
        },
    )
    task.update(updates)
    while len(RECENT_FEISHU_TASKS) > MAX_RECENT_FEISHU_TASKS:
        oldest_key = next(iter(RECENT_FEISHU_TASKS))
        RECENT_FEISHU_TASKS.pop(oldest_key, None)
    persist_feishu_tasks()


def schedule_async_task(name: str, coro: Any) -> None:
    task = asyncio.create_task(coro)

    def log_task_done(done_task: asyncio.Task[Any]) -> None:
        try:
            done_task.result()
        except Exception as exc:
            print(f"Background task failed name={name} error={type(exc).__name__}: {exc}", flush=True)

    task.add_done_callback(log_task_done)


def configured_provider_guidance() -> str:
    configured = [
        key.replace("_api_key", "").replace("_token", "")
        for key, env_name in OPENBB_CREDENTIAL_ENV.items()
        if os.getenv(env_name, "").strip()
    ]
    base = ["yfinance", "finviz", "sec"]
    providers = ", ".join(sorted(set(base + configured))) or "yfinance, finviz, sec"
    return (
        f"Providers available or preferred in this deployment: {providers}. "
        "For equity profile, quote, price history, dividends, and splits prefer yfinance or finviz. "
        "Use fmp only for the few fundamentals or valuation fields that are not available from yfinance, finviz, or sec, and avoid generating many fmp commands in one routine because free FMP keys can rate-limit. "
        "For SEC filings, company facts, annual reports, and regulatory fundamentals prefer sec. "
        "For macro use fred when available. "
        "Do not use intrinio, polygon, benzinga, tradier, nasdaq, or tradingeconomics unless that provider is explicitly requested or listed as available."
    )


def format_cli_answer(question: str, commands: list[str], result: dict[str, Any], summary: str | None = None) -> str:
    parts = [
        "OpenBB Platform CLI 执行结果",
        "",
        "实际执行的 CLI routine：",
        "```text",
        "\n".join(commands),
        "```",
        "",
        f"退出码：{result['returncode']}",
    ]
    if summary:
        parts.extend(["", "整理结果：", summary.strip()])
    if result.get("stdout"):
        parts.extend(["", "CLI 原始输出：", "```text", result["stdout"].strip(), "```"])
    if result.get("stderr"):
        parts.extend(["", "CLI 错误输出：", "```text", result["stderr"].strip(), "```"])
    return "\n".join(parts).strip()


def format_platform_answer(commands: list[str], result: dict[str, Any], summary: str | None = None) -> str:
    parts = [
        "OpenBB Platform 执行结果",
        "",
        "执行方式：OpenBB Platform Python API（常驻轻量引擎）",
        "",
        "实际执行的 OpenBB 路径：",
        "```text",
        "\n".join(commands),
        "```",
        "",
        f"退出码：{result['returncode']}",
        f"耗时：{result.get('seconds', 0)} 秒",
    ]
    if summary:
        parts.extend(["", "整理结果：", summary.strip()])

    result_text = json.dumps(result.get("results", []), ensure_ascii=False, indent=2, default=str)
    if len(result_text) > MAX_OUTPUT_CHARS:
        result_text = result_text[:MAX_OUTPUT_CHARS] + "\n...（结构化结果已截断）"
    parts.extend(["", "结构化结果：", "```json", result_text, "```"])
    return "\n".join(parts).strip()


async def answer_with_openbb(message: str, timeout_seconds: int, message_id: str | None = None) -> str:
    if OPENBB_FAST_EQUITY_SNAPSHOT and is_equity_research_request(message):
        try:
            equity_answer = await answer_equity_snapshot(message)
            if equity_answer:
                return equity_answer
        except Exception as exc:
            print(f"Fast equity snapshot failed: {type(exc).__name__}: {exc}", flush=True)
            symbol = extract_symbol(message)
            if symbol:
                try:
                    return await answer_price_snapshot(symbol, "equity")
                except Exception as fallback_exc:
                    print(f"Equity price fallback failed: {type(fallback_exc).__name__}: {fallback_exc}", flush=True)
            return f"股票快照查询失败：{type(exc).__name__}: {exc}"
        return "股票快照查询失败：没有识别到可用股票代码或 FMP_API_KEY 未配置。"

    commands = commands_from_user_message(message)
    if not commands:
        if message_id:
            remember_feishu_task(message_id, status="translating", translate_started_at=now_iso())
        print(f"OpenBB translate start message_id={message_id or '-'} text={message[:300]}", flush=True)
        command_text = await call_model(
            [
                {
                    "role": "system",
                    "content": (
                        "Convert the user request into OpenBB Platform CLI routine commands. "
                        "Return only executable OpenBB Platform CLI routine commands, one per line. "
                        "Use current OpenBB Platform paths and include enough commands for a complete research answer. "
                        f"{configured_provider_guidance()} "
                        "Do not answer from your own knowledge. Do not use FMP/Yahoo directly. "
                        "Do not include shell commands or explanations."
                    ),
                },
                {"role": "user", "content": message},
            ],
            max_tokens=1200,
        )
        commands = extract_commands(command_text)
        print(
            "OpenBB translate finished "
            f"message_id={message_id or '-'} commands={str(commands)[:1000]}",
            flush=True,
        )
    if not commands:
        if message_id:
            remember_feishu_task(message_id, status="error", error="no_openbb_commands", ended_at=now_iso())
        return (
            "我收到了，但没能生成可执行的 OpenBB Platform CLI routine。\n"
            "这不是改走快照层；我会保持 CLI 直连模式。你可以直接发：查 AAPL 的完整公司信息、行情、估值、财务、管理层和拆股。"
        )

    if message_id:
        remember_feishu_task(message_id, status="running_cli", commands=commands, cli_started_at=now_iso())
    print(
        "OpenBB CLI task commands "
        f"message_id={message_id or '-'} commands={' | '.join(commands)[:1000]}",
        flush=True,
    )
    if OPENBB_EXECUTION_MODE in {"platform_api", "platform", "api"}:
        result = await run_openbb_platform_routine(commands, timeout_seconds)
        if message_id:
            remember_feishu_task(
                message_id,
                status="summarizing",
                returncode=result.get("returncode"),
                platform_result_preview=json.dumps(result.get("results", []), ensure_ascii=False, default=str)[-1000:],
                cli_finished_at=now_iso(),
            )
        summary = await call_model(
            [
                {
                    "role": "system",
                    "content": (
                        "Summarize the OpenBB Platform structured output for an investment research chat. "
                        "Preserve important available fields, metrics, dates, units, and errors. "
                        "If a command failed or data is missing, say exactly which part failed or is missing."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "question": message,
                            "commands": commands,
                            "execution_mode": result["execution_mode"],
                            "returncode": result["returncode"],
                            "results": result["results"],
                        },
                        ensure_ascii=False,
                        default=str,
                    ),
                },
            ],
            max_tokens=2200,
        )
        return format_platform_answer(commands, result, summary)

    result = await run_openbb_routine("\n".join(commands) + "\n", timeout_seconds)
    if message_id:
        remember_feishu_task(
            message_id,
            status="summarizing" if result.get("stdout") else "replying",
            returncode=result.get("returncode"),
            stdout_preview=(result.get("stdout") or "")[-1000:],
            stderr_preview=(result.get("stderr") or "")[-1000:],
            cli_finished_at=now_iso(),
        )
    summary = ""
    if result.get("stdout"):
        summary = await call_model(
            [
                {
                    "role": "system",
                    "content": (
                        "Summarize the OpenBB Platform CLI output for an investment research chat. "
                        "Preserve all important available fields. If data is missing, say it is missing from CLI output."
                    ),
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
            max_tokens=1800,
        )
    return format_cli_answer(message, commands, result, summary)


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


def split_feishu_text(text: str, limit: int = 2800) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        split_at = remaining.rfind("\n", 0, limit)
        if split_at < limit // 2:
            split_at = limit
        chunks.append(remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip()
    return chunks


async def reply_feishu_message_chunks(message_id: str, text: str) -> None:
    chunks = split_feishu_text(text)
    total = len(chunks)
    for idx, chunk in enumerate(chunks, start=1):
        prefix = f"({idx}/{total})\n" if total > 1 else ""
        await reply_feishu_message(message_id, prefix + chunk)


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


async def add_feishu_reaction_safely(message_id: str, emoji_type: str = FEISHU_ACK_REACTION) -> None:
    try:
        await add_feishu_reaction(message_id, emoji_type)
        remember_feishu_task(message_id, reaction=emoji_type, reaction_at=now_iso())
        print(f"Feishu reaction added message_id={message_id} emoji={emoji_type}", flush=True)
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"
        response = getattr(exc, "response", None)
        if response is not None:
            detail = f"{detail} body={response.text[:1000]}"
        remember_feishu_task(message_id, reaction_error=detail, reaction_error_at=now_iso())
        print(f"Failed to add Feishu reaction: {detail}", flush=True)


async def recover_interrupted_feishu_tasks() -> None:
    for message_id, task in list(RECENT_FEISHU_TASKS.items()):
        if task.get("status") not in INTERRUPTED_TASK_STATUSES:
            continue
        if task.get("send_feishu_reply") is False or task.get("recovery_notice_sent_at"):
            continue
        previous_status = task.get("status")
        remember_feishu_task(
            message_id,
            status="interrupted_on_restart",
            interrupted_status=previous_status,
            ended_at=now_iso(),
        )
        notice = (
            "OpenBB Platform CLI 执行被服务进程重启中断。\n\n"
            f"- 中断前状态：{previous_status}\n"
            "- 这说明飞书消息和表情反应链路已到达后台，但 CLI 子进程在生成结果前被 Render 进程重启打断。\n"
            "- 这不是把功能限制成股票快照，也不是飞书表情导致的。"
        )
        try:
            await reply_feishu_message_chunks(message_id, notice)
            remember_feishu_task(message_id, recovery_notice_sent_at=now_iso())
            print(f"Recovered interrupted Feishu task message_id={message_id}", flush=True)
        except Exception as exc:
            detail = f"{type(exc).__name__}: {exc}"
            response = getattr(exc, "response", None)
            if response is not None:
                detail = f"{detail} body={response.text[:1000]}"
            remember_feishu_task(message_id, recovery_notice_error=detail)
            print(f"Failed to recover interrupted task message_id={message_id} error={detail}", flush=True)


async def process_openbb_task(
    message_id: str,
    text: str,
    *,
    send_feishu_reply: bool,
    timeout_seconds: int,
) -> None:
    remember_feishu_task(
        message_id,
        status="running",
        input=text,
        send_feishu_reply=send_feishu_reply,
        started_at=now_iso(),
    )
    print(f"Feishu OpenBB task start message_id={message_id} text={text[:300]}", flush=True)
    try:
        answer = await answer_with_openbb(text, timeout_seconds, message_id=message_id)
    except Exception as exc:
        answer = f"查询过程中出错了：{type(exc).__name__}: {exc}"
        remember_feishu_task(message_id, status="error", error=f"{type(exc).__name__}: {exc}", ended_at=now_iso())
    else:
        remember_feishu_task(message_id, status="replying", answer_preview=answer[:1000])
        print(f"Feishu OpenBB task answer ready message_id={message_id} length={len(answer)}", flush=True)
    if send_feishu_reply:
        try:
            await reply_feishu_message_chunks(message_id, answer)
            remember_feishu_task(message_id, reply_sent_at=now_iso())
            print(f"Feishu reply sent message_id={message_id} length={len(answer)}", flush=True)
        except Exception as exc:
            detail = f"{type(exc).__name__}: {exc}"
            response = getattr(exc, "response", None)
            if response is not None:
                detail = f"{detail} body={response.text[:1000]}"
            remember_feishu_task(message_id, status="reply_error", reply_error=detail, ended_at=now_iso())
            print(f"Failed to send Feishu reply message_id={message_id} error={detail}", flush=True)
            return
    remember_feishu_task(message_id, status="done", ended_at=now_iso())
    print(f"Feishu OpenBB task done message_id={message_id}", flush=True)


async def process_feishu_query(message_id: str, text: str) -> None:
    await process_openbb_task(
        message_id,
        text,
        send_feishu_reply=True,
        timeout_seconds=OPENBB_TIMEOUT_SECONDS,
    )


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


async def process_price_card_query(message_id: str, symbol: str, asset: str) -> None:
    try:
        await add_feishu_reaction(message_id, FEISHU_RUN_REACTION)
    except Exception as exc:
        print(f"Failed to add Feishu run reaction: {type(exc).__name__}: {exc}", flush=True)
    try:
        answer = await answer_price_snapshot(symbol, asset)
    except Exception as exc:
        answer = f"\u67e5\u8be2\u8fc7\u7a0b\u4e2d\u51fa\u9519\u4e86\uff1a{type(exc).__name__}: {exc}"
    await reply_feishu_message(message_id, answer[:3000])


async def process_macro_card_query(message_id: str, series_id: str, name: str) -> None:
    try:
        await add_feishu_reaction(message_id, FEISHU_RUN_REACTION)
    except Exception as exc:
        print(f"Failed to add Feishu run reaction: {type(exc).__name__}: {exc}", flush=True)
    try:
        answer = await answer_macro_snapshot(series_id, name)
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

    if action == "cli_query":
        query = str(action_value.get("query", "")).strip()
        if not query:
            return {"toast": {"type": "warning", "content": "\u672a\u627e\u5230\u8981\u4f20\u7ed9 CLI \u7684\u67e5\u8be2"}}
        background_tasks.add_task(process_feishu_query, message_id, query)
        return {"toast": {"type": "success", "content": "\u5df2\u4ea4\u7ed9 OpenBB CLI \u6267\u884c"}}

    if action == "asset":
        asset = str(action_value.get("asset", ""))
        if asset == "equity":
            card = build_equity_console_card()
        elif asset == "etf":
            card = build_etf_console_card()
        elif asset == "index":
            card = build_index_card()
        elif asset == "macro":
            card = build_macro_card()
        elif asset == "crypto":
            card = build_crypto_card()
        else:
            card = build_asset_coming_card(asset)
        await reply_feishu_card(message_id, card)
        return {"toast": {"type": "success", "content": "\u5df2\u6253\u5f00\u6a21\u5757"}}

    if action == "quick_search":
        asset = str(action_value.get("asset", "equity"))
        query = str(action_value.get("query", ""))
        candidates = await search_fmp_candidates(query, asset)
        if not candidates:
            return {"toast": {"type": "warning", "content": "\u6ca1\u6709\u627e\u5230\u5019\u9009\u6807\u7684"}}
        await reply_feishu_card(message_id, build_symbol_candidates_card(query, candidates, asset))
        return {"toast": {"type": "success", "content": "\u5df2\u8fd4\u56de\u5019\u9009"}}

    if action == "select_symbol":
        asset = str(action_value.get("asset", "equity"))
        symbol = str(action_value.get("symbol", "")).upper()
        name = str(action_value.get("name", ""))
        if not symbol:
            return {"toast": {"type": "warning", "content": "\u672a\u8bc6\u522b\u6807\u7684"}}
        label = f"{symbol} {name}".strip()
        query = f"\u67e5 {label} \u7684\u516c\u53f8\u6982\u51b5\u3001\u884c\u60c5\u3001\u4f30\u503c\u3001\u8d22\u52a1\u3001\u6536\u5165\u589e\u957f\u3001\u5229\u6da6\u7387\u3001\u7ba1\u7406\u5c42\u548c\u62c6\u80a1"
        background_tasks.add_task(process_feishu_query, message_id, query)
        return {"toast": {"type": "success", "content": f"\u5df2\u4ea4\u7ed9 OpenBB CLI\uff1a{symbol}"}}

    if action == "run_equity":
        symbol = str(action_value.get("symbol", "")).upper()
        sections = action_value.get("sections") or ["price", "valuation", "growth", "margin"]
        if not symbol:
            return {"toast": {"type": "warning", "content": "\u672a\u8bc6\u522b\u6807\u7684"}}
        if not isinstance(sections, list):
            sections = ["price", "valuation", "growth", "margin"]
        query = f"\u67e5 {symbol} \u7684\u4f30\u503c\u3001\u6536\u5165\u589e\u957f\u3001\u5229\u6da6\u7387\u548c\u6700\u8fd1\u4e00\u5e74\u80a1\u4ef7"
        background_tasks.add_task(process_feishu_query, message_id, query)
        return {"toast": {"type": "success", "content": "\u5df2\u4ea4\u7ed9 OpenBB CLI \u6267\u884c"}}

    if action == "run_price":
        symbol = str(action_value.get("symbol", "")).upper()
        asset = str(action_value.get("asset", "instrument"))
        if not symbol:
            return {"toast": {"type": "warning", "content": "\u672a\u8bc6\u522b\u6807\u7684"}}
        query = f"\u67e5 {symbol} \u7684\u884c\u60c5\u548c\u6700\u8fd1\u4e00\u5e74\u4ef7\u683c\u8868\u73b0"
        background_tasks.add_task(process_feishu_query, message_id, query)
        return {"toast": {"type": "success", "content": "\u5df2\u4ea4\u7ed9 OpenBB CLI \u6267\u884c"}}

    if action == "run_macro":
        series_id = str(action_value.get("series_id", ""))
        name = str(action_value.get("name", series_id))
        if not series_id:
            return {"toast": {"type": "warning", "content": "\u672a\u8bc6\u522b\u5b8f\u89c2\u6307\u6807"}}
        background_tasks.add_task(process_feishu_query, message_id, f"\u67e5 {series_id} {name} \u7684\u5b8f\u89c2\u6570\u636e")
        return {"toast": {"type": "success", "content": "\u5df2\u4ea4\u7ed9 OpenBB CLI \u6267\u884c"}}

    return {"toast": {"type": "warning", "content": "\u672a\u8bc6\u522b\u7684\u5361\u7247\u64cd\u4f5c"}}


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "runtime": "openbb-platform-cli"}


@app.get("/debug/tasks")
async def debug_tasks(authorization: str | None = Header(default=None)) -> dict[str, Any]:
    require_api_token(authorization)
    return {
        "count": len(RECENT_FEISHU_TASKS),
        "tasks": list(RECENT_FEISHU_TASKS.values()),
    }


@app.get("/debug/tasks/{message_id}")
async def debug_task(message_id: str, authorization: str | None = Header(default=None)) -> dict[str, Any]:
    require_api_token(authorization)
    task = RECENT_FEISHU_TASKS.get(message_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@app.post("/debug/run")
async def debug_run(
    payload: ChatRequest,
    background_tasks: BackgroundTasks,
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    require_api_token(authorization)
    message_id = f"debug-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
    remember_feishu_task(message_id, status="queued", input=payload.message, received_at=now_iso())
    background_tasks.add_task(
        process_openbb_task,
        message_id,
        payload.message,
        send_feishu_reply=False,
        timeout_seconds=payload.timeout_seconds or OPENBB_TIMEOUT_SECONDS,
    )
    return {"status": "queued", "message_id": message_id}


@app.post("/debug/prewarm")
async def debug_prewarm(authorization: str | None = Header(default=None)) -> dict[str, Any]:
    require_api_token(authorization)
    started = datetime.now(timezone.utc)
    await get_openbb_obb()
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    return {"status": "ok", "execution_mode": OPENBB_EXECUTION_MODE, "seconds": round(elapsed, 3)}


@app.on_event("startup")
async def startup() -> None:
    load_feishu_tasks()
    write_openbb_user_settings()
    schedule_async_task("prewarm-openbb-platform", prewarm_openbb_platform())
    schedule_async_task("recover-interrupted-feishu-tasks", recover_interrupted_feishu_tasks())


@app.post("/routine")
async def routine_endpoint(payload: RoutineRequest, authorization: str | None = Header(default=None)) -> dict[str, Any]:
    require_api_token(authorization)
    routine = normalize_routine(payload)
    commands = [line for line in routine.splitlines() if line.strip()]
    if OPENBB_EXECUTION_MODE in {"platform_api", "platform", "api"}:
        return await run_openbb_platform_routine(commands, payload.timeout_seconds or OPENBB_TIMEOUT_SECONDS)
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

    remember_feishu_task(message_id, status="queued", input=text, received_at=now_iso())
    print(
        "Feishu message received "
        f"message_id={message_id} text={text[:300]} event_type={event.get('type', '')}",
        flush=True,
    )
    schedule_async_task(
        f"feishu-reaction-{message_id}",
        add_feishu_reaction_safely(message_id, FEISHU_OPEN_REACTION),
    )

    if text:
        schedule_async_task(
            f"feishu-openbb-{message_id}",
            process_feishu_query(message_id, text),
        )
        print(f"Feishu OpenBB task scheduled message_id={message_id}", flush=True)
        return {"status": "openbb_cli_task_started"}

    await reply_feishu_card(message_id, build_query_builder_card())
    return {"status": "console_card_sent"}
