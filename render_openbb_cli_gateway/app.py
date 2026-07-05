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

MIKOTO_BASE_URL = os.getenv("MIKOTO_BASE_URL", "").rstrip("/")
MIKOTO_API_KEY = os.getenv("MIKOTO_API_KEY", "")
MIKOTO_MODEL = os.getenv("MIKOTO_MODEL", "gpt-5.5")
FMP_API_KEY = os.getenv("FMP_API_KEY", "")

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
    for match in re.findall(r"\b[A-Z]{1,6}\b", message.upper()):
        if match not in blocked:
            return match
    return None


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


async def add_feishu_reaction(message_id: str, emoji_type: str = "Typing") -> None:
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

    if FEISHU_VERIFICATION_TOKEN and body.get("token") != FEISHU_VERIFICATION_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid Feishu verification token")

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

    if not text:
        try:
            await add_feishu_reaction(message_id, "OK")
        except Exception as exc:
            print(f"Failed to add Feishu reaction: {type(exc).__name__}: {exc}", flush=True)
        return {"status": "empty"}

    if is_smalltalk_message(text):
        try:
            await add_feishu_reaction(message_id, "OK")
        except Exception as exc:
            print(f"Failed to add Feishu reaction: {type(exc).__name__}: {exc}", flush=True)
        return {"status": "smalltalk_ignored"}

    try:
        await add_feishu_reaction(message_id)
    except Exception as exc:
        print(f"Failed to add Feishu reaction: {type(exc).__name__}: {exc}", flush=True)
    background_tasks.add_task(process_feishu_query, message_id, text)
    return {"status": "accepted"}
