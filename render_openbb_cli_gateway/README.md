# OpenBB Platform CLI Gateway for Render

This deploys the current OpenBB Platform CLI (`openbb-cli`) on Render and exposes a small HTTP gateway for automation.

It is not the legacy OpenBB Terminal.

## Endpoints

- `GET /health`
- `POST /routine` with `Authorization: Bearer $API_TOKEN`
- `POST /chat` with `Authorization: Bearer $API_TOKEN`
- `POST /feishu/events` for Feishu event callbacks

## Render

Use `render_openbb_cli_gateway` as the root directory.

Build command:

```text
pip install -r requirements.txt
```

Start command:

```text
uvicorn app:app --host 0.0.0.0 --port $PORT
```

## Required environment variables

- `API_TOKEN`
- `MIKOTO_BASE_URL`
- `MIKOTO_API_KEY`
- `MIKOTO_MODEL`
- `FEISHU_APP_ID`
- `FEISHU_APP_SECRET`

Optional:

- `FEISHU_VERIFICATION_TOKEN`
- `OPENBB_TIMEOUT_SECONDS`
- `OPENBB_ALLOWED_PREFIXES`
