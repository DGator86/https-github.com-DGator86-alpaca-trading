# Trader2B Live Scanner Backend

Persistent market-data ingestion and live state service for the Toro100 scanner.

## Design rule

Execution prices never come from search snippets, finance web pages, or cached quote pages. A trade setup is eligible only while the live market stream is healthy and fresh.

## What it streams

- Every current S&P 500 constituent
- SPY and the major broad/sector ETFs configured in `ETF_SYMBOLS`
- Tradier `quote`, `timesale`, and `summary` events
- Dynamically selected near-ATM option contracts for candidate underlyings

The service maintains live last price, NBBO, cumulative volume, session OHLC, VWAP, 5/15-minute opening ranges, one-minute bars, return versus previous close, relative strength versus SPY, and market breadth. Option quotes can be added to the same live session through the option-watch endpoint.

Tradier documents that one market-data stream can carry several hundred symbols and allows symbols to be changed without closing the stream. The service therefore uses one persistent market session and reconnection logic rather than polling quotes.

## Start

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
cp .env.example .env
# set TRADIER_TOKEN in .env
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## Endpoints

- `GET /health` — stream age and health gate
- `GET /api/state` — compact state for the whole universe
- `GET /api/symbol/{symbol}` — full current state for one symbol
- `GET /api/breadth` — live market breadth
- `POST /api/options/watch/{symbol}` — add near-ATM options for an underlying to the same live stream
- `DELETE /api/options/watch/{symbol}` — remove its option contracts
- `WS /ws/state` — downstream live state stream for scanners/bots

## Hard stale-data gate

`MAX_STREAM_AGE_SECONDS` defaults to 3 seconds during the regular session. If the source stops updating beyond that threshold, `/health` returns `stream_healthy=false`; downstream code must refuse to generate execution entries/stops/targets.

## Deployment

This is a persistent worker and API server, not a serverless request handler. Run it on a host that allows long-running processes and outbound WebSockets/HTTP streaming: Railway, Render, Fly.io, ECS, a VM, or a local always-on machine.
