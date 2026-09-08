import asyncio
import json
import math
import os
import time
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, time as dtime
from typing import Dict, List, Optional, Set
from zoneinfo import ZoneInfo

import aiohttp
import pandas as pd
import websockets
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic_settings import BaseSettings, SettingsConfigDict

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    tradier_token: str
    tradier_base_url: str = "https://api.tradier.com"
    tradier_ws_url: str = "wss://ws.tradier.com/v1/markets/events"
    max_stream_age_seconds: float = 3.0
    universe_refresh_minutes: int = 360
    state_push_interval_ms: int = 1000
    option_strikes_each_side: int = 2
    risk_free_rate: float = 0.04
    etf_symbols: str = "SPY,QQQ,IWM,DIA,XLK,XLF,XLE,XLV,XLY,XLP,XLI,XLB,XLU,XLRE,SMH,SOXX,IYT"

    @property
    def etfs(self) -> List[str]:
        return [x.strip().upper() for x in self.etf_symbols.split(",") if x.strip()]


settings = Settings()


@dataclass
class MinuteBar:
    minute: str
    open: float
    high: float
    low: float
    close: float
    volume: int = 0


@dataclass
class SymbolState:
    symbol: str
    sector: Optional[str] = None
    is_option: bool = False
    underlying: Optional[str] = None
    strike: Optional[float] = None
    option_type: Optional[str] = None
    expiration: Optional[str] = None

    bid: Optional[float] = None
    ask: Optional[float] = None
    last: Optional[float] = None
    last_size: Optional[int] = None
    cvol: Optional[int] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    prev_close: Optional[float] = None
    avg_volume: Optional[float] = None

    vwap_num: float = 0.0
    vwap_den: float = 0.0
    vwap: Optional[float] = None
    or5_high: Optional[float] = None
    or5_low: Optional[float] = None
    or15_high: Optional[float] = None
    or15_low: Optional[float] = None
    session_return_pct: Optional[float] = None
    rs_vs_spy_pct: Optional[float] = None
    rvol_pace: Optional[float] = None

    last_event_ts: Optional[float] = None
    last_quote_ts: Optional[float] = None
    last_trade_ts: Optional[float] = None
    bars: deque = field(default_factory=lambda: deque(maxlen=390))

    def compact(self) -> dict:
        now = time.time()
        return {
            "symbol": self.symbol,
            "sector": self.sector,
            "is_option": self.is_option,
            "underlying": self.underlying,
            "strike": self.strike,
            "option_type": self.option_type,
            "expiration": self.expiration,
            "bid": self.bid,
            "ask": self.ask,
            "last": self.last,
            "cvol": self.cvol,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "prev_close": self.prev_close,
            "vwap": self.vwap,
            "or5_high": self.or5_high,
            "or5_low": self.or5_low,
            "or15_high": self.or15_high,
            "or15_low": self.or15_low,
            "session_return_pct": self.session_return_pct,
            "rs_vs_spy_pct": self.rs_vs_spy_pct,
            "rvol_pace": self.rvol_pace,
            "age_seconds": None if self.last_event_ts is None else round(now - self.last_event_ts, 3),
            "last_event_ts": self.last_event_ts,
        }


class MarketStore:
    def __init__(self):
        self.states: Dict[str, SymbolState] = {}
        self.latest_event_ts: Optional[float] = None
        self.connected = False
        self.session_id: Optional[str] = None
        self.source = "tradier_stream"
        self.universe: Set[str] = set()
        self.option_symbols: Set[str] = set()
        self.option_underlyings: Set[str] = set()
        self._lock = asyncio.Lock()

    async def ensure(self, symbol: str, **kwargs) -> SymbolState:
        async with self._lock:
            if symbol not in self.states:
                self.states[symbol] = SymbolState(symbol=symbol, **kwargs)
            else:
                for k, v in kwargs.items():
                    if v is not None:
                        setattr(self.states[symbol], k, v)
            return self.states[symbol]

    def stream_age(self) -> Optional[float]:
        if self.latest_event_ts is None:
            return None
        return max(0.0, time.time() - self.latest_event_ts)

    def stream_healthy(self) -> bool:
        age = self.stream_age()
        if not self.connected or age is None:
            return False
        now = datetime.now(ET).time()
        regular = dtime(9, 30) <= now <= dtime(16, 0)
        limit = settings.max_stream_age_seconds if regular else max(30.0, settings.max_stream_age_seconds)
        return age <= limit


store = MarketStore()
app = FastAPI(title="Trader2B Live Scanner Backend", version="1.0.0")
streamer = None


def _event_dt(event: dict) -> datetime:
    raw = event.get("date") or event.get("biddate") or event.get("askdate")
    if raw is None:
        return datetime.now(ET)
    try:
        value = float(raw)
        if value > 1e12:
            value /= 1000.0
        return datetime.fromtimestamp(value, tz=UTC).astimezone(ET)
    except Exception:
        return datetime.now(ET)


def _regular_session(dt: datetime, event: dict) -> bool:
    session = str(event.get("session", "normal")).lower()
    return session == "normal" and dtime(9, 30) <= dt.time() <= dtime(16, 0)


def _update_bar(s: SymbolState, dt: datetime, price: float, size: int):
    key = dt.strftime("%Y-%m-%dT%H:%M")
    if not s.bars or s.bars[-1].minute != key:
        s.bars.append(MinuteBar(key, price, price, price, price, size))
    else:
        b = s.bars[-1]
        b.high = max(b.high, price)
        b.low = min(b.low, price)
        b.close = price
        b.volume += size


def _update_opening_ranges(s: SymbolState, dt: datetime, price: float):
    t = dt.time()
    if dtime(9, 30) <= t < dtime(9, 35):
        s.or5_high = price if s.or5_high is None else max(s.or5_high, price)
        s.or5_low = price if s.or5_low is None else min(s.or5_low, price)
    if dtime(9, 30) <= t < dtime(9, 45):
        s.or15_high = price if s.or15_high is None else max(s.or15_high, price)
        s.or15_low = price if s.or15_low is None else min(s.or15_low, price)


def _expected_session_fraction(now: datetime) -> float:
    start = now.replace(hour=9, minute=30, second=0, microsecond=0)
    end = now.replace(hour=16, minute=0, second=0, microsecond=0)
    if now <= start:
        return 0.01
    if now >= end:
        return 1.0
    return max(0.01, min(1.0, (now - start).total_seconds() / (end - start).total_seconds()))


def _recompute_derived(s: SymbolState):
    if s.last is not None and s.prev_close:
        s.session_return_pct = 100.0 * (s.last / s.prev_close - 1.0)
    spy = store.states.get("SPY")
    if s.session_return_pct is not None and spy and spy.last is not None and spy.prev_close:
        spy_ret = 100.0 * (spy.last / spy.prev_close - 1.0)
        s.rs_vs_spy_pct = s.session_return_pct - spy_ret
    if s.cvol is not None and s.avg_volume:
        frac = _expected_session_fraction(datetime.now(ET))
        expected = max(1.0, s.avg_volume * frac)
        s.rvol_pace = s.cvol / expected


async def handle_event(event: dict):
    symbol = str(event.get("symbol", "")).upper()
    if not symbol:
        return
    s = await store.ensure(symbol)
    now_ts = time.time()
    store.latest_event_ts = now_ts
    s.last_event_ts = now_ts
    etype = event.get("type")

    if etype == "quote":
        if event.get("bid") is not None:
            s.bid = float(event["bid"])
        if event.get("ask") is not None:
            s.ask = float(event["ask"])
        s.last_quote_ts = now_ts

    elif etype == "summary":
        for attr, key in (("open", "open"), ("high", "high"), ("low", "low"), ("prev_close", "prevClose")):
            if event.get(key) is not None:
                setattr(s, attr, float(event[key]))

    elif etype in {"timesale", "trade", "tradex"}:
        raw_price = event.get("last") if event.get("last") is not None else event.get("price")
        if raw_price is None:
            return
        price = float(raw_price)
        size = int(float(event.get("size") or 0))
        if event.get("cvol") is not None:
            try:
                s.cvol = int(float(event["cvol"]))
            except Exception:
                pass
        if etype == "timesale":
            if event.get("cancel") or event.get("correction"):
                return
            if event.get("bid") is not None:
                s.bid = float(event["bid"])
            if event.get("ask") is not None:
                s.ask = float(event["ask"])
        s.last = price
        s.last_size = size
        s.last_trade_ts = now_ts
        dt = _event_dt(event)
        if _regular_session(dt, event):
            if s.open is None:
                s.open = price
            s.high = price if s.high is None else max(s.high, price)
            s.low = price if s.low is None else min(s.low, price)
            if size > 0 and etype == "timesale":
                s.vwap_num += price * size
                s.vwap_den += size
                s.vwap = s.vwap_num / s.vwap_den
                _update_bar(s, dt, price, size)
                _update_opening_ranges(s, dt, price)

    _recompute_derived(s)


async def load_universe() -> Dict[str, str]:
    def _load():
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        frame = tables[0]
        result = {}
        for _, row in frame.iterrows():
            sym = str(row["Symbol"]).strip().upper()
            sector = str(row.get("GICS Sector", "")).strip() or None
            result[sym] = sector
        return result

    result = await asyncio.to_thread(_load)
    if len(result) < 450:
        raise RuntimeError(f"S&P 500 universe load returned only {len(result)} symbols")
    return result


class TradierStreamer:
    def __init__(self):
        self.http: Optional[aiohttp.ClientSession] = None
        self.ws = None
        self.base_symbols: Set[str] = set()
        self.option_symbols: Set[str] = set()
        self._send_lock = asyncio.Lock()
        self._stop = asyncio.Event()

    @property
    def headers(self):
        return {"Authorization": f"Bearer {settings.tradier_token}", "Accept": "application/json"}

    async def start(self):
        self.http = aiohttp.ClientSession(headers=self.headers)
        asyncio.create_task(self._universe_refresh_loop())
        asyncio.create_task(self._connection_loop())

    async def stop(self):
        self._stop.set()
        if self.ws:
            await self.ws.close()
        if self.http:
            await self.http.close()

    async def _universe_refresh_loop(self):
        while not self._stop.is_set():
            try:
                sectors = await load_universe()
                symbols = set(sectors) | set(settings.etfs)
                self.base_symbols = symbols
                store.universe = symbols
                for sym in symbols:
                    await store.ensure(sym, sector=sectors.get(sym))
                await self._bootstrap_reference_quotes(symbols)
                if self.ws:
                    await self.resubscribe()
            except Exception as exc:
                print(f"universe refresh failed: {exc}")
            await asyncio.sleep(settings.universe_refresh_minutes * 60)

    async def _bootstrap_reference_quotes(self, symbols: Set[str]):
        if not self.http:
            return
        all_syms = sorted(symbols)
        for i in range(0, len(all_syms), 100):
            chunk = all_syms[i:i + 100]
            try:
                async with self.http.get(
                    f"{settings.tradier_base_url}/v1/markets/quotes",
                    params={"symbols": ",".join(chunk), "greeks": "false"},
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as r:
                    if r.status != 200:
                        continue
                    data = await r.json()
                    quotes = (((data or {}).get("quotes") or {}).get("quote"))
                    if isinstance(quotes, dict):
                        quotes = [quotes]
                    for q in quotes or []:
                        sym = str(q.get("symbol", "")).upper()
                        if not sym:
                            continue
                        s = await store.ensure(sym)
                        for field_name, key in (("prev_close", "prevclose"), ("avg_volume", "average_volume"), ("open", "open"), ("high", "high"), ("low", "low"), ("last", "last")):
                            if q.get(key) not in (None, ""):
                                try:
                                    setattr(s, field_name, float(q[key]))
                                except Exception:
                                    pass
                        if q.get("volume") not in (None, ""):
                            try:
                                s.cvol = int(float(q["volume"]))
                            except Exception:
                                pass
                        _recompute_derived(s)
            except Exception as exc:
                print(f"reference bootstrap chunk failed: {exc}")

    async def _create_session(self) -> str:
        assert self.http is not None
        async with self.http.post(
            f"{settings.tradier_base_url}/v1/markets/events/session",
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            text = await r.text()
            if r.status != 200:
                raise RuntimeError(f"create session failed {r.status}: {text[:300]}")
            data = json.loads(text)
            session_id = data["stream"]["sessionid"]
            store.session_id = session_id
            return session_id

    async def _connection_loop(self):
        backoff = 1
        while not self._stop.is_set():
            try:
                if not self.base_symbols:
                    await asyncio.sleep(1)
                    continue
                sid = await self._create_session()
                async with websockets.connect(
                    settings.tradier_ws_url,
                    additional_headers={"Authorization": f"Bearer {settings.tradier_token}"},
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_size=None,
                ) as ws:
                    self.ws = ws
                    store.connected = True
                    backoff = 1
                    await self.resubscribe(session_id=sid)
                    async for raw in ws:
                        try:
                            event = json.loads(raw)
                            if isinstance(event, list):
                                for e in event:
                                    await handle_event(e)
                            elif isinstance(event, dict):
                                if "error" in event:
                                    print(f"Tradier stream error: {event}")
                                else:
                                    await handle_event(event)
                        except Exception as exc:
                            print(f"event parse/handle failed: {exc}")
            except Exception as exc:
                print(f"stream disconnected: {exc}")
            finally:
                store.connected = False
                self.ws = None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    async def resubscribe(self, session_id: Optional[str] = None):
        if not self.ws:
            return
        sid = session_id or store.session_id
        if not sid:
            return
        symbols = sorted(self.base_symbols | self.option_symbols)
        payload = {
            "symbols": symbols,
            "sessionid": sid,
            "filter": ["quote", "timesale", "summary"],
            "linebreak": True,
            "validOnly": True,
            "advancedDetails": True,
        }
        async with self._send_lock:
            await self.ws.send(json.dumps(payload))

    async def watch_options(self, underlying: str) -> List[str]:
        underlying = underlying.upper()
        s = store.states.get(underlying)
        if not s or s.last is None:
            raise HTTPException(409, f"No live underlying price for {underlying}")
        assert self.http is not None

        async with self.http.get(
            f"{settings.tradier_base_url}/v1/markets/options/expirations",
            params={"symbol": underlying, "includeAllRoots": "true", "strikes": "false"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status != 200:
                raise HTTPException(r.status, f"Tradier expiration lookup failed: {(await r.text())[:200]}")
            data = await r.json()
        dates = (((data or {}).get("expirations") or {}).get("date"))
        if isinstance(dates, str):
            dates = [dates]
        if not dates:
            raise HTTPException(404, f"No option expirations for {underlying}")
        today = datetime.now(ET).date().isoformat()
        expiration = next((d for d in dates if d >= today), dates[0])

        async with self.http.get(
            f"{settings.tradier_base_url}/v1/markets/options/chains",
            params={"symbol": underlying, "expiration": expiration, "greeks": "true"},
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            if r.status != 200:
                raise HTTPException(r.status, f"Tradier chain lookup failed: {(await r.text())[:200]}")
            chain = await r.json()
        options = (((chain or {}).get("options") or {}).get("option"))
        if isinstance(options, dict):
            options = [options]
        options = options or []

        strikes = sorted({float(o["strike"]) for o in options if o.get("strike") is not None}, key=lambda x: abs(x - s.last))
        selected_strikes = set(strikes[: max(2, 1 + 2 * settings.option_strikes_each_side)])
        selected = []
        for o in options:
            strike = float(o.get("strike", -1))
            if strike not in selected_strikes:
                continue
            osym = str(o.get("symbol", "")).upper()
            if not osym:
                continue
            selected.append(osym)
            await store.ensure(
                osym,
                is_option=True,
                underlying=underlying,
                strike=strike,
                option_type=o.get("option_type"),
                expiration=o.get("expiration_date") or expiration,
            )
        self.option_symbols.update(selected)
        store.option_symbols = set(self.option_symbols)
        store.option_underlyings.add(underlying)
        await self.resubscribe()
        return selected

    async def unwatch_options(self, underlying: str):
        underlying = underlying.upper()
        remove = {sym for sym in self.option_symbols if store.states.get(sym) and store.states[sym].underlying == underlying}
        self.option_symbols -= remove
        store.option_symbols = set(self.option_symbols)
        store.option_underlyings.discard(underlying)
        await self.resubscribe()
        return sorted(remove)


@app.on_event("startup")
async def startup():
    global streamer
    streamer = TradierStreamer()
    await streamer.start()


@app.on_event("shutdown")
async def shutdown():
    if streamer:
        await streamer.stop()


@app.get("/health")
async def health():
    age = store.stream_age()
    return {
        "source": store.source,
        "connected": store.connected,
        "stream_healthy": store.stream_healthy(),
        "stream_age_seconds": None if age is None else round(age, 3),
        "max_regular_session_age_seconds": settings.max_stream_age_seconds,
        "latest_event_ts": store.latest_event_ts,
        "latest_event_et": None if store.latest_event_ts is None else datetime.fromtimestamp(store.latest_event_ts, ET).isoformat(),
        "universe_count": len(store.universe),
        "streamed_option_count": len(store.option_symbols),
        "option_underlyings": sorted(store.option_underlyings),
        "execution_gate": "OPEN" if store.stream_healthy() else "CLOSED_STALE_OR_DISCONNECTED",
    }


@app.get("/api/symbol/{symbol}")
async def symbol_state(symbol: str):
    symbol = symbol.upper()
    s = store.states.get(symbol)
    if not s:
        raise HTTPException(404, "symbol not loaded")
    data = s.compact()
    data["stream_healthy"] = store.stream_healthy()
    data["execution_eligible"] = store.stream_healthy() and data["age_seconds"] is not None and data["age_seconds"] <= settings.max_stream_age_seconds
    data["bars"] = [asdict(b) for b in s.bars]
    return data


@app.get("/api/state")
async def market_state(limit: int = 600):
    rows = [s.compact() for s in store.states.values() if not s.is_option]
    rows.sort(key=lambda x: (x["symbol"] != "SPY", x["symbol"]))
    return {
        "generated_et": datetime.now(ET).isoformat(),
        "stream_healthy": store.stream_healthy(),
        "execution_gate": "OPEN" if store.stream_healthy() else "CLOSED",
        "symbols": rows[: max(1, min(limit, 1000))],
    }


@app.get("/api/breadth")
async def breadth():
    equities = [s for s in store.states.values() if not s.is_option and s.symbol in store.universe and s.last is not None]
    advances = sum(1 for s in equities if s.session_return_pct is not None and s.session_return_pct > 0)
    declines = sum(1 for s in equities if s.session_return_pct is not None and s.session_return_pct < 0)
    above_vwap = sum(1 for s in equities if s.vwap is not None and s.last is not None and s.last > s.vwap)
    vwap_known = sum(1 for s in equities if s.vwap is not None and s.last is not None)
    by_sector = {}
    for s in equities:
        sec = s.sector or "Unknown"
        x = by_sector.setdefault(sec, {"n": 0, "adv": 0, "dec": 0, "avg_return_pct": 0.0})
        x["n"] += 1
        if s.session_return_pct is not None:
            x["avg_return_pct"] += s.session_return_pct
            if s.session_return_pct > 0:
                x["adv"] += 1
            elif s.session_return_pct < 0:
                x["dec"] += 1
    for x in by_sector.values():
        if x["n"]:
            x["avg_return_pct"] /= x["n"]
    return {
        "generated_et": datetime.now(ET).isoformat(),
        "stream_healthy": store.stream_healthy(),
        "covered": len(equities),
        "advances": advances,
        "declines": declines,
        "advance_pct": None if not equities else advances / len(equities),
        "above_vwap_pct": None if not vwap_known else above_vwap / vwap_known,
        "sectors": by_sector,
    }


@app.post("/api/options/watch/{symbol}")
async def option_watch(symbol: str):
    if not streamer:
        raise HTTPException(503, "streamer unavailable")
    selected = await streamer.watch_options(symbol)
    return {"underlying": symbol.upper(), "streamed_contracts": selected}


@app.delete("/api/options/watch/{symbol}")
async def option_unwatch(symbol: str):
    if not streamer:
        raise HTTPException(503, "streamer unavailable")
    removed = await streamer.unwatch_options(symbol)
    return {"underlying": symbol.upper(), "removed_contracts": removed}


@app.websocket("/ws/state")
async def ws_state(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            equities = [s.compact() for s in store.states.values() if not s.is_option]
            equities.sort(key=lambda x: abs(x.get("session_return_pct") or 0), reverse=True)
            payload = {
                "generated_et": datetime.now(ET).isoformat(),
                "stream_healthy": store.stream_healthy(),
                "stream_age_seconds": store.stream_age(),
                "execution_gate": "OPEN" if store.stream_healthy() else "CLOSED",
                "spy": store.states.get("SPY").compact() if store.states.get("SPY") else None,
                "top_movers": equities[:30],
                "watched_options": [store.states[s].compact() for s in sorted(store.option_symbols) if s in store.states],
            }
            await ws.send_json(payload)
            await asyncio.sleep(settings.state_push_interval_ms / 1000.0)
    except WebSocketDisconnect:
        return
