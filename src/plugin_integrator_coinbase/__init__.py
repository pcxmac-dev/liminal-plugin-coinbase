"""Coinbase Exchange (coinbase.com) for Liminal: crypto minute and day bars from 2015 and its public trade
stream, no key; the reference exchange for crypto (the app's composite model).

A Liminal plugin released on its own, a class on plugin API 2.0 (liminal_libs.sources.archive:
CryptoVenuePlugin, a crypto exchange's; liminal-libs 1.3.0: what it declares class attributes, what
it does methods), registered under the entry point group `liminal.providers` as `coinbase`. It relies
on nothing of the app outside the contract (docs/ARCHITECTURE.md in liminal-charts: "Providers are
plugins"); its version is this distribution's (pyproject.toml), so PLUGIN.version is left empty.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Iterator

from liminal_libs.config import Tier
from liminal_libs.freshness import Delay
from liminal_libs.models import Instrument
from liminal_libs.sources.archive import CRYPTO_PAIRS, EXCHANGE_FRAMES, Archive as _Archive, bounds as _bounds, crypto_pair as _pair
from liminal_libs.sources.archive import CryptoVenuePlugin
from liminal_libs.sources.base import Page
from liminal_libs.sources.stream import Live, ns, trade

BASE_URL = "https://api.exchange.coinbase.com"
# Its plan and caps (once the app's config.PUBLIC_FEEDS["coinbase"]): all the history it holds, unless
# CRYPTO_VENUE_DAYS limits it (CryptoVenuePlugin.view). Coinbase allows 10 public requests a second.
TIER = Tier("coinbase-public", requests_per_minute=300, history_years=None, history_start=date(2015, 1, 26))
PER_RUN, PER_DAY = 3000, 20000


class CoinbaseSource(_Archive):
    """Coinbase Exchange public candles: 300 per request, newest first (time, low, high, open, close,
    volume). Minutes without trades have no candle."""

    name = "coinbase"
    assets = ("crypto",)
    archival = False
    PAGE = 300
    EMPTY_RUN = 200  # empty windows in a row (1-minute candles: about six weeks) before the listing
    STEPS = {("minute", 1): 60, ("minute", 5): 300, ("minute", 15): 900, ("hour", 1): 3600, ("hour", 6): 21600,
             ("day", 1): 86400}

    def resolutions(self, asset: str) -> tuple[str, ...]:
        return ("minute",) if asset == "crypto" else ()

    def native_frames(self, asset: str) -> tuple[tuple[str, int], ...]:
        return EXCHANGE_FRAMES if asset == "crypto" else ()

    def covers(self, ticker: str) -> bool:
        return ticker.upper().removeprefix("X:") in CRYPTO_PAIRS

    def bar_pages(self, ticker, start, end, timespan, multiplier=1, descending=False, asset="crypto",
                  limit: int | None = None) -> Iterator[list[dict]]:
        if (timespan, multiplier) not in self.STEPS:
            raise ValueError(f"Coinbase granularities: {tuple(self.STEPS)}")
        base, quote = _pair(ticker)
        step = self.STEPS[(timespan, multiplier)]
        a, b = _bounds(start, end)
        a_s, b_s = -(-a // 1000) // step * step, b // 1000
        windows = []
        t = a_s
        while t < b_s:
            windows.append((t, min(b_s, t + self.PAGE * step)))
            t += self.PAGE * step
        empty = 0
        for lo, hi in (reversed(windows) if descending else windows):
            if descending and empty >= self.EMPTY_RUN:
                return  # this long with nothing, going back: before the pair was listed (the sync records the rest as empty)
            iso = lambda s: datetime.fromtimestamp(s, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
            rows = self.client.get(f"/products/{base}-{quote}/candles",
                                   {"granularity": step, "start": iso(lo), "end": iso(hi - step)})
            empty = 0 if rows else empty + 1
            bars = [{"t": int(r[0]) * 1000, "o": float(r[3]), "h": float(r[2]), "l": float(r[1]), "c": float(r[4]),
                     "v": float(r[5]), "vw": None, "n": None} for r in rows or [] if lo <= int(r[0]) < hi]
            bars.sort(key=lambda x: -x["t"] if descending else x["t"])
            yield Page(bars[:limit] if limit else bars, window=(lo * 1000, hi * 1000))
            if limit:
                return

    def instrument(self, ticker: str, resolve_exchange: bool = True) -> Instrument:
        base, quote = _pair(ticker)
        return Instrument(ticker=base + quote, name=f"{base}/{quote} on Coinbase", details={"venue": "Coinbase"})


@dataclass
class CoinbaseStream:
    """Coinbase Exchange's public feed: the matches channel (every trade) and a heartbeat a second.
    `dynamic`: its coins are those open on a chart (`change` while connected), not all LIVE_COINS."""
    source: str = "coinbase"
    url: str = "wss://ws-feed.exchange.coinbase.com"
    keepalive: str | None = None
    dynamic: bool = False
    asset: str = "crypto"
    authed: bool = True  # nothing to authenticate: symbols can change as soon as it is connected
    confirmed: frozenset = frozenset()  # the pairs the server says it streams (dynamic)

    def subscribe(self, coins) -> list[str]:
        self.confirmed = frozenset()
        return [json.dumps({"type": "subscribe", "product_ids": [f"{c}-USD" for c in coins],
                            "channels": ["matches", "heartbeat"]})] if coins else []

    def change(self, add, drop) -> list[str]:
        """Messages taking pairs (BTCUSD) off and onto the connection."""
        return [json.dumps({"type": kind, "product_ids": [f"{t[:-3]}-USD" for t in sorted(pairs)],
                            "channels": ["matches", "heartbeat"]})
                for kind, pairs in (("unsubscribe", drop), ("subscribe", add)) if pairs]

    def parse(self, msg: dict) -> list[tuple[str, dict]]:
        kind = msg.get("type")
        if kind == "error":  # e.g. "Failed to subscribe" "subscription limit reached": a refusal
            raise ConnectionError(f"coinbase: {msg.get('message')} {msg.get('reason') or ''}".strip())
        if kind == "subscriptions":  # the whole list after each change
            self.confirmed = frozenset(p.replace("-", "") for ch in msg.get("channels") or []
                                       if isinstance(ch, dict) and ch.get("name") == "matches"
                                       for p in ch.get("product_ids") or [])
            return []
        if kind not in ("match", "last_match"):
            return []
        ticker = msg["product_id"].replace("-", "")
        tid = int(msg["trade_id"])
        return [(ticker, trade(ns(msg["time"]), tid, msg["price"], msg["size"], tid))]


class CoinbasePlugin(CryptoVenuePlugin):
    """The feed's plugin, a crypto exchange's (its `assets` and `kind`; its source on the contract's
    public client at `base_url`; its view: the plan and caps, all the history it holds unless
    CRYPTO_VENUE_DAYS limits it): what it declares as attributes, what it does as methods (the window
    its limiter keeps, the pairs it carries); the contract's defaults for the rest."""

    api = "2.0"  # the contract it is written for (the class contract); its version: the distribution's
    name = "coinbase"
    title = "Coinbase"
    about = "crypto minute and day bars from 2015 (no key); live trades"
    order = 30
    base_url = BASE_URL
    source_class = CoinbaseSource
    tier = TIER
    per_run = PER_RUN
    per_day = PER_DAY
    limits = "10 requests per second per IP (public endpoints)"
    reference_for = frozenset({"crypto"})
    per_request = {"day": CoinbaseSource.PAGE, "minute": CoinbaseSource.PAGE}
    streams = {"crypto": "real time, public trades (no key), coins open on a chart"}
    live = (Live("coinbase", "crypto", stream=CoinbaseStream),)
    # when its data is published (the library's freshness row for its plan): each minute once it closes
    delays = {"coinbase-public": {"crypto": Delay("delay")}}

    def rate(self, settings):
        return (8, 1.0)  # 10 a second per address: a minute's window let 300 go in a burst

    def carries(self, spec):
        return spec.ticker.upper() in CRYPTO_PAIRS


PLUGIN = CoinbasePlugin()
