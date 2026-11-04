"""The Coinbase plugin, run against the app installed beside it: its candle windows and its trade
stream's messages, and the app loading it from this package with its own version and the facts it
had as a built-in."""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from importlib.metadata import version

import pytest

from liminal_libs import composite, freshness
from liminal_libs.config import load_settings
from liminal_libs.freshness import Delay
from liminal_libs.sources import is_archive, markets, provider, rate_window, registry, supports
from liminal_libs.sources.archive import CryptoVenuePlugin
from liminal_libs.sources.http import PublicClient
from liminal_libs.symbols import resolve

import plugin_integrator_coinbase
from plugin_integrator_coinbase import BASE_URL, PER_DAY, PER_RUN, TIER, CoinbasePlugin, CoinbaseSource, CoinbaseStream

from .conftest import FakeResponse, FakeSession


def _client(responses):
    session = FakeSession(responses)
    return PublicClient("https://x.test/api", session=session, sleep=lambda s: None), session


def test_it_asks_for_candles_by_granularity_in_windows_of_three_hundred():
    t0 = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())
    lo, hi = datetime.fromtimestamp(t0, timezone.utc), datetime.fromtimestamp(t0 + 3600, timezone.utc)
    client, session = _client([FakeResponse(200, [])] * 2)
    assert [list(p) for p in CoinbaseSource(client).bar_pages("X:BTCUSD", lo, hi, "hour", 1)] == [[]]
    url, params, headers = session.calls[0]
    assert url.endswith("/products/BTC-USD/candles") and params["granularity"] == 3600 and "liminal" in headers["User-Agent"]
    assert params == {"granularity": 3600, "start": "2024-01-01T00:00:00Z", "end": "2024-01-01T00:00:00Z"}  # one hour: one candle
    # minutes: a window of 300 candles a request, Coinbase's rows (time, low, high, open, close, volume)
    rows = [[t0 + 60 * i, 9.0, 11.0, 10.0, 10.5, 2.0] for i in range(3)]
    client, session = _client([FakeResponse(200, rows)])
    pages = list(CoinbaseSource(client).bar_pages("X:BTCUSD", lo, hi, "minute", 1))
    assert len(pages) == 1 and pages[0].window == (t0 * 1000, (t0 + 3600) * 1000)
    assert list(pages[0])[0] == {"t": t0 * 1000, "o": 10.0, "h": 11.0, "l": 9.0, "c": 10.5, "v": 2.0, "vw": None, "n": None}
    assert session.calls[0][1] == {"granularity": 60, "start": "2024-01-01T00:00:00Z", "end": "2024-01-01T00:59:00Z"}
    with pytest.raises(ValueError):
        list(CoinbaseSource(client).bar_pages("X:BTCUSD", lo, hi, "minute", 3))  # not a Coinbase granularity
    assert CoinbaseSource(client).native_frames("crypto") == (("minute", 5), ("hour", 1))
    inst = CoinbaseSource(client).instrument("X:ETHUSD")
    assert inst.ticker == "ETHUSD" and inst.details["venue"] == "Coinbase"


def test_it_stops_going_back_after_a_long_run_of_empty_windows(monkeypatch):
    class Empty(PublicClient):
        def __init__(self):
            self.requests_made = 0

        def get(self, path, params=None):
            self.requests_made += 1
            return []

    src = CoinbaseSource(Empty())
    monkeypatch.setattr(CoinbaseSource, "EMPTY_RUN", 5)
    pages = list(src.bar_pages("SOLUSD", date(2015, 1, 26), date(2026, 10, 20), "minute", descending=True))
    assert src.client.requests_made == 5 and all(list(p) == [] for p in pages)  # not ~20,000 empty windows


def test_its_stream_messages_become_trades_and_its_subscriptions_are_confirmed():
    s = CoinbaseStream()
    cb = s.parse({"type": "match", "trade_id": 7, "product_id": "ETH-USD", "price": "2500.10",
                  "size": "0.5", "time": "2026-10-24T07:00:00.123456Z"})
    assert cb == [("ETHUSD", {"sip_timestamp": 1_792_825_200_123_456_000, "sequence_number": 7, "price": 2500.1,
                              "size": 0.5, "exchange": None, "conditions": [], "id": "7", "tape": None})]
    assert s.parse({"type": "heartbeat"}) == []
    with pytest.raises(ConnectionError, match="subscription limit"):
        s.parse({"type": "error", "message": "Failed to subscribe", "reason": "subscription limit reached"})
    assert s.subscribe(()) == []  # connected with nothing yet: coins come and go while connected (dynamic)
    sub = json.loads(s.subscribe(("BTC", "ETH"))[0])
    assert sub == {"type": "subscribe", "product_ids": ["BTC-USD", "ETH-USD"], "channels": ["matches", "heartbeat"]}
    s.parse({"type": "subscriptions", "channels": [{"name": "matches", "product_ids": ["BTC-USD", "ETH-USD"]},
                                                    {"name": "heartbeat", "product_ids": ["BTC-USD", "ETH-USD"]}]})
    assert s.confirmed == {"BTCUSD", "ETHUSD"}
    changes = [json.loads(m) for m in s.change(add={"SOLUSD"}, drop={"ETHUSD"})]
    assert [(c["type"], c["product_ids"]) for c in changes] == [("unsubscribe", ["ETH-USD"]), ("subscribe", ["SOL-USD"])]


def test_the_app_loads_it_from_this_package_with_its_own_version():
    registry.reload()
    p = provider("coinbase")
    assert p is not None, dict(registry.errors())
    assert p.origin in {"package: plugin-integrator-coinbase (plugin_integrator_coinbase:PLUGIN)", "plugins/plugin-integrator-coinbase (plugin_integrator_coinbase:PLUGIN)"}  # its package, or its checkout in the workspace\'s plugin directory
    assert p.version == version("plugin-integrator-coinbase") and plugin_integrator_coinbase.PLUGIN.version == ""
    assert registry.api_refusal(p.api) is None
    assert not [why for where, why in registry.errors() if "coinbase" in where or "coinbase" in why]
    row = {r["name"]: r for r in registry.menu()}["coinbase"]
    assert (row["version"], row["api"]) == (version("plugin-integrator-coinbase"), "2.0")


def test_it_keeps_the_facts_it_had_built_in():
    registry.reload()
    s = load_settings(env={}, env_file=None)
    view = s.for_source("coinbase")
    assert (view.tier, view.max_requests_per_run, view.max_requests_per_day) == (TIER, PER_RUN, PER_DAY)
    assert rate_window("coinbase", s) == (8, 1.0)  # 10 a second per address
    limited = load_settings(env={"CRYPTO_VENUE_DAYS": "30"}, env_file=None).for_source("coinbase")
    assert limited.tier.history_days == 30 and limited.tier.history_start == TIER.history_start
    assert not is_archive("coinbase") and markets("coinbase", s) == {"crypto": "coinbase"}
    assert supports("coinbase", resolve("BTCUSD")) and not supports("coinbase", resolve("AAPL"))
    assert composite.feed_kind("coinbase", "crypto") == "exchange"
    assert composite.reference("crypto") == "coinbase"  # the reference exchange for crypto
    p = provider("coinbase")
    assert [(f.name, f.asset, f.stream) for f in p.live] == [("coinbase", "crypto", CoinbaseStream)]


def test_it_is_a_class_on_the_contract_and_the_app_loads_it_as_one():
    p = plugin_integrator_coinbase.PLUGIN
    assert isinstance(p, CoinbasePlugin) and isinstance(p, CryptoVenuePlugin) and p.api == "2.0" and not p.is_record
    assert p.assets == frozenset({"crypto"}) and p.kind == "exchange"  # a crypto venue's, from its base
    assert p.gives("build") and p.gives("view") and p.gives("rate") and p.gives("carries")
    assert not p.has_scan and not p.has_settings and not p.gives("markets")
    assert p.delays == {"coinbase-public": {"crypto": Delay("delay")}} and p.hours == {}
    loaded = registry.reload()["coinbase"]
    assert type(loaded) is CoinbasePlugin and (loaded.version, loaded.api) == (version("plugin-integrator-coinbase"), "2.0")
    assert not [why for where, why in registry.errors() if "coinbase" in where or "coinbase" in why]
    src = loaded.build(load_settings(env={}, env_file=None), 5)  # its base's build: its source on the public client
    assert isinstance(src, CoinbaseSource) and isinstance(src.client, PublicClient) and src.client.base_url == BASE_URL
    assert freshness.plan_delay(TIER, "crypto").kind == "delay"  # its own rule (CoinbasePlugin.delays; the library's too)
    moved = p.with_(order=1)  # a changed copy (the registry's version and origin come this way)
    assert type(moved) is CoinbasePlugin and moved.order == 1 and p.order == 30 and moved.rate(None) == (8, 1.0)
