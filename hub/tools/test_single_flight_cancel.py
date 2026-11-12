#!/usr/bin/env python3
"""
A coalesced caller is never handed another caller's cancellation (B-241).

The hub has two single-flight sites: `MoodService._fetch` (one CLAP search per prompt
in flight) and `HttpProxy.call` (identical GETs share one upstream call). Both used to
run the shared work INLINE in the first caller (the leader) and forward whatever ended
it to everyone who joined: when the leader was cancelled (the handshake deadline on a
`/sonic/mood/*` route, a client hanging up, shutdown), its own CancelledError was put on
the shared future and every follower's `shield(fut)` raised it, although nobody had
cancelled the followers and the work itself had not failed.

In-process, no sockets. SONIC.call / `_call_upstream` are replaced by stubs held on an
event; callers are ordered by events and a join count, never by sleeps.

Cases:
  mood_leader_cancelled      leader A and follower B ask one prompt; A is cancelled
                             mid-search: B gets the rows, one search ran, the rows are
                             stored for the next ask
  mood_all_cancelled         every caller cancelled: the search still lands (stored),
                             and nothing is logged as "exception never retrieved"
  mood_all_cancelled_then_fails  every caller cancelled, then the search raises: the
                             loop's exception handler stays silent (on 3.13+ too, where
                             `shield` logs an abandoned inner's exception itself)
  mood_failure_shared        a search that raises (not a cancel) reaches every caller as
                             that error, once
  proxy_leader_cancelled     the same through HttpProxy.call: B gets the answer, one
                             upstream call
  proxy_follower_cancelled   a cancelled follower does not cancel the leader (kept)

Exits non-zero on failure.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))

import hub as hubmod  # noqa: E402

ROWS = [{"item_id": f"M{i:03d}", "title": f"Song {i}", "author": f"Artist {i}"}
        for i in range(10)]

# Ordering is by events, not sleeps (review finding 5: B-179/B-257 were this class of
# flake under VM load). `JOINS` counts the callers that are inside `_single_flight`'s
# await: the call and the await are one step, with no yield between them.
JOINS: list = []
# None on a hub that predates the helper (B-241's base), where `joined` only yields.
_ORIG_SINGLE_FLIGHT = getattr(hubmod, "_single_flight", None)


def _counting_single_flight(inflight, key, work):
    JOINS.append(key)
    return _ORIG_SINGLE_FLIGHT(inflight, key, work)


async def until(pred, limit: int = 5000) -> bool:
    for _ in range(limit):
        if pred():
            return True
        await asyncio.sleep(0)
    return pred()


async def joined(n: int) -> None:
    if _ORIG_SINGLE_FLIGHT is None:
        for _ in range(50):
            await asyncio.sleep(0)
        return
    if not await until(lambda: len(JOINS) >= n):
        raise AssertionError(f"only {len(JOINS)} of {n} callers joined")


class SlowSonic:
    """Holds every search until `release` is set; `started` says one is upstream."""

    def __init__(self, fail: bool = False):
        self.calls = 0
        self.fail = fail
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def call(self, route, spec, params, body):
        self.calls += 1
        self.started.set()
        await self.release.wait()
        if self.fail:
            raise RuntimeError("boom")
        return 200, json.dumps({"results": ROWS}).encode(), "application/json"


async def settle(task):
    try:
        return await task
    except BaseException as e:  # noqa: BLE001 — the outcome is what is judged
        return e


async def two_callers(start):
    """Leader A upstream, follower B joined, both waiting."""
    JOINS.clear()
    a = asyncio.ensure_future(start())
    await joined(1)
    b = asyncio.ensure_future(start())
    await joined(2)
    return a, b


async def case_mood_leader_cancelled(failures):
    fake = SlowSonic()
    hubmod.SONIC.call = fake.call
    svc = hubmod.MoodService(None)
    a, b = await two_callers(lambda: svc._fetch("calm piano"))
    await fake.started.wait()
    a.cancel()
    await settle(a)
    fake.release.set()
    rb = await settle(b)
    if not isinstance(rb, list):
        failures.append(f"mood_leader_cancelled: follower got {rb!r}, not the rows")
    if fake.calls != 1:
        failures.append(f"mood_leader_cancelled: {fake.calls} searches, expected 1")
    if "calm piano" not in svc._searches:
        failures.append("mood_leader_cancelled: the search's rows were not stored")


async def case_mood_all_cancelled(failures, unretrieved):
    fake = SlowSonic()
    hubmod.SONIC.call = fake.call
    svc = hubmod.MoodService(None)
    a, b = await two_callers(lambda: svc._fetch("dark synth"))
    await fake.started.wait()
    a.cancel()
    b.cancel()
    await settle(a)
    await settle(b)
    fake.release.set()
    await until(lambda: not svc._inflight)
    if "dark synth" not in svc._searches:
        failures.append("mood_all_cancelled: the search did not land after its callers left")
    if svc._inflight:
        failures.append(f"mood_all_cancelled: still in flight: {list(svc._inflight)}")


async def case_mood_all_cancelled_then_fails(failures, unretrieved):
    # Partial review finding 3: on Python 3.13+ `asyncio.shield` logs an exception of
    # an inner task whose outer was cancelled first ("exception in shielded future"),
    # whatever the hub's done callback retrieves. Every caller gone, then the work
    # raises: nothing may reach the loop's exception handler (3.12 and 3.14 alike).
    fake = SlowSonic(fail=True)
    hubmod.SONIC.call = fake.call
    svc = hubmod.MoodService(None)
    a, b = await two_callers(lambda: svc._fetch("noise"))
    await fake.started.wait()
    a.cancel()
    b.cancel()
    await settle(a)
    await settle(b)
    seen = len(unretrieved)
    fake.release.set()
    await until(lambda: not svc._inflight)
    import gc
    gc.collect()
    for _ in range(5):
        await asyncio.sleep(0)
    if len(unretrieved) != seen:
        failures.append(f"mood_all_cancelled_then_fails: the loop's exception handler "
                        f"fired: {unretrieved[seen:]}")
    if svc._inflight:
        failures.append(f"mood_all_cancelled_then_fails: still in flight: {list(svc._inflight)}")


async def case_mood_failure_shared(failures):
    fake = SlowSonic(fail=True)
    hubmod.SONIC.call = fake.call
    svc = hubmod.MoodService(None)
    a, b = await two_callers(lambda: svc._fetch("loud"))
    fake.release.set()
    ra, rb = await settle(a), await settle(b)
    for name, r in (("leader", ra), ("follower", rb)):
        if not isinstance(r, RuntimeError):
            failures.append(f"mood_failure_shared: {name} got {r!r}, not the search's error")
    if fake.calls != 1:
        failures.append(f"mood_failure_shared: {fake.calls} searches, expected 1")


class SlowUpstream:
    def __init__(self):
        self.calls = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, route, spec, upstream_path, params, body, key):
        self.calls += 1
        self.started.set()
        await self.release.wait()
        return 200, b'{"ok":true}', "application/json"


def proxy_and_route():
    proxy = hubmod.SonicProxy()
    up = SlowUpstream()
    proxy._call_upstream = up
    route = ("GET", "/sonic/clap/stats")
    spec = dict(hubmod.SONIC_ROUTES[route])
    spec.pop("cache", None)           # the in-flight join, not the cache, is under test
    return proxy, up, route, spec


async def case_proxy_leader_cancelled(failures):
    proxy, up, route, spec = proxy_and_route()
    a, b = await two_callers(lambda: proxy.call(route, spec, [], None))
    await up.started.wait()
    a.cancel()
    await settle(a)
    up.release.set()
    rb = await settle(b)
    if not (isinstance(rb, tuple) and rb[0] == 200):
        failures.append(f"proxy_leader_cancelled: follower got {rb!r}, not the answer")
    if up.calls != 1:
        failures.append(f"proxy_leader_cancelled: {up.calls} upstream calls, expected 1")


async def case_proxy_follower_cancelled(failures):
    proxy, up, route, spec = proxy_and_route()
    a, b = await two_callers(lambda: proxy.call(route, spec, [], None))
    await up.started.wait()
    b.cancel()
    await settle(b)
    up.release.set()
    ra = await settle(a)
    if not (isinstance(ra, tuple) and ra[0] == 200):
        failures.append(f"proxy_follower_cancelled: leader got {ra!r}")


async def amain() -> list:
    failures: list = []
    unretrieved: list = []
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda _l, ctx: unretrieved.append(ctx.get("message")))
    orig = hubmod.SONIC.call
    if _ORIG_SINGLE_FLIGHT is not None:
        hubmod._single_flight = _counting_single_flight
    try:
        await case_mood_leader_cancelled(failures)
        await case_mood_all_cancelled(failures, unretrieved)
        await case_mood_all_cancelled_then_fails(failures, unretrieved)
        await case_mood_failure_shared(failures)
        await case_proxy_leader_cancelled(failures)
        await case_proxy_follower_cancelled(failures)
    finally:
        hubmod.SONIC.call = orig
        if _ORIG_SINGLE_FLIGHT is not None:
            hubmod._single_flight = _ORIG_SINGLE_FLIGHT
    import gc
    gc.collect()
    await asyncio.sleep(0)
    if unretrieved:
        failures.append(f"loop exception handler fired: {unretrieved}")
    return failures


def main() -> int:
    failures = asyncio.run(amain())
    if failures:
        for f in failures:
            print("FAIL -", f)
        return 1
    print("PASS - single-flight: mood_leader_cancelled/mood_all_cancelled/"
          "mood_all_cancelled_then_fails/mood_failure_shared/proxy_leader_cancelled/"
          "proxy_follower_cancelled")
    return 0


if __name__ == "__main__":
    sys.exit(main())
