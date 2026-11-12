#!/usr/bin/env python3
"""
A route's 404 fallback shares the route's timeout; it does not get a second one.

`_fetch_with` retries a 404 once on `spec["fallback"]` (an upstream older than the
route). The retry used to get the route's whole timeout again, so one call could
hold its pool slot, and since B-241 a GET whose every caller left runs to its end,
for twice the route timeout (partial review 5). The fallback now gets what is left of
the route's timeout (at least FALLBACK_MIN_TIMEOUT), so a call is bounded by the
route timeout plus that floor.

In process: `_proxy_upstream_blocking` is a stub that records the timeout each call
was given and answers the route 404 after a delay.

Cases:
  fallback_gets_the_rest   primary 404 after 0.6 s on a 1.0 s route: the fallback is
                           given at most the 0.4 s left (plus the floor), not 1.0 s
  fast_404_keeps_budget    a primary 404 at once leaves the fallback ~the whole timeout
  probe_override_kept      the probe's shorter first-call timeout (LB_PROBE_TIMEOUT)
                           does not shorten the fallback below the route's remainder

Exits non-zero on failure.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

os.environ.setdefault("HUB_TOKEN", "test-token")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import hub  # noqa: E402

CALLS: list[tuple[str, float]] = []
PRIMARY_DELAY = [0.0]


def stub(method, url, payload, token, timeout, label):
    CALLS.append((url, timeout))
    if "/fallback" not in url:
        time.sleep(PRIMARY_DELAY[0])
        return 404, b'{"error":"not found"}', "application/json"
    return 200, b'{"ok":true}', "application/json"


async def fetch(spec, timeout=None):
    CALLS.clear()
    proxy = hub.SonicProxy()
    return await proxy._fetch(spec, spec["path"], [], None, timeout=timeout)


async def run() -> list[str]:
    f: list[str] = []
    floor = getattr(hub, "FALLBACK_MIN_TIMEOUT", 0.0)
    spec = {"method": "GET", "path": "/primary", "fallback": "/fallback", "timeout": 1.0}

    PRIMARY_DELAY[0] = 0.6
    st, _b, _c = await fetch(spec)
    if st != 200 or len(CALLS) != 2:
        f.append(f"fallback_gets_the_rest: {st}, calls {CALLS}")
    else:
        given = CALLS[1][1]
        if given > max(0.45, floor) + 1e-6:
            f.append(f"fallback_gets_the_rest: the fallback was given {given:.2f} s after "
                     f"0.6 s of a 1.0 s route (a second whole timeout)")

    PRIMARY_DELAY[0] = 0.0
    await fetch(spec)
    if len(CALLS) == 2 and CALLS[1][1] < 0.9:
        f.append(f"fast_404_keeps_budget: an immediate 404 left the fallback {CALLS[1][1]:.2f} s")

    await fetch(spec, timeout=0.2)
    if len(CALLS) == 2 and (CALLS[0][1] != 0.2 or CALLS[1][1] < 0.9):
        f.append(f"probe_override_kept: first {CALLS[0][1]}, fallback {CALLS[1][1]:.2f} s")
    return f


def main() -> int:
    original = hub._proxy_upstream_blocking
    original_up = type(hub.SONIC).upstream
    hub._proxy_upstream_blocking = stub
    type(hub.SONIC).upstream = property(lambda self: "http://am.invalid")
    try:
        failures = asyncio.run(run())
    finally:
        hub._proxy_upstream_blocking = original
        type(hub.SONIC).upstream = original_up
    if failures:
        print("FAIL (proxy fallback deadline):")
        for x in failures:
            print("  -", x)
        return 1
    print("PASS - proxy fallback deadline: fallback-gets-the-rest/fast-404-keeps-budget/"
          "probe-override-kept")
    return 0


if __name__ == "__main__":
    sys.exit(main())
