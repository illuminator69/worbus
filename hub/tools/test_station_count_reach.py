#!/usr/bin/env python3
"""
`/sonic/station/count`'s `reach` is the pool at the top of the ladder the station
itself draws with (B-178, PROTOCOL §18.1/§18.3).

The count used to re-derive that top from `ceiling_max` (and, on Tier 1, from
`min(ceiling_max, STATION_TIER1_MAX)`) beside `StationEngine._levels`, the ladder a
station actually widens through: two copies of one rule, equal today only because
nobody has changed either. The forcing case is the change itself: with `_levels`
made to stop lower (as a later tuning would), `reach` must follow it.

In process. AudioMuse's nearest lists and Navidrome's similar songs are stubbed on
the engine; the count's own code runs.

Cases:
  reach_is_ladder_top     balanced / close / explore on Tier 2, and close on Tier 1:
                          `reach` = the pool at `_levels`' top ceiling
  reach_follows_levels    `_levels` capped at 600: balanced's reach is 600, not 1000

Exits non-zero on failure.
"""
from __future__ import annotations

import asyncio
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))

import hub as hubmod  # noqa: E402

SEED = "S0001"


class StubHub:
    def __init__(self):
        self.session = hubmod.Session()
        self.moods = None


def make_engine(tier1: bool):
    hub = StubHub()
    eng = hubmod.StationEngine(hub)
    eng.tier2_ok = not tier1

    async def nearest(item_id, depth, b):
        return [{"id": f"N{k:05d}"} for k in range(depth)]

    async def tier1_list(item_id, n, b):
        n = max(1, min(n, hubmod.STATION_TIER1_MAX))
        return [{"id": f"T{k:05d}"} for k in range(n)]

    eng._nearest = nearest
    eng._tier1_list = tier1_list
    return eng


def ladder_top(eng, character: str, tier1: bool) -> int:
    st = hubmod.Station(id="st_probe", seed={"kind": "track", "ids": [SEED]},
                        name="probe", character=character, created_at=0, used_at=0)
    return max(c for _r, c, _d in eng._levels(st.tune(), tier1, bool(st.mood)))


async def count(eng, character: str):
    return await eng.count({"seed": {"kind": "track", "ids": [SEED]},
                            "character": character})


async def amain(failures):
    hubmod.STATION_COUNT_WIDEN_BELOW = 10 ** 6   # always ask for reach
    for character, tier1 in (("balanced", False), ("close", False),
                             ("explore", False), ("close", True)):
        hubmod.AUDIOMUSE_URL = "" if tier1 else "http://audiomuse.invalid"
        eng = make_engine(tier1)
        status, body = await count(eng, character)
        top = ladder_top(eng, character, tier1)
        if status != 200 or body.get("reach") != top:
            failures.append(f"reach_is_ladder_top {character} tier1={tier1}: {status} "
                            f"reach {body.get('reach')}, the ladder's top is {top}")

    hubmod.AUDIOMUSE_URL = "http://audiomuse.invalid"
    orig = hubmod.StationEngine._levels

    def capped(self, *a, **k):
        return [(r, min(c, 600), d) for r, c, d in orig(self, *a, **k)]

    hubmod.StationEngine._levels = capped
    try:
        eng = make_engine(False)
        status, body = await count(eng, "balanced")
        if status != 200 or body.get("reach") != 600:
            failures.append(f"reach_follows_levels: {status} reach {body.get('reach')}, "
                            "the station's ladder stops at 600")
        if body.get("count") != 400:
            failures.append(f"reach_follows_levels: count {body.get('count')}, expected 400")
    finally:
        hubmod.StationEngine._levels = orig


def main() -> int:
    failures: list[str] = []
    asyncio.run(amain(failures))
    if failures:
        print("FAIL (station count reach):")
        for f in failures:
            print("  -", f)
        return 1
    print("PASS - station count reach: reach-is-ladder-top/reach-follows-levels")
    return 0


if __name__ == "__main__":
    sys.exit(main())
