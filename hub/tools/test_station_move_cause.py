#!/usr/bin/env python3
"""
A report says why the index moved, and only the user's move can be a skip (B-316).

PROTOCOL §5.3 `moveCause` / §18.3. A report that moves the index said only "the index
moved, at position P", so the hub guessed intent from P: a player that could not play
a file and moved on at 0:00 (Worbus desktop's error-skip calls its natural-end path)
read exactly like a user pressing next at 0:00, and the hub cooled that artist on the
bound station. A device now names the cause on that report: `user`, `end` (it played
to the end, or its decoder said so) or `error`. `end` and `error` are never a skip;
whether the track was played through is still read from the position. A report
without the key (0.7.0 clients) or with a value the hub does not know is judged as
before.

Real WebSocket clients against a real hub subprocess (HUB_DEBUG), the fake AudioMuse
and Navidrome of test_station_engine, a station bound by setQueue.

Cases (each a report pair: the track's last position, then the report that moves on):
  error_at_0          0 s, moveCause error        -> neither (no steer)
  end_early           3 s, moveCause end          -> neither (a truncated file)
  end_at_the_end      199.5 s, moveCause end      -> played (steers)
  user_at_10s         10 s, moveCause user        -> skip (steers)
  absent_at_10s       10 s, no key                -> skip (0.7.0, unchanged)
  unknown_at_10s      10 s, moveCause "bogus"     -> skip (judged as absent)
  error_at_90         180 s, moveCause error      -> played (position decides)
  history_once        B-316(b): a setQueue that keeps a 60 %-played song, then that
                      song's `ended`: one play-history row, not two

Exits non-zero on failure.
"""
from __future__ import annotations

import asyncio
import glob
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, HERE)

from test_edits import _free_port  # noqa: E402
from test_station_engine import (LIB, AudioMuse, Navidrome, acall, serve,  # noqa: E402
                                 spawn, stop, wait_up)
from test_station_signals import SClient, judgements, settle  # noqa: E402

DUR = 200_000

CASES = [  # (name, last position, moveCause or None for absent, expected verdict)
    ("error_at_0", 0, "error", "neither"),
    ("end_early", 3_000, "end", "neither"),
    ("end_at_the_end", 199_500, "end", "played"),
    ("user_at_10s", 10_000, "user", "skip"),
    ("absent_at_10s", 10_000, None, "skip"),
    ("unknown_at_10s", 10_000, "bogus", "skip"),
    ("error_at_90", 180_000, "error", "played"),
]


async def suite(failures, log_path, state):
    am_srv, am_url = serve(AudioMuse)
    nd_srv, nd_url = serve(Navidrome)
    port = _free_port()
    url = f"ws://127.0.0.1:{port}"
    proc = spawn(port, state, am_url, nd_url, extra={"HUB_DEBUG": "1"}, log_path=log_path)
    try:
        await asyncio.to_thread(wait_up, port)
        r = SClient("Recv", device_id="recv")
        c = SClient("Ctl", device_id="ctl", caps=["controller"])
        await r.connect(url)
        await c.connect(url)
        seed = "L12_0"
        st_, a = await acall(port, "POST", "/sonic/station/start", {
            "seed": {"kind": "track", "ids": [seed], "name": "Indie Song 12-0"},
            "character": "balanced", "count": 14, "deviceId": "recv"})
        if st_ != 200:
            failures.append(f"start: {st_} {a}")
            return None
        sid = a["stationId"]
        queue = [{"id": seed, "title": LIB[seed]["title"], "artist": LIB[seed]["artist"],
                  "durationMs": DUR}] + [
            {"id": t["id"], "title": t["title"], "artist": t["artist"], "durationMs": DUR}
            for t in a["tracks"]]
        await r.act(action="setQueue", tracks=queue, index=0, play=True,
                    savedQueueId="q_mc", stationId=sid)
        await settle()
        if c.session.get("stationId") != sid:
            failures.append("setQueue did not bind the station")
            return None
        # Past INTENT_GRACE of the setQueue's play intent: reports alone from here.
        await asyncio.sleep(2.3)
        for i, (name, pos, cause, want) in enumerate(CASES):
            rev = c.rev(sid)
            await r.report(index=i, positionMs=pos, isPlaying=True)
            await settle(0.2)
            move = {"index": i + 1, "positionMs": 0, "isPlaying": True}
            if cause is not None:
                move["moveCause"] = cause
            await r.report(**move)
            await settle()
            steered = c.rev(sid) != rev
            if steered != (want != "neither"):
                failures.append(f"{name}: steered={steered}, expected verdict {want}")
            if c.session.get("index") != i + 1:
                failures.append(f"{name}: the report did not move the session: "
                                f"{c.session.get('index')}")
        got = [(i, v) for i, _j, v, steer in judgements(log_path) if steer]
        want = [(i, w) for i, (_n, _p, _c, w) in enumerate(CASES)]
        if got[:len(want)] != want:
            failures.append(f"the hub's judgements {got[:len(want)]}, expected {want}")

        # history_once (B-316(b)): the song at k, 60 % in, kept by a setQueue, then ended.
        k = len(CASES)
        kept = queue[k]["id"]
        await r.report(index=k, positionMs=120_000, isPlaying=True)
        await settle()
        await r.act(action="setQueue", tracks=queue, index=k, positionMs=120_000,
                    play=True, savedQueueId="q_mc2", stationId=sid)
        await settle()
        await asyncio.sleep(2.3)
        await r.report(index=k, positionMs=199_000, isPlaying=False, ended=True)
        await settle()
        # Both judgements happened, so one row means they were folded into one play.
        both = [(steer, v) for i, _j, v, steer in judgements(log_path) if i == k]
        if (False, "neither") not in both or (True, "played") not in both:
            failures.append(f"history_once: the kept song was not judged by the setQueue "
                            f"and by `ended`: {both}")
        await r.ws.close()
        await c.ws.close()
        return kept
    finally:
        stop(proc)          # SIGTERM: the hub writes its side files on a graceful stop
        am_srv.shutdown()
        nd_srv.shutdown()


def history_rows(state: str, track_id: str) -> int:
    rows = 0
    for path in glob.glob(state + "*.history"):
        with open(path, encoding="utf-8") as f:
            plays = json.load(f).get("plays") or []
        rows += sum(1 for p in plays if p and p[0] == track_id)
    return rows


async def main():
    failures: list[str] = []
    tmp = tempfile.mkdtemp()
    state = os.path.join(tmp, "state.json")
    log_path = os.path.join(tmp, "hub.log")
    kept = await suite(failures, log_path, state)
    if kept is not None:
        n = history_rows(state, kept)
        if n != 1:
            failures.append(f"history_once: {n} play-history rows for the kept song, expected 1")
    if failures:
        print("FAIL (station move cause):")
        for f in failures:
            print("  -", f)
        sys.exit(1)
    print("PASS - station move cause: error-at-0-neither/end-early-neither/end-at-end-played/"
          "user-skip/absent-skip/unknown-as-absent/error-at-90-played/history-once")


if __name__ == "__main__":
    asyncio.run(main())
