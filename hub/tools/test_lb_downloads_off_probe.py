#!/usr/bin/env python3
"""
A relayed 409 `downloads_off` makes the hub look again and tell that space (B-298).

PROTOCOL §15.7: a client that still believes downloads are "on" reads lb-bot's first
409 `downloads_off` as the off state. The hub broadcast `t:"lb"` only on a change it
saw at its 60 s probe, so an lb-bot restarted off and then on again between two probes
was no change to it, and the client stayed "off" until its next welcome. The contract
(RESULTS-phase-b-desktop-2026-11-10, B-298): when the hub relays a 409 `downloads_off`,
it probes lb-bot at once and sends `t:"lb"` with the result to that space's devices,
whether or not its own view changed.

A real hub subprocess (tools/test_signin.HubProc, the fake Navidrome) and a fake lb-bot
that proves identity (R17) and whose health and download answers the cases set. Users:
owner (admin, primary space) and alice (admin, her own space).

Cases:
  unchanged_view_told   health "on", the download route answers 409 downloads_off (lb-bot
                        was off for that call and is on again): one health read follows,
                        and owner's device hears `t:"lb"` with downloads "on"; alice's,
                        in another space, hears nothing
  off_seen              health "off", a 409 downloads_off: the probe sees the change and
                        owner hears downloads "off"
  other_409_ignored     a 409 with another code: no health read, no frame

Exits non-zero on failure.
"""
from __future__ import annotations

import asyncio
import http.server
import json
import os
import socketserver
import sys
import threading
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from fake_navidrome import FakeNavidrome  # noqa: E402
from test_signin import TOKEN, USERS, HubProc, Sock  # noqa: E402

DOWNLOADS_OFF = {"code": "downloads_off", "error": "Downloading is not set up on this server."}
LB = {"downloads": "on", "download": (409, DOWNLOADS_OFF), "health_hits": 0,
      "lock": threading.Lock()}


class FakeLb:
    def __init__(self) -> None:
        class H(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_a):  # noqa: D102
                pass

            def _send(self, status: int, body: dict) -> None:
                raw = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("X-Worbus-Identity", "1")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self):  # noqa: N802
                path = self.path.partition("?")[0]
                if path == "/api/health":
                    with LB["lock"]:
                        LB["health_hits"] += 1
                        mode = LB["downloads"]
                    return self._send(200, {"ok": True, "identity": 1, "downloads": mode})
                return self._send(404, {"error": "not found"})

            def do_POST(self):  # noqa: N802
                n = int(self.headers.get("Content-Length") or 0)
                self.rfile.read(n)
                if self.path.partition("?")[0] == "/api/album/download":
                    status, body = LB["download"]
                    return self._send(status, body)
                return self._send(404, {"error": "not found"})

        class S(socketserver.ThreadingMixIn, http.server.HTTPServer):
            daemon_threads = True

        self.server = S(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def lb_frames(s: Sock, since: int) -> list[dict]:
    return [f for f in s.frames[since:] if f.get("t") == "lb"]


async def until(pred, timeout: float = 3.0) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        await asyncio.sleep(0.05)
    return pred()


async def connect(h: HubProc, user: str, name: str) -> Sock:
    st, p = await h.signin(user)
    if st != 200:
        raise RuntimeError(f"{user}'s sign-in answered {st} {p}")
    return await Sock().hello(h.ws_url, {"id": p["deviceId"], "name": name},
                              secret=p["deviceSecret"])


def download(h: HubProc) -> tuple[int, dict]:
    return h.http("POST", "/lb/album/download", {"rgid": "rg1", "release_mbid": "m1"}, TOKEN)


async def run(h: HubProc) -> list[str]:
    f: list[str] = []
    owner = await connect(h, "owner", "Owner D")
    alice = await connect(h, "alice", "Alice D")
    if not await until(lambda: (owner.first("welcome") or {}).get("lb", {}).get("downloads") == "on"):
        f.append(f"setup: owner's welcome.lb is not downloads on: {owner.first('welcome')}")
        return f

    # unchanged_view_told
    o0, a0 = len(owner.frames), len(alice.frames)
    with LB["lock"]:
        hits0 = LB["health_hits"]
    st, p = await asyncio.to_thread(download, h)
    if (st, p) != (409, DOWNLOADS_OFF):
        f.append(f"unchanged_view_told: the 409 was not relayed as is: {st} {p}")
    told = await until(lambda: any(fr.get("downloads") == "on" for fr in lb_frames(owner, o0)))
    if not told:
        f.append(f"unchanged_view_told: owner heard no `lb` frame saying downloads on: "
                 f"{owner.frames[o0:]}")
    with LB["lock"]:
        hits = LB["health_hits"] - hits0
    if hits < 1:
        f.append("unchanged_view_told: no health read followed the 409")
    await asyncio.sleep(0.3)
    if lb_frames(alice, a0):
        f.append(f"unchanged_view_told: alice (another space) heard {lb_frames(alice, a0)}")

    # off_seen
    with LB["lock"]:
        LB["downloads"] = "off"
    o0 = len(owner.frames)
    st, p = await asyncio.to_thread(download, h)
    if st != 409:
        f.append(f"off_seen: {st} {p}")
    if not await until(lambda: any(fr.get("downloads") == "off" for fr in lb_frames(owner, o0))):
        f.append(f"off_seen: owner heard no `lb` frame saying downloads off: {owner.frames[o0:]}")

    # other_409_ignored
    LB["download"] = (409, {"code": "busy", "error": "busy"})
    await asyncio.sleep(0.3)
    o0 = len(owner.frames)
    with LB["lock"]:
        hits0 = LB["health_hits"]
    st, _p = await asyncio.to_thread(download, h)
    await asyncio.sleep(0.6)
    with LB["lock"]:
        hits = LB["health_hits"] - hits0
    if st != 409 or hits or lb_frames(owner, o0):
        f.append(f"other_409_ignored: {st}, {hits} health reads, frames {lb_frames(owner, o0)}")
    for s in (owner, alice):
        await s.ws.close()
    return f


async def main_async() -> list[str]:
    nd = FakeNavidrome(USERS).start()
    lb = FakeLb()
    h = HubProc(nd.url, LBBOT_URL=lb.url)
    try:
        await h.up()
        try:
            return await run(h)
        except Exception:  # noqa: BLE001
            return [f"raised:\n{traceback.format_exc()}"]
    finally:
        h.stop()
        lb.stop()
        nd.stop()


def main() -> int:
    failures = asyncio.run(main_async())
    if failures:
        print("FAIL (lb downloads_off probe):")
        for x in failures:
            print("  -", x)
        return 1
    print("PASS - lb downloads_off probe: unchanged-view-told-to-the-space/off-seen/"
          "other-409-ignored")
    return 0


if __name__ == "__main__":
    sys.exit(main())
