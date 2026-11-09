#!/usr/bin/env python3
"""
W1 / B-105: under shuffle the PLAYING device owns play order (decision D1,
navi-connect/DESIGN-w1-play-order-2026-11-10.md).

A device that advertises the cap `playOrder` publishes `shuffle` + `order` (a
permutation of its queue's rows, in play order) with every `act:setQueue`, and
the hub steps THAT order for `act:next/previous` instead of one of its own. Every
`load`, `queueChanged` and `setShuffle` the hub sends such a device carries
`shuffle` + `order`, which it adopts verbatim. A device without the cap (a 0.7.0
client) keeps today's frames byte for byte and today's hub-made order.

Spins up a real hub subprocess; asserts on `session` broadcasts and on the `do`
directives each fake device receives. Exits non-zero on failure.
"""
from __future__ import annotations

import ast
import asyncio
import json
import os
import socket
import subprocess
import sys
import tempfile
import urllib.request

import websockets


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


PORT = _free_port()
HEALTH_PORT = _free_port()
TOKEN = "test-token"
URL = f"ws://127.0.0.1:{PORT}"
HUB_PY = os.path.join(os.path.dirname(__file__), "..", "hub.py")

CAP = ["receiver", "controller", "playOrder"]
OLD = ["receiver", "controller"]          # a 0.7.0 player: no playOrder


def rows(prefix, n):
    return [{"id": f"{prefix}{i}", "title": f"{prefix.upper()}{i}", "durationMs": 180000}
            for i in range(n)]


class Client:
    def __init__(self, name, caps):
        self.name = name
        self.device_id = name.lower()
        self.caps = list(caps)
        self.ws = None
        self.welcome = None
        self.session = {}
        self.dos = []
        self.index = 0

    async def connect(self):
        self.ws = await websockets.connect(URL)
        await self.ws.send(json.dumps({
            "t": "hello", "token": TOKEN,
            "device": {"id": self.device_id, "name": self.name,
                       "platform": "test", "caps": self.caps},
        }))
        asyncio.create_task(self._loop())
        await asyncio.sleep(0.25)

    async def _loop(self):
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                t = msg.get("t")
                if t == "welcome":
                    self.welcome = msg
                    self.session = msg.get("session", {})
                elif t == "session":
                    self.session = msg
                elif t == "do":
                    self.dos.append(msg)
                    cmd = msg.get("cmd")
                    if cmd in ("load", "jump", "queueChanged"):
                        self.index = msg.get("index", self.index)
                    elif cmd == "release":
                        await self.ws.send(json.dumps({
                            "t": "report", "index": self.index, "positionMs": 1000,
                            "isPlaying": False}))
                        await self.ws.send(json.dumps({
                            "t": "released", "transferId": msg.get("transferId")}))
        except websockets.ConnectionClosed:
            pass

    async def act(self, **kw):
        if kw.get("action") == "setQueue":
            self.index = kw.get("index", 0)   # it plays what it publishes
        await self.ws.send(json.dumps({"t": "act", **kw}))
        await asyncio.sleep(0.25)

    async def report(self, **kw):
        self.index = kw.get("index", self.index)
        await self.ws.send(json.dumps({"t": "report", **kw}))
        await asyncio.sleep(0.25)

    def last(self, cmd):
        return next((d for d in reversed(self.dos) if d.get("cmd") == cmd), None)

    def clear(self):
        self.dos.clear()


def health() -> dict:
    with urllib.request.urlopen(f"http://127.0.0.1:{HEALTH_PORT}/", timeout=3) as r:
        return json.loads(r.read())


def is_perm(order, n):
    return isinstance(order, list) and sorted(order) == list(range(n))


def source_guard(failures):
    """`_do` is the one place `shuffle`/`order` join load/queueChanged/setShuffle.

    A call site that built them itself could send them to a device without the
    cap (breaking "today's frames byte for byte") or forget them for one with it.
    """
    tree = ast.parse(open(HUB_PY, encoding="utf-8").read())
    do_src = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_do":
            do_src = ast.get_source_segment(open(HUB_PY, encoding="utf-8").read(), node)
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "_do" and len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)):
            continue
        cmd = node.args[1].value
        kws = {k.arg for k in node.keywords}
        if cmd in ("load", "queueChanged") and kws & {"order", "shuffle"}:
            failures.append(f"guard: hub.py:{node.lineno} passes {sorted(kws & {'order', 'shuffle'})} "
                            f"to _do('{cmd}') itself; only _do may add them")
        if cmd == "setShuffle" and "shuffle" in kws:
            failures.append(f"guard: hub.py:{node.lineno} passes shuffle= to _do('setShuffle')")
    if do_src is None or "PLAY_ORDER_CAP" not in do_src:
        failures.append("guard: _do does not add shuffle/order for the playOrder cap "
                        "(no PLAY_ORDER_CAP in its body)")


async def main():
    state = tempfile.NamedTemporaryFile(suffix=".json", delete=False).name
    env = {**_inherited_env(), "HUB_TOKEN": TOKEN, "HUB_PORT": str(PORT),
           "HUB_MIRROR_PLAYQUEUE": "false", "HUB_STATE": state, "HUB_HOST": "127.0.0.1",
           "HUB_HEALTH_PORT": str(HEALTH_PORT)}
    hub = subprocess.Popen([sys.executable, "hub.py"], env=env,
                           cwd=os.path.join(os.path.dirname(__file__), ".."))
    failures: list[str] = []
    try:
        await asyncio.sleep(1.2)
        d = Client("Dev-D", CAP)
        e = Client("Dev-E", CAP)
        c = Client("Ctl-C", ["controller"])
        o = Client("Old-O", OLD)
        for x in (d, e, c, o):
            await x.connect()

        feats = (d.welcome or {}).get("features") or []
        if "playOrder" not in feats:
            failures.append(f"welcome.features lacks \"playOrder\": {feats!r}")

        # --- B-105: the hub steps the order the playing device published ------
        # The session is already shuffled (as after any earlier shuffle act), so
        # today's hub answers D's publish with an order of its own.
        await c.act(action="shuffle", on=True)
        q = rows("a", 6)
        pub = [0, 3, 5, 1, 4, 2]          # order[0] != index: never a hub-made order
        await d.act(action="setQueue", tracks=q, index=3, play=True,
                    shuffle=True, order=pub)
        if c.session.get("order") != pub or c.session.get("shuffle") is not True:
            failures.append(f"B-105: session after D's setQueue is shuffle="
                            f"{c.session.get('shuffle')!r} order={c.session.get('order')!r}, "
                            f"expected shuffle=True order={pub}")
        if d.last("load"):
            failures.append("B-105: D was echoed a load for its own setQueue")
        # Two auto-advances on D, in ITS order: 3 -> 5 -> 1.
        await d.report(index=5, positionMs=0, isPlaying=True)
        await d.report(index=1, positionMs=0, isPlaying=True)
        d.clear()
        await c.act(action="next")
        j = d.last("jump")
        if not j or j.get("index") != 4:
            failures.append(f"B-105: controller next after D played 3,5,1 sent D "
                            f"{j!r}; expected jump index 4 (D's published order)")
        await c.act(action="previous")
        j = d.last("jump")
        if not j or j.get("index") != 1:
            failures.append(f"B-105: previous from row 4 sent D {j!r}; expected jump 1")
        await c.act(action="next")  # back on row 4

        # --- rule 2: Play next lands right after current, Add at the end --------
        # queue a0..a5, order [0,3,5,1,4,2], current row 4.
        d.clear()
        await c.act(action="enqueue", tracks=rows("n", 2), at="next")
        qc = d.last("queueChanged")
        want = [0, 3, 7, 1, 4, 5, 6, 2]
        if not qc or qc.get("order") != want or qc.get("shuffle") is not True:
            failures.append(f"rule 2 (play next): D's queueChanged order="
                            f"{(qc or {}).get('order')!r} shuffle={(qc or {}).get('shuffle')!r}; "
                            f"expected {want}, True")
        d.clear()
        await c.act(action="enqueue", tracks=rows("e", 2), at="end")
        qc = d.last("queueChanged")
        want = [0, 3, 7, 1, 4, 5, 6, 2, 8, 9]
        if not qc or qc.get("order") != want:
            failures.append(f"rule 2 (add to queue): D's queueChanged order="
                            f"{(qc or {}).get('order')!r}; expected {want}")
        if c.session.get("order") != want:
            failures.append(f"rule 2: session.order {c.session.get('order')!r} != {want}")

        # --- rule 2: removing the playing row plays its PLAY-ORDER successor ----
        # queue [a0,a1,a2,a3,a4,n0,n1,a5,e0,e1]; play row 3 (a3): its play-order
        # successor is row 7 (a5), its queue successor row 4 (a4).
        await c.act(action="jump", index=3)
        d.clear()
        await c.act(action="remove", index=3)
        tracks = c.session.get("queue") or []
        qc, j = d.last("queueChanged"), d.last("jump")
        want = [0, 6, 1, 3, 4, 5, 2, 7, 8]
        if not j or j.get("index") is None or j["index"] >= len(tracks) \
                or tracks[j["index"]].get("id") != "a5":
            got = tracks[j["index"]].get("id") if j and j.get("index") is not None \
                and j["index"] < len(tracks) else None
            failures.append(f"rule 2 (remove playing): D jumped to {j!r} ({got!r}); "
                            f"expected a5, the play-order successor (row 6)")
        if not qc or qc.get("order") != want or qc.get("index") != 6:
            failures.append(f"rule 2 (remove playing): queueChanged index="
                            f"{(qc or {}).get('index')!r} order={(qc or {}).get('order')!r}; "
                            f"expected 6, {want}")

        # --- the playing device's own shuffle-off reaches the hub ----------------
        q9 = c.session.get("queue") or []
        await d.act(action="setQueue", tracks=q9, index=6, play=True,
                    shuffle=False, order=None)
        if c.session.get("shuffle") is not False or c.session.get("order") is not None:
            failures.append(f"D's setQueue shuffle=False left the session at shuffle="
                            f"{c.session.get('shuffle')!r} order={c.session.get('order')!r}")

        # --- an invalid published order: counted, and the hub's sent back --------
        before = health().get("badOrders")
        d.clear()
        await d.act(action="setQueue", tracks=q9, index=6, play=True,
                    shuffle=True, order=[0, 0, 1, 2, 3, 4, 5, 6, 7])
        ss = d.last("setShuffle")
        so = c.session.get("order")
        if not is_perm(so, 9) or so[0] != 6:
            failures.append(f"bad order: session.order {so!r}; expected a current-first "
                            f"permutation of 9")
        if not ss or ss.get("on") is not True or ss.get("order") != so:
            failures.append(f"bad order: D was not sent its replacement as setShuffle: {ss!r}")
        after = health().get("badOrders")
        if not isinstance(after, int) or after != (before or 0) + 1:
            failures.append(f"bad order: /health badOrders {before!r} -> {after!r}, "
                            f"expected +1")

        # --- transfer carries shuffle + the published order ----------------------
        qb = rows("b", 6)
        pub = [5, 2, 0, 4, 1, 3]
        await d.act(action="setQueue", tracks=qb, index=2, play=True,
                    shuffle=True, order=pub)
        await c.act(action="transfer", target="dev-e", play=True)
        await asyncio.sleep(0.4)
        ld = e.last("load")
        if not ld or ld.get("shuffle") is not True or ld.get("order") != pub \
                or ld.get("index") != 2:
            failures.append(f"transfer: E's load shuffle={(ld or {}).get('shuffle')!r} "
                            f"order={(ld or {}).get('order')!r} index={(ld or {}).get('index')!r}; "
                            f"expected True, {pub}, 2")

        # --- a controller's new queue: the hub proposes, the device adopts --------
        e.clear()
        await c.act(action="setQueue", tracks=rows("c", 4), index=1, play=True,
                    shuffle=False, order=[3, 2, 1, 0])   # a controller's keys: ignored
        ld = e.last("load")
        prop = (ld or {}).get("order")
        if not ld or ld.get("shuffle") is not True or not is_perm(prop, 4) or prop[0] != 1:
            failures.append(f"controller setQueue: E's load shuffle={(ld or {}).get('shuffle')!r} "
                            f"order={prop!r}; expected True and a start-row-first proposal")
        if c.session.get("order") != prop:
            failures.append(f"controller setQueue: session.order {c.session.get('order')!r} "
                            f"!= the proposal {prop!r}")

        # --- orphaned session: the hub keeps and steps the order; takeover gets it -
        await e.ws.close()
        await asyncio.sleep(0.4)
        if c.session.get("activeDeviceId") is not None:
            failures.append("orphan: E's disconnect left an active device")
        await c.act(action="next")
        if prop and c.session.get("index") != prop[1]:
            failures.append(f"orphan: next stepped to {c.session.get('index')!r}, "
                            f"expected {prop[1]} (custody order)")
        d.clear()
        await d.act(action="play")
        ld = d.last("load")
        if not ld or ld.get("shuffle") is not True or ld.get("order") != prop:
            failures.append(f"takeover: D's load shuffle={(ld or {}).get('shuffle')!r} "
                            f"order={(ld or {}).get('order')!r}; expected True, {prop}")

        # --- back-compat: a device without the cap gets today's frames -----------
        await c.act(action="transfer", target="old-o", play=True)
        await asyncio.sleep(0.4)
        ld = o.last("load")
        if not ld or set(ld) != {"t", "cmd", "transferId", "timeoutMs", "tracks",
                                 "index", "positionMs", "play"}:
            failures.append(f"back-compat: O's transfer load keys {sorted(ld or {})}")
        await c.act(action="shuffle", on=True)
        # O publishes with the new keys (as if it had them): ignored, hub-made order.
        await o.act(action="setQueue", tracks=rows("o", 6), index=3, play=True,
                    shuffle=False, order=[0, 1, 2, 3, 4, 5])
        so = c.session.get("order")
        if c.session.get("shuffle") is not True or not is_perm(so, 6) or so[0] != 3:
            failures.append(f"back-compat: O's setQueue gave shuffle={c.session.get('shuffle')!r} "
                            f"order={so!r}; expected today's (shuffle kept, current-first)")
        o.clear()
        await c.act(action="enqueue", tracks=rows("p", 1), at="next")
        qc = o.last("queueChanged")
        if not qc or set(qc) != {"t", "cmd", "tracks", "index"}:
            failures.append(f"back-compat: O's queueChanged keys {sorted(qc or {})}")
        await c.act(action="shuffle", on=True)
        ss = o.last("setShuffle")
        if not ss or set(ss) != {"t", "cmd", "on", "order"}:
            failures.append(f"back-compat: O's setShuffle keys {sorted(ss or {})}")
        # Remove the playing row: today's behaviour is its QUEUE successor.
        cur = c.session.get("index")
        queue_now = c.session.get("queue") or []
        succ_id = queue_now[cur + 1]["id"] if cur is not None and cur + 1 < len(queue_now) else None
        o.clear()
        await c.act(action="remove", index=cur)
        j = o.last("jump")
        queue_now = c.session.get("queue") or []
        got = queue_now[j["index"]]["id"] if j and j.get("index") is not None \
            and j["index"] < len(queue_now) else None
        if got != succ_id:
            failures.append(f"back-compat: removing O's playing row jumped to {got!r}, "
                            f"expected the queue successor {succ_id!r}")
        o.clear()
        await c.act(action="setQueue", tracks=rows("r", 3), index=0, play=True)
        ld = o.last("load")
        if not ld or set(ld) != {"t", "cmd", "tracks", "index", "positionMs", "play"}:
            failures.append(f"back-compat: O's load keys {sorted(ld or {})}")

        source_guard(failures)
    finally:
        hub.terminate()
        try:
            hub.wait(timeout=5)
        except subprocess.TimeoutExpired:
            hub.kill()
        for p in (state, state + ".bak"):
            try:
                os.unlink(p)
            except FileNotFoundError:
                pass

    if failures:
        print("FAIL:")
        for f in failures:
            print("  -", f)
        sys.exit(1)
    print("PASS - play order: the hub steps the playing device's published order "
          "(next/previous, remove, enqueue, transfer, orphan takeover); controllers' "
          "keys ignored; bad orders counted and answered; 0.7.0 frames unchanged")


def _inherited_env() -> dict:
    """os.environ without PREVIEW_* plus the service account P2 needs at start-up
    (see test_transfer.py); nothing here calls Navidrome."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("PREVIEW_")}
    env.setdefault("NAVIDROME_URL", "http://127.0.0.1:9")
    env.setdefault("HUB_ND_USER", "owner")
    env.setdefault("HUB_ND_PASS", "test-pass")
    return env


if __name__ == "__main__":
    asyncio.run(main())
