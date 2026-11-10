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

    async def connect(self, url=None):
        self.ws = await websockets.connect(url or URL)
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
    """`_do` is the one place a `do` frame is built, and the one place `shuffle` /
    `order` join load/queueChanged/setShuffle.

    A call site that built them itself could send them to a device without the
    cap (breaking "today's frames byte for byte") or forget them for one with it.
    Fails on: a `{"t": "do"}` literal outside `_do`; a `_do` call whose command is
    not a literal (or a conditional between literals); a `**` splat, `order=` or
    `shuffle=` into one of the three; a `_do` that never names PLAY_ORDER_CAP.
    """
    src = open(HUB_PY, encoding="utf-8").read()
    tree = ast.parse(src)
    do_fn = next((n for n in ast.walk(tree)
                  if isinstance(n, ast.AsyncFunctionDef) and n.name == "_do"), None)
    if do_fn is None:
        failures.append("guard: no _do in hub.py")
        return
    inside = range(do_fn.lineno, do_fn.end_lineno + 1)
    if not any(isinstance(n, ast.Name) and n.id == "PLAY_ORDER_CAP" for n in ast.walk(do_fn)):
        failures.append("guard: _do does not test PLAY_ORDER_CAP (as a name, not a comment)")
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict) and node.lineno not in inside:
            for k, v in zip(node.keys, node.values):
                if (isinstance(k, ast.Constant) and k.value == "t"
                        and isinstance(v, ast.Constant) and v.value == "do"):
                    failures.append(f"guard: hub.py:{node.lineno} builds a do frame outside _do")
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "_do"):
            continue
        arg = node.args[1] if len(node.args) >= 2 else next(
            (k.value for k in node.keywords if k.arg == "cmd"), None)
        if isinstance(arg, ast.Constant):
            cmds = {arg.value}
        elif (isinstance(arg, ast.IfExp) and isinstance(arg.body, ast.Constant)
              and isinstance(arg.orelse, ast.Constant)):
            cmds = {arg.body.value, arg.orelse.value}
        else:
            failures.append(f"guard: hub.py:{node.lineno} calls _do with a non-literal command")
            continue
        kws = {k.arg for k in node.keywords}
        hit = cmds & {"load", "queueChanged", "setShuffle"}
        if not hit:
            continue
        if None in kws:
            failures.append(f"guard: hub.py:{node.lineno} splats ** into _do({sorted(hit)})")
        if hit & {"load", "queueChanged"} and kws & {"order", "shuffle"}:
            failures.append(f"guard: hub.py:{node.lineno} passes {sorted(kws & {'order', 'shuffle'})} "
                            f"to _do({sorted(hit)}) itself; only _do may add them")
        if "setShuffle" in hit and "shuffle" in kws:
            failures.append(f"guard: hub.py:{node.lineno} passes shuffle= to _do('setShuffle')")


def insert_rule(failures):
    """The controller's ruling on where an inserted row goes in play order, on the
    hub's one insert function (in-process): an APPEND (lands at the end of the
    queue) goes to the end of play order; Play next right after the current row;
    an insert into the MIDDLE of the queue right after its queue predecessor's
    play position (at the front when it has none)."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(HUB_PY)))
    import hub  # noqa: E402
    from types import SimpleNamespace

    def run(order, index, at, count, play_next):
        sp = SimpleNamespace(session=hub.Session(queue=[{}] * len(order), index=index,
                                                 order=list(order), shuffle=True))
        hub.UserSpace._order_after_insert(sp, at, count, play_next)
        return sp.session.order

    base = [0, 3, 5, 1, 4, 2]   # 6 rows, playing row 4
    cases = [
        ("append 2", (base, 4, 6, 2, False), [0, 3, 5, 1, 4, 2, 6, 7]),
        ("play next 2", (base, 4, 5, 2, True), [0, 3, 7, 1, 4, 5, 6, 2]),
        ("play next, playing row last in queue", ([0, 5, 1, 2, 3, 4], 5, 6, 1, True),
         [0, 5, 6, 1, 2, 3, 4]),
        ("middle insert at row 2 (after row 1)", (base, 4, 2, 1, False), [0, 4, 6, 1, 2, 5, 3]),
        ("middle insert of 2 at row 4 (after row 3)", (base, 0, 4, 2, False),
         [0, 3, 4, 5, 7, 1, 6, 2]),
        ("middle insert at row 0 (no predecessor)", (base, 4, 0, 1, False), [0, 1, 4, 6, 2, 5, 3]),
    ]
    for name, args, want in cases:
        got = run(*args)
        if got != want:
            failures.append(f"insert rule ({name}): order {got}, expected {want}")


async def review_cases(failures):
    """Review fixes (2026-11-10): no `shuffle:true, order:null` on the wire and no
    queue-order stepping under shuffle; a repeated bad order answered once; removing
    the playing row at the end of play order pauses the device too."""
    # An old state.json: shuffle on, no `order`.
    state = tempfile.NamedTemporaryFile(suffix=".json", delete=False).name
    with open(state, "w") as f:
        json.dump({"session": {"queue": rows("s", 4), "index": 2, "shuffle": True,
                               "positionMs": 0, "repeat": "none"}}, f)
    port, hport = _free_port(), _free_port()
    url = f"ws://127.0.0.1:{port}"
    env = {**_inherited_env(), "HUB_TOKEN": TOKEN, "HUB_PORT": str(port),
           "HUB_MIRROR_PLAYQUEUE": "false", "HUB_STATE": state, "HUB_HOST": "127.0.0.1",
           "HUB_HEALTH_PORT": str(hport)}
    hub = subprocess.Popen([sys.executable, "hub.py"], env=env,
                           cwd=os.path.join(os.path.dirname(__file__), ".."))

    def bad_count():
        with urllib.request.urlopen(f"http://127.0.0.1:{hport}/", timeout=3) as r:
            return json.loads(r.read()).get("badOrders")
    try:
        await asyncio.sleep(1.2)
        d = Client("Rev-D", CAP)
        c = Client("Rev-C", ["controller"])
        await d.connect(url)
        await c.connect(url)
        wo = c.session.get("order")
        if not is_perm(wo, 4) or wo[0] != 2:
            failures.append(f"old state: welcome.session.order {wo!r} under shuffle; "
                            f"expected a current-first permutation of 4")
        await d.act(action="play")
        ld = d.last("load")
        lo = (ld or {}).get("order")
        if not ld or ld.get("shuffle") is not True or not is_perm(lo, 4) or lo[0] != 2:
            failures.append(f"old state: takeover load shuffle={(ld or {}).get('shuffle')!r} "
                            f"order={lo!r}; expected True and a current-first permutation")
        await c.act(action="next")
        if is_perm(lo, 4) and c.session.get("index") != lo[1]:
            failures.append(f"old state: next from row 2 -> {c.session.get('index')!r}, "
                            f"expected {lo[1]} (the order sent), not queue order")

        # clear, then a controller enqueue: shuffle stays on, so the order must exist.
        await c.act(action="clear")
        d.clear()
        await c.act(action="enqueue", tracks=rows("k", 5), at="end")
        qc = d.last("queueChanged")
        qo = (qc or {}).get("order")
        if not qc or qc.get("shuffle") is not True or not is_perm(qo, 5) \
                or qo[0] != qc.get("index"):
            failures.append(f"clear+enqueue: D's queueChanged shuffle={(qc or {}).get('shuffle')!r} "
                            f"order={qo!r} index={(qc or {}).get('index')!r}; expected True and a "
                            f"current-first permutation of 5")
        if c.session.get("order") != qo:
            failures.append(f"clear+enqueue: session.order {c.session.get('order')!r} != sent {qo!r}")
        if is_perm(qo, 5):
            await c.act(action="next")
            if c.session.get("index") != qo[1]:
                failures.append(f"clear+enqueue: next -> {c.session.get('index')!r}, expected {qo[1]}")

        # The same bad publish, three times: answered once, counted three times.
        q = rows("x", 6)
        bad = [0, 0, 1, 2, 3, 4]
        before = bad_count() or 0
        d.clear()
        await d.act(action="setQueue", tracks=q, index=3, play=True, shuffle=True, order=bad)
        first = c.session.get("order")
        await d.act(action="setQueue", tracks=q, index=3, play=True, shuffle=True, order=bad)
        await d.act(action="setQueue", tracks=q, index=3, play=True, shuffle=True, order=bad)
        n = sum(1 for x in d.dos if x.get("cmd") == "setShuffle")
        if n != 1:
            failures.append(f"repeated bad order: {n} setShuffle frames for 3 identical "
                            f"publishes; expected 1")
        if c.session.get("order") != first:
            failures.append(f"repeated bad order: session.order moved {first!r} -> "
                            f"{c.session.get('order')!r} without telling the device")
        if bad_count() != before + 3:
            failures.append(f"repeated bad order: badOrders {before} -> {bad_count()!r}, expected +3")
        # A valid publish in between, then the same bad pair: answered again.
        await d.act(action="setQueue", tracks=q, index=3, play=True, shuffle=True,
                    order=[3, 0, 1, 2, 4, 5])
        await d.act(action="setQueue", tracks=q, index=3, play=True, shuffle=True, order=bad)
        n = sum(1 for x in d.dos if x.get("cmd") == "setShuffle")
        if n != 2:
            failures.append(f"repeated bad order: after a valid publish the bad pair got "
                            f"{n - 1} more answers; expected 1")

        # Removing the playing row when it is LAST in play order (no repeat).
        await d.act(action="setQueue", tracks=rows("z", 4), index=2, play=True, shuffle=True,
                    order=[0, 1, 3, 2])
        d.clear()
        await c.act(action="remove", index=2)
        cmds = [x.get("cmd") for x in d.dos]
        if "pause" not in cmds or "jump" not in cmds or cmds.index("pause") < cmds.index("jump"):
            failures.append(f"remove last of play order: D got {cmds}; expected queueChanged, "
                            f"jump, then pause")
        if c.session.get("isPlaying") is not False:
            failures.append(f"remove last of play order: session isPlaying "
                            f"{c.session.get('isPlaying')!r}")
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

    source_guard(failures)
    insert_rule(failures)
    await review_cases(failures)
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
