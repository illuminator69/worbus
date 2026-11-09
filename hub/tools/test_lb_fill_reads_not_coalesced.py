#!/usr/bin/env python3
"""
A fill/gap progress read sent after an action's answer must reach lb-bot itself.

Feishin's B-233 watch counts a `/lb/fills` or `/lb/gap` read only when it was
SENT after the action's (fetch, auto, cancel) answer arrived. Both routes are
uncached, but `HttpProxy.call` lets an identical GET (same params, same caller
class) JOIN a call already in flight — typically the client's own 5 s poll,
sent before the tap and still upstream. The read sent after the answer then
returns the body lb-bot computed BEFORE the action: "sent after" is not
"computed after", and the watch settles on the pre-action state. The listening
routes opted out for the same reason (`coalesce: False`).

A stub lb-bot holds the first request open; a second identical read is issued
while it is in flight. Each must be its own upstream hit, and the second must
carry the answer computed after it was sent. Exits non-zero on failure.
"""
from __future__ import annotations

import asyncio
import http.server
import json
import os
import sys
import threading

os.environ.setdefault("HUB_TOKEN", "test-token")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import hub  # noqa: E402

HITS: list[str] = []
HOLD_FIRST = threading.Event()
LOCK = threading.Lock()


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        with LOCK:
            HITS.append(self.path)
            rev = len(HITS)
        if rev == 1:
            HOLD_FIRST.wait(5)      # the poll sent before the action, still upstream
        raw = json.dumps({"rev": rev, "albums": {}, "gaps": {}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *_args):
        pass


async def _two_reads(proxy, path: str, headers: dict) -> tuple[dict, dict]:
    first = asyncio.ensure_future(proxy.handle(None, path, headers, "GET"))
    for _ in range(200):                     # until the first is upstream
        if HITS:
            break
        await asyncio.sleep(0.01)
    second = asyncio.ensure_future(proxy.handle(None, path, headers, "GET"))
    await asyncio.sleep(0.3)                 # let the second be sent (or join)
    HOLD_FIRST.set()
    (s1, _h1, b1), (s2, _h2, b2) = await asyncio.gather(first, second)
    return {"http": s1, **json.loads(b1)}, {"http": s2, **json.loads(b2)}


def main() -> int:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    original_url = hub.LBBOT_URL
    hub.LBBOT_URL = f"http://127.0.0.1:{port}"
    headers = {"Authorization": f"Bearer {hub.TOKEN}"}
    failures: list[str] = []
    try:
        for path in ("/lb/fills?group_ids=g1&release_mbids=r1", "/lb/gap?group_id=g1"):
            HITS.clear()
            HOLD_FIRST.clear()
            proxy = hub.LbProxy()
            a, b = asyncio.run(_two_reads(proxy, path, headers))
            route = path.split("?")[0]
            if len(HITS) != 2:
                failures.append(
                    f"{route}: a read sent while an identical one was in flight joined it "
                    f"(upstream hits={len(HITS)}), so it got lb-bot's answer from before it was sent")
            if b.get("rev") != 2:
                failures.append(f"{route}: the later read answered rev={b.get('rev')} (want 2): {b}")
            if a.get("http") != 200 or b.get("http") != 200:
                failures.append(f"{route}: statuses {a.get('http')}, {b.get('http')}")
    finally:
        hub.LBBOT_URL = original_url
        HOLD_FIRST.set()
        server.shutdown()

    for f in failures:
        print(f"FAIL - {f}")
    if failures:
        return 1
    print("PASS - lb fills/gap reads: a read sent during an identical one reaches lb-bot itself")
    return 0


if __name__ == "__main__":
    sys.exit(main())
