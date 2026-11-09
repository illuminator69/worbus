#!/usr/bin/env python3
"""
`GET /lb/album/sources?refresh=1` must reach lb-bot (B-256).

lb-bot's `/api/album/sources` keeps the list it found and answers from it until
`refresh=1` asks it to search again. The hub route listed `rgid, release_mbid,
artist, album, total` and `_filtered_params` drops every other param BEFORE
`PROXY_NO_CACHE_PARAMS` is looked at, so a client's "Search again" never left
the hub: the 60 s proxy cache answered it, and lb-bot was never asked.

Serves a stub lb-bot whose answer changes on every hit and drives the real
`LbProxy.handle`, so it tells "the refresh reached lb-bot" from "the hub
answered it from its cache":

  - a plain read is cached (one upstream hit for two reads);
  - `refresh=1` goes upstream, forwards `refresh`, and returns the new answer;
  - the next plain read sees the refreshed answer, from cache.

Exits non-zero on failure.
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


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        if self.path.split("?")[0] != "/api/album/sources":
            self.send_response(404)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{}")
            return
        HITS.append(self.path)
        # One answer per search, told apart by its revision.
        raw = json.dumps({"sources": [{"id": len(HITS), "peer": f"p{len(HITS)}"}],
                          "rev": len(HITS), "cached": False}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *_args):
        pass


def main() -> int:
    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    original_url = hub.LBBOT_URL
    hub.LBBOT_URL = f"http://127.0.0.1:{port}"
    proxy = hub.LbProxy()
    headers = {"Authorization": f"Bearer {hub.TOKEN}"}
    failures: list[str] = []

    def call(path: str):
        return asyncio.run(proxy.handle(None, path, headers, "GET"))

    try:
        base = "/lb/album/sources?rgid=rg1&release_mbid=rel1&artist=A&album=B&total=9"
        status, _h, first = call(base)
        if status != 200 or json.loads(first).get("rev") != 1:
            failures.append(f"the first read should reach lb-bot: {status} {first!r}")
        call(base)
        if len(HITS) != 1:
            failures.append(f"a plain repeat should be the cached answer, hits={HITS}")

        status, _h, again = call(base + "&refresh=1")
        if len(HITS) != 2:
            failures.append(
                "`refresh=1` was answered from the hub cache and never reached "
                f"lb-bot (hits={HITS}): Search again returns the list it replaces")
        elif "refresh=1" not in HITS[-1]:
            failures.append(
                f"`refresh` was not forwarded to lb-bot: {HITS[-1]} — lb-bot "
                "answers from its kept list without it")
        if status != 200 or json.loads(again).get("rev") != 2:
            failures.append(f"the refresh did not return the new search: {again!r}")

        before = len(HITS)
        _s, _h, after = call(base)
        if len(HITS) != before:
            failures.append("the plain read after a refresh went upstream")
        if json.loads(after).get("rev") != 2:
            failures.append(
                "a plain read after the refresh still sees the old list: the "
                "refresh got its own cache slot instead of replacing the shared one")
    finally:
        hub.LBBOT_URL = original_url
        server.shutdown()

    if failures:
        for f in failures:
            print(f"FAIL - {f}")
        return 1
    print("PASS - lb album sources: refresh reaches lb-bot, then the plain read "
          "sees the new list")
    return 0


if __name__ == "__main__":
    sys.exit(main())
