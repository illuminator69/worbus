#!/usr/bin/env python3
"""
The hub sends the Navidrome service account to AudioMuse in a POST body, never in a URL.

The hub counterpart of B-359 (Worbus for Android's direct route). AudioMuse's
fingerprint (`/api/sonic_fingerprint/generate`) reads the listening history of the
account it is given. The hub put `navidrome_user` / `navidrome_password` (HUB_ND_USER /
HUB_ND_PASS, the service account) in that GET's query string, where AudioMuse's access
log, and any proxy's, keep it. AudioMuse 3.7.1 reads the same fields from a JSON body on
POST (`app_sonic_fingerprint.py`, methods GET and POST), so the hub now POSTs
`{"n", "navidrome_user", "navidrome_password"}`. The client-facing route is unchanged:
`GET /sonic/fingerprint?n=`.

A real hub subprocess (tools/test_signin.HubProc, the fake Navidrome) and a fake
AudioMuse that records every request line and body.

Cases:
  route_posts_body     `GET /sonic/fingerprint?n=7` reaches AudioMuse as a POST whose JSON
                       body is {n: 7, navidrome_user, navidrome_password}
  station_posts_body   Your mix's fingerprint call (the station engine) does the same
  no_password_in_url   no request line AudioMuse saw names `navidrome_` or carries the
                       password, and the hub's log does not either
  post_refused_named   fix round 2 (review minor 2): an AudioMuse that refuses the POST
                       (Flask's 405 HTML, or a 400/415 that is not JSON) answers the client
                       503 {"code": "audiomuse_too_old", "need": "3.7.1", "error"}, not the
                       relayed HTML; a JSON 400 of AudioMuse's own is still relayed as is.
                       Never a GET retry (that would put the account back in a URL)
  bad_n_400            `n` that is not 1-6 ASCII digits (`²`, `abc`, `-1`, 7 digits) is the
                       hub's 400 `bad_request`, with no AudioMuse call (was a 502)

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
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from fake_navidrome import FakeNavidrome  # noqa: E402
from test_signin import PASSWORDS, TOKEN, USERS, HubProc  # noqa: E402

FP = "/api/sonic_fingerprint/generate"
SEEN: list[tuple[str, str, object]] = []    # (method, request target, parsed body)
LOCK = threading.Lock()
# What a POST to the fingerprint answers instead of rows: (status, content type, body).
FP_REFUSE: list = [None]
FLASK_405 = (b'<!doctype html>\n<html lang=en>\n<title>405 Method Not Allowed</title>\n'
             b'<h1>Method Not Allowed</h1>\n<p>The method is not allowed for the requested '
             b'URL.</p>\n')


class FakeAudioMuse:
    def __init__(self) -> None:
        class H(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_a):  # noqa: D102
                pass

            def _answer(self, body) -> None:
                with LOCK:
                    SEEN.append((self.command, self.path, body))
                refuse = FP_REFUSE[0]
                if (refuse is not None and self.command == "POST"
                        and self.path.partition("?")[0] == FP):
                    status, ctype, raw = refuse
                    self.send_response(status)
                    self.send_header("Content-Type", ctype)
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                    return
                raw = b"[]"
                if self.path.partition("?")[0] == "/api/clap/stats":
                    raw = b'{"clap_enabled": true, "loaded": true, "song_count": 1}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self):  # noqa: N802
                self._answer(None)

            def do_POST(self):  # noqa: N802
                n = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(n)
                try:
                    body = json.loads(raw or b"null")
                except ValueError:
                    body = raw
                self._answer(body)

        class S(socketserver.ThreadingMixIn, http.server.HTTPServer):
            daemon_threads = True

        self.server = S(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def fp_calls(since: int) -> list[tuple[str, str, object]]:
    with LOCK:
        return [c for c in SEEN[since:] if c[1].partition("?")[0] == FP]


async def run(h: HubProc) -> list[str]:
    f: list[str] = []
    want = {"navidrome_user": "owner", "navidrome_password": PASSWORDS["owner"]}

    with LOCK:
        n0 = len(SEEN)
    st, p = await asyncio.to_thread(h.http, "GET", "/sonic/fingerprint?n=7", None, TOKEN)
    calls = fp_calls(n0)
    if st != 200 or len(calls) != 1:
        f.append(f"route_posts_body: {st} {p}, AudioMuse saw {calls}")
    else:
        method, _target, body = calls[0]
        if method != "POST" or body != {"n": 7, **want}:
            f.append(f"route_posts_body: AudioMuse saw {method} with body "
                     f"{ {k: ('***' if 'password' in k else v) for k, v in body.items()} if isinstance(body, dict) else body!r}")

    with LOCK:
        n0 = len(SEEN)
    st, p = await asyncio.to_thread(h.http, "POST", "/sonic/station/start",
                                    {"seed": {"kind": "yourMix"}, "count": 3}, TOKEN)
    calls = fp_calls(n0)
    if not calls:
        f.append(f"station_posts_body: no fingerprint call reached AudioMuse ({st} {p})")
    for method, _target, body in calls:
        ok = (method == "POST" and isinstance(body, dict)
              and {k: body.get(k) for k in want} == want and isinstance(body.get("n"), int))
        if not ok:
            f.append(f"station_posts_body: AudioMuse saw {method}, body keys "
                     f"{sorted(body) if isinstance(body, dict) else body!r}")

    with LOCK:
        targets = [t for _m, t, _b in SEEN]
    for t in targets:
        if "navidrome_" in t or PASSWORDS["owner"] in t:
            f.append(f"no_password_in_url: a request line carries the account: "
                     f"{t.partition('?')[0]}?…")
            break
    # post_refused_named
    for n, refuse in enumerate(((405, "text/html; charset=utf-8", FLASK_405),
                                (400, "text/html; charset=utf-8", b"<p>Bad Request</p>"),
                                (415, "text/html; charset=utf-8", b"<p>Unsupported</p>"))):
        FP_REFUSE[0] = refuse
        with LOCK:
            n0 = len(SEEN)
        st, p = await asyncio.to_thread(h.http, "GET", f"/sonic/fingerprint?n={30 + n}",
                                        None, TOKEN)
        if (st, p.get("code"), p.get("need")) != (503, "audiomuse_too_old", "3.7.1") \
                or not isinstance(p.get("error"), str):
            f.append(f"post_refused_named: AudioMuse's {refuse[0]} came back as {st} {p}")
        with LOCK:
            methods = [m for m, t, _b in SEEN[n0:] if t.partition("?")[0] == FP]
        if methods != ["POST"]:
            f.append(f"post_refused_named: AudioMuse was asked {methods} (a GET retry would "
                     f"put the account in a URL)")
    FP_REFUSE[0] = (400, "application/json", b'{"error": "n is out of range"}')
    st, p = await asyncio.to_thread(h.http, "GET", "/sonic/fingerprint?n=40", None, TOKEN)
    if (st, p) != (400, {"error": "n is out of range"}):
        f.append(f"post_refused_named: AudioMuse's own JSON 400 was not relayed: {st} {p}")
    FP_REFUSE[0] = None

    # bad_n_400
    for bad in ("%C2%B2", "abc", "-1", "1234567", "%EF%BC%93"):
        with LOCK:
            n0 = len(SEEN)
        st, p = await asyncio.to_thread(h.http, "GET", f"/sonic/fingerprint?n={bad}", None, TOKEN)
        if st != 400 or p.get("code") != "bad_request":
            f.append(f"bad_n_400: n={bad} answered {st} {p}")
        if fp_calls(n0):
            f.append(f"bad_n_400: n={bad} reached AudioMuse")

    h.logf.flush()
    with open(h.log_path, encoding="utf-8", errors="replace") as lf:
        logged = lf.read()
    if PASSWORDS["owner"] in logged or "navidrome_password" in logged:
        f.append("no_password_in_url: the hub's log holds the service account's password")
    return f


async def main_async() -> list[str]:
    nd = FakeNavidrome(USERS).start()
    am = FakeAudioMuse()
    h = HubProc(nd.url, AUDIOMUSE_URL=am.url, AUDIOMUSE_TOKEN="am-tok")
    try:
        await h.up()
        try:
            return await run(h)
        except Exception:  # noqa: BLE001
            return [f"raised:\n{traceback.format_exc()}"]
    finally:
        h.stop()
        am.stop()
        nd.stop()


def main() -> int:
    failures = asyncio.run(main_async())
    if failures:
        print("FAIL (AudioMuse account in a POST body):")
        for x in failures:
            print("  -", x)
        return 1
    print("PASS - audiomuse account in body: route-posts-body/station-posts-body/"
          "no-password-in-url/post-refused-named/bad-n-400")
    return 0


if __name__ == "__main__":
    sys.exit(main())
