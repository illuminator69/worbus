#!/usr/bin/env python3
"""
Guard: no host path and no peer path leaves `/lb/fills` or `/lb/gap`, for any
caller (B-097, Q-017(c), Q-043).

Not a list of forbidden keys. A fake lb-bot answers its two routes in the shapes
lb-bot's own view builders write today (umbrella-era lb-bot `41b75c8`:
`_gap_detail_view` for `/api/gaps/<id>`, and `_fills_view` = that view minus
`sources`, `_strip_place_paths`, and `_without_peer_names` for a non-admin), and
every field those builders fill with a path carries a planted marker:
`tracks[].placeFolder` (an lb-bot host path), `tracks[].placeFile` and
`tracks[].manualPick.filename` (a peer's raw slskd path), `sources[].files[].filename`
(a peer's listing), `sources[].folder` (the peer's folder path). The guard walks
everything the hub hands the caller and fails on any string carrying a marker,
wherever it sits, so a new route shape, a renamed key or a dropped hub rule all
show up without this file knowing their names.

The one exception is `sources[].folder` on `/lb/gap`: Worbus for Android draws its
last segment as a source's label (GapBox.kt), so dropping it is a client change.
It is reported, not failed (decision needed: lb-bot sends `folderName`, the last
segment, and the hub or lb-bot drops `folder`).

In process, against a real HTTP fake (the hub's own `_proxy_upstream_blocking`,
identity headers and proof), as the primary admin and as a signed-in non-admin.

Exits non-zero on failure.
"""
from __future__ import annotations

import asyncio
import http.server
import json
import os
import sys
import threading
import urllib.parse

os.environ.setdefault("HUB_TOKEN", "test-token")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import hub  # noqa: E402

MARK = "PLANTED-PATH"
HOST = f"/downloads/{MARK} Artist - Album"
PEER = f"@@peer\\Music\\{MARK}\\Artist\\Album"
PEER_NAME_KEYS = {"source", "pickedSource", "peer", "username", "source_user"}


def _without_peer_names(value):
    # lb-bot's own rule for a non-admin (listenbrainz_bot.py `_without_peer_names`).
    if isinstance(value, dict):
        return {k: _without_peer_names(v) for k, v in value.items()
                if k not in PEER_NAME_KEYS}
    if isinstance(value, list):
        return [_without_peer_names(v) for v in value]
    return value


def track(i: int, picked: bool) -> dict:
    # `_gap_detail_view`'s track row, every key it writes.
    return {
        "index": i, "recordingMbid": f"rec-{i}", "disc": 1, "position": i + 1,
        "title": f"Track {i}", "artist": "Artist", "state": "downloaded",
        "downloadError": "", "canForcePlace": False, "forcePlaceKind": "",
        "placementNote": "", "forcePlaceConflict": "",
        "manualPick": ({"peer": "alice", "filename": f"{PEER}\\0{i} Track.flac"}
                       if picked else None),
        "placeFolder": HOST,
        "placeFile": f"0{i} {MARK}.flac",
    }


def gap_view() -> dict:
    # `_album_view` + `_gap_detail_view`'s update, with one source (`_source_view`).
    return {
        "id": "g1", "albumId": "al1", "artist": "Artist", "artistId": "ar1",
        "artistMbid": "", "album": "Album", "present": 5, "total": 7, "extra": 0,
        "missingCount": 2, "status": "downloading", "sourceCount": 1,
        "coverUrl": "/api/cover/al1?size=160", "updatedAt": 1.0, "releaseMbid": "r1",
        "stalledPlacement": False, "loose": True,
        "tracks": [track(0, True), track(1, False)],
        "sources": [{
            "id": 0, "peer": "alice", "folder": PEER, "format": "FLAC",
            "bitrate": "", "size": "300 MB", "fileCount": 2, "speedMbps": 1,
            "queueLength": 0, "freeSlot": True, "coverage": "full",
            "coverageDetail": {"haveTracks": 2, "totalTracks": 2, "unmatched": []},
            "files": [{"filename": f"{PEER}\\01 Track.flac", "track": 0}],
            "filesTruncated": False, "missingTracks": [], "flags": [],
            "recommendation": "", "albumMatch": 100, "albumMatchOk": True,
            "yearInPath": False, "artistVerified": True, "artistEvidence": "tags",
            "searchPass": 1, "score": 9, "rank": 1, "recommended": True}],
        "sourcesTotal": 1, "sourcesPage": 0, "sourcesPages": 1, "sourceQuery": "q",
        "sourcesFoundAt": 0, "sourceTask": {"status": "done"}, "failReason": "",
        "failDetail": "", "allowMp3": False, "canonicalMbid": "r1",
        "noFileInSourceCount": 0, "noSourceReason": "", "mp3WouldHelp": False,
    }


def fills_view() -> dict:
    # `_fills_view`: the summary is the gap view minus `sources`, with
    # `_strip_place_paths` applied to its tracks, plus three counts.
    summary = gap_view()
    summary.pop("sources")
    summary["tracks"] = [{k: v for k, v in t.items()
                          if k not in ("placeFolder", "placeFile")}
                         for t in summary["tracks"]]
    summary.update(done=0, failed=0, taskStatus="done")
    album = {"releaseMbid": "r2", "rgid": "rg2", "state": "downloading",
             "artist": "A", "album": "B", "source": "bob", "pickedSource": "",
             "reason": "", "groupId": "", "taskId": "t2", "ndAlbumIds": []}
    return {"albums": {"r2": album}, "gaps": {"g1": summary}, "serverTime": 1.0}


class FakeLb(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        path = urllib.parse.urlsplit(self.path).path
        admin = self.headers.get("X-Worbus-Admin") == "1"
        if path == "/api/fills":
            body = fills_view()
            if not admin:
                body = _without_peer_names(body)
        elif path == "/api/gaps/g1":
            body = gap_view()          # lb-bot applies no peer-name rule here today
        else:
            self.send_response(404)
            self.end_headers()
            return
        raw = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("X-Worbus-Identity", "1")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *_a):
        pass


def leaks(value, at="") -> list[str]:
    if isinstance(value, dict):
        return [p for k, v in value.items() for p in leaks(v, f"{at}.{k}")]
    if isinstance(value, list):
        return [p for i, v in enumerate(value) for p in leaks(v, f"{at}[{i}]")]
    return [at] if isinstance(value, str) and MARK in value else []


KNOWN = {("/lb/gap", ".sources[0].folder")}   # decision needed, see the docstring

CALLERS = {
    "admin": hub.Caller(user="owner", admin=True, primary=True),
    "user": hub.Caller(user="guest", admin=False, primary=False),
}
ROUTES = {
    "/lb/fills": [("group_ids", "g1"), ("release_mbids", "r2")],
    "/lb/gap": [("group_id", "g1")],
}


async def run(failures: list, known_seen: list) -> None:
    for who, caller in CALLERS.items():
        for path, params in ROUTES.items():
            route = ("GET", path)
            token = hub._CALLER.set(caller)
            try:
                status, body, _ct = await hub.LB.call(route, hub.LB_ROUTES[route],
                                                      list(params), None)
            finally:
                hub._CALLER.reset(token)
            if status != 200:
                failures.append(f"{who} {path}: status {status} {body[:200]!r}")
                continue
            payload = json.loads(body)
            if path == "/lb/fills" and "g1" not in (payload.get("gaps") or {}):
                failures.append(f"{who} {path}: the gap is not in the answer: {payload}")
            for p in leaks(payload):
                if (path, p) in KNOWN:
                    known_seen.append(f"{who} {path}{p}")
                else:
                    failures.append(f"{who} {path}: a path leaves at {p}")


def main() -> int:
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FakeLb)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    hub.LBBOT_URL = f"http://127.0.0.1:{srv.server_address[1]}"
    failures: list[str] = []
    known: list[str] = []
    try:
        asyncio.run(run(failures, known))
    finally:
        srv.shutdown()
    if failures:
        print("FAIL (no paths leave /lb/fills, /lb/gap):")
        for f in failures:
            print("  -", f)
        return 1
    for k in known:
        print(f"KNOWN (decision needed, B-097): a peer folder path leaves at {k}")
    print("PASS - lb no paths leave: fills+gap x admin+user, planted host and peer paths")
    return 0


if __name__ == "__main__":
    sys.exit(main())
