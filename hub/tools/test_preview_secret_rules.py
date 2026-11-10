#!/usr/bin/env python3
"""
The preview secret is refused by one rule on both sides of it (B-312).

The hub refused an example PREVIEW_SECRET (`_is_example_secret`, the `change-me`
prefix that `.env.example` ships, quoted or not) while the sidecar refused only an
empty one, so a sidecar started from `preview/.env.example` as shipped ran with a
publicly known key: anyone could sign a `/stream` capability, which makes it an open
relay to the extractor. The two programs ship in separate images (preview/'s build
context is its own folder), so the rule is written in each, and this file is what
keeps them one rule: every value is judged the same way by both, or it fails.

Nothing compared PREVIEW_SECRET with HUB_TOKEN either. Only the hub holds both; it
now refuses to start when they are the same (`.env.example`: "must NOT be HUB_TOKEN").

Cases:
  parity            preview.secret_refusal(v) is set iff the hub refuses v as
                    PREVIEW_SECRET, over empty, blank, example (any case, quoted)
                    and real values
  sidecar_refuses   `python preview/preview.py` with the shipped example secret exits
                    non-zero at once, naming PREVIEW_SECRET and never the value
  not_hub_token     the hub refuses PREVIEW_SECRET == HUB_TOKEN with PREVIEW_URL set,
                    naming both settings and neither value; a different secret passes

Exits non-zero on failure.
"""
from __future__ import annotations

import importlib.util
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
HUB_DIR = os.path.dirname(HERE)
PREVIEW = os.path.join(os.path.dirname(HUB_DIR), "preview", "preview.py")
sys.path.insert(0, HUB_DIR)
sys.path.insert(0, HERE)

import hub as hubmod  # noqa: E402
from test_edits import _free_port  # noqa: E402

TOKEN = "0123456789abcdef0123456789abcdef"
EXAMPLE = "change-me-to-a-long-random-secret-that-is-not-the-hub-token"
VALUES = ["", "   ", "change-me", EXAMPLE, "CHANGE-ME-x", f"'{EXAMPLE}'", '"change-me"',
          "  change-me-x  ", "fedcba9876543210fedcba9876543210", "changeme-not-the-prefix"]


def hub_refuses(secret: str) -> bool:
    return hubmod.startup_config_refusal(TOKEN, "0.0.0.0", False,
                                         "http://preview:4792", secret) is not None


def load_preview():
    spec = importlib.util.spec_from_file_location("preview_under_test", PREVIEW)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def case_parity(failures):
    preview = load_preview()
    rule = getattr(preview, "secret_refusal", None)
    if rule is None:
        failures.append("parity: preview.py has no secret_refusal; its start-up check "
                        "is a rule of its own")
        return
    for v in VALUES:
        if bool(rule(v)) != hub_refuses(v):
            failures.append(f"parity: {v!r}: sidecar refuses={bool(rule(v))}, "
                            f"hub refuses={hub_refuses(v)}")


def case_sidecar_refuses(failures):
    env = {k: v for k, v in os.environ.items() if not k.startswith("PREVIEW_")}
    env.update(PREVIEW_SECRET=EXAMPLE, PREVIEW_HOST="127.0.0.1",
               PREVIEW_PORT=str(_free_port()))
    try:
        proc = subprocess.run([sys.executable, PREVIEW], env=env, capture_output=True,
                              text=True, timeout=8)
    except subprocess.TimeoutExpired:
        failures.append("sidecar_refuses: still running after 8 s with the example secret")
        return
    out = proc.stdout + proc.stderr
    if proc.returncode == 0:
        failures.append("sidecar_refuses: exit 0 with the example secret")
    if "PREVIEW_SECRET" not in out:
        failures.append(f"sidecar_refuses: the refusal does not name PREVIEW_SECRET: {out!r}")
    if EXAMPLE in out:
        failures.append("sidecar_refuses: the refusal echoes the value")


def case_not_hub_token(failures):
    r = hubmod.startup_config_refusal
    got = r(TOKEN, "0.0.0.0", False, "http://preview:4792", TOKEN)
    if got is None:
        failures.append("not_hub_token: PREVIEW_SECRET == HUB_TOKEN was accepted")
    elif "PREVIEW_SECRET" not in got or "HUB_TOKEN" not in got or TOKEN in got:
        failures.append(f"not_hub_token: the refusal should name both settings and "
                        f"no value: {got!r}")
    if r(TOKEN, "0.0.0.0", False, "http://preview:4792", f" {TOKEN} ") is None:
        failures.append("not_hub_token: the same value with spaces was accepted")
    if r(TOKEN, "0.0.0.0", False, "http://preview:4792",
         "fedcba9876543210fedcba9876543210") is not None:
        failures.append("not_hub_token: a different secret was refused")
    if r(TOKEN, "0.0.0.0", False, "", TOKEN) is not None:
        failures.append("not_hub_token: refused with no PREVIEW_URL (no preview proxy)")


def main() -> int:
    failures: list[str] = []
    case_parity(failures)
    case_sidecar_refuses(failures)
    case_not_hub_token(failures)
    if failures:
        print("FAIL (preview secret rules):")
        for f in failures:
            print("  -", f)
        return 1
    print("PASS - preview secret rules: parity/sidecar-refuses-example/not-hub-token")
    return 0


if __name__ == "__main__":
    sys.exit(main())
