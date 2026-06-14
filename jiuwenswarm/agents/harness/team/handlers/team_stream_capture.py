#!/usr/bin/env python3
"""Live per-event capture of a running team, plus a turn-grouping pass.

Two pieces, by design:

1.  ``TeamStreamCapture`` — a drop-in ``TeamStreamLogger`` subclass. Pass it
    as ``stream_logger=`` to ``Runner.run_agent_team_streaming`` and it writes
    one JSON line per stream chunk to ``stream-{session}.jsonl`` — the flat,
    uninterpreted SOURCE OF TRUTH. It also keeps the base class's human text
    dump (free, harmless). It never raises into the run.

2.  ``group_turns`` — an OFFLINE pass over that JSONL. It splits the global
    stream by member (members run concurrently, so their chunks interleave),
    then by LLM call within each member (each ``llm_usage`` chunk closes a
    call), and emits per-member "turns" bundling the reasoning text, the tool
    calls (with file contents), the tool results, and that call's exact token
    usage. Grouping is a convenience derived from the flat log — if a heuristic
    is ever wrong, the flat log is still authoritative.

Why two pieces: token usage is per-LLM-call, never per-action, and members
interleave on the wire. Keeping the raw stream separate from the grouping
means the recorded data can't be wrong; only the (re-derivable) grouping can.

File-content policy: full content is stored inline when under MAX_INLINE_CHARS;
larger payloads are clipped to a preview plus the true length and a sha1 of the
full text, so a big file read is identifiable without bloating the log. Set
MAX_INLINE_CHARS huge for "always full", or small for "previews only".

CLI:  python team_stream_capture.py group  stream-<session>.jsonl
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Optional

try:
    # Available in the jiuwenswarm runtime; only needed for live capture.
    from openjiuwen.agent_teams.monitor import TeamStreamLogger
except Exception:  # offline grouping doesn't need the SDK
    TeamStreamLogger = object  # type: ignore

MAX_INLINE_CHARS = 20_000  # store full content under this; clip + hash above it

# Chunk type strings, mirroring openjiuwen.agent_teams.monitor.stream_logger.
T_REASONING = "llm_reasoning"
T_OUTPUT = "llm_output"
T_ANSWER = "answer"
T_TOOL_CALL = "tool_call"
T_TOOL_RESULT = "tool_result"
T_TOOL_UPDATE = "tool_update"
T_MESSAGE = "message"
T_USAGE = "llm_usage"


def _clip(text: str) -> dict[str, Any]:
    """Return a content holder: full inline if small, else preview+len+sha1."""
    if text is None:
        return {"text": None}
    if len(text) <= MAX_INLINE_CHARS:
        return {"text": text, "len": len(text)}
    return {
        "text": text[:MAX_INLINE_CHARS],
        "clipped": True,
        "len": len(text),
        "sha1": hashlib.sha1(text.encode("utf-8", "replace")).hexdigest(),
    }


def _as_text(payload: Any) -> str:
    if isinstance(payload, dict):
        return payload.get("content", "") or payload.get("output", "") or ""
    if isinstance(payload, str):
        return payload
    return str(payload)


def _member_of(chunk: Any) -> tuple[Optional[str], Optional[str]]:
    """Return (member, role_str). Leader chunks carry no source_member."""
    src = getattr(chunk, "source_member", None)
    role = getattr(chunk, "role", None)
    role_str = getattr(role, "value", role)
    role_str = str(role_str).lower() if role_str is not None else None
    if src:
        return str(src), role_str
    # Leader emits with source_member=None; label it from the role.
    if role_str and "lead" in role_str:
        return "team-leader", role_str
    return None, role_str


def _record_for(chunk: Any, seq: int) -> dict[str, Any]:
    """Normalise one raw OutputSchema chunk into a flat JSON record."""
    ctype = getattr(chunk, "type", None)
    payload = getattr(chunk, "payload", None)
    member, role = _member_of(chunk)
    rec: dict[str, Any] = {
        "seq": seq,
        "ts": round(time.time(), 6),
        "member": member,
        "role": role,
        "type": ctype,
    }
    if ctype in (T_REASONING, T_OUTPUT, T_ANSWER):
        rec["data"] = _clip(_as_text(payload))
    elif ctype == T_TOOL_CALL and isinstance(payload, dict):
        rec["data"] = {
            "tool_name": payload.get("tool_name", ""),
            "tool_args": _clip(str(payload.get("tool_args", ""))),
        }
    elif ctype == T_TOOL_RESULT and isinstance(payload, dict):
        rec["data"] = {
            "tool_name": payload.get("tool_name", ""),
            "tool_args": _clip(str(payload.get("tool_args", ""))),
            "tool_result": _clip(str(payload.get("tool_result", ""))),
        }
    elif ctype == T_TOOL_UPDATE and isinstance(payload, dict):
        upd = payload.get("tool_update", payload)
        rec["data"] = {
            "tool_name": upd.get("tool_name", "") if isinstance(upd, dict) else "",
            "status": upd.get("status", "") if isinstance(upd, dict) else "",
            "tool_call_id": upd.get("tool_call_id", "") if isinstance(upd, dict) else "",
        }
    elif ctype == T_USAGE and isinstance(payload, dict):
        rec["data"] = {
            "usage_metadata": payload.get("usage_metadata", {}),
            "result_type": payload.get("result_type"),
            "perf": {k: payload[k] for k in ("total_latency_ms", "ttft_ms", "tpot_ms") if k in payload},
        }
    else:
        # message / controller_output / todo / unknown: keep capped raw text.
        rec["data"] = {"raw": _clip(_as_text(payload))}
    return rec


class TeamStreamCapture(TeamStreamLogger):  # type: ignore[misc]
    """Drop-in ``stream_logger`` that also writes a flat per-event JSONL.

    Usage in team_helpers.py (replacing the existing TeamStreamLogger build)::

        lg = TeamStreamCapture(
            jsonl_path=str(traces_dir / f"stream-{session_id}.jsonl"),
            dump_path=str(traces_dir / f"dump-team-{session_id}.txt"),
        )
        ... Runner.run_agent_team_streaming(..., stream_logger=lg)
    """

    def __init__(self, jsonl_path: str, dump_path: str | None = None) -> None:
        # The base class requires a text-dump path; default it alongside.
        super().__init__(file_path=dump_path or (jsonl_path + ".dump.txt"))
        self._jsonl = open(jsonl_path, "a", encoding="utf-8")
        self._seq = 0

    def feed(self, chunk: Any) -> None:  # called by the runner per chunk
        try:
            rec = _record_for(chunk, self._seq)
            self._seq += 1
            self._jsonl.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self._jsonl.flush()
        except Exception:
            pass  # capture must never break the run
        try:
            super().feed(chunk)  # keep the base text dump
        except Exception:
            pass

    def flush(self) -> None:
        try:
            self._jsonl.flush()
        except Exception:
            pass
        try:
            super().flush()
        except Exception:
            pass


# --------------------------------------------------------------------------
# Offline grouping
# --------------------------------------------------------------------------
def group_turns(jsonl_path: str | Path) -> dict[str, list[dict[str, Any]]]:
    """Fold the flat per-event stream into per-member turns.

    A "turn" = one LLM call for a member: the reasoning + answer text streamed
    before its ``llm_usage`` marker, the tool calls it made, the results, and
    that call's token usage. ``input_delta`` is the rise in input_tokens since
    the member's previous turn — an approximate marginal cost of whatever
    entered context (e.g. a file read) between calls. It is a reconstruction,
    not a provider-reported per-action figure.
    """
    records = [json.loads(l) for l in Path(jsonl_path).read_text(encoding="utf-8").splitlines() if l.strip()]

    turns: dict[str, list[dict[str, Any]]] = {}
    cur: dict[str, dict[str, Any]] = {}  # member -> in-progress turn

    def blank() -> dict[str, Any]:
        return {"reasoning": [], "answer": [], "tool_calls": [], "tool_results": [], "usage": None}

    for r in records:
        m = r.get("member") or "(unknown)"
        t = r.get("type")
        d = r.get("data") or {}
        turn = cur.setdefault(m, blank())
        if t == T_REASONING:
            turn["reasoning"].append((d or {}).get("text") or "")
        elif t in (T_OUTPUT, T_ANSWER):
            turn["answer"].append((d or {}).get("text") or "")
        elif t == T_TOOL_CALL:
            turn["tool_calls"].append(d)
        elif t == T_TOOL_RESULT:
            turn["tool_results"].append(d)
        elif t == T_USAGE:
            um = d.get("usage_metadata", {}) or {}
            turn["usage"] = {
                "model": um.get("model_name"),
                "input_tokens": int(um.get("input_tokens") or 0),
                "output_tokens": int(um.get("output_tokens") or 0),
                "cache_tokens": int(um.get("cache_tokens") or 0),
                "total_tokens": int(um.get("total_tokens") or 0),
                "perf": d.get("perf", {}),
            }
            # Close the turn.
            prev = turns.get(m, [])
            prev_in = prev[-1]["usage"]["input_tokens"] if prev and prev[-1].get("usage") else 0
            closed = {
                "member": m,
                "turn": len(prev),
                "usage": turn["usage"],
                "input_delta": turn["usage"]["input_tokens"] - prev_in,
                "reasoning_text": "".join(turn["reasoning"]),
                "answer_text": "".join(turn["answer"]),
                "tool_calls": turn["tool_calls"],
                "tool_results": turn["tool_results"],
            }
            turns.setdefault(m, []).append(closed)
            cur[m] = blank()
    return turns


def _summary(turns: dict[str, list[dict[str, Any]]]) -> None:
    print(f"{'member':<24} {'turns':>5} {'out_tok':>8} {'billed_in':>10} {'tool_calls':>11}")
    print("-" * 62)
    for m in sorted(turns):
        ts = turns[m]
        out = sum(t["usage"]["output_tokens"] for t in ts if t.get("usage"))
        bin_ = sum(t["usage"]["input_tokens"] for t in ts if t.get("usage"))
        tc = sum(len(t["tool_calls"]) for t in ts)
        print(f"{m:<24} {len(ts):>5} {out:>8,} {bin_:>10,} {tc:>11}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) == 3 and sys.argv[1] == "group":
        grouped = group_turns(sys.argv[2])
        out = Path(sys.argv[2]).with_name("turns_by_member.json")
        out.write_text(json.dumps(grouped, indent=2, ensure_ascii=False), encoding="utf-8")
        _summary(grouped)
        print(f"\nWrote {out}")
    else:
        print(__doc__)
