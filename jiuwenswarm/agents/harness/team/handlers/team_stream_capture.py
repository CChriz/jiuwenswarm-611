#!/usr/bin/env python3
"""Live per-event capture of a running team, plus a turn-grouping pass.

Two pieces, by design:

1.  ``TeamStreamCapture`` — a drop-in ``TeamStreamLogger`` subclass. Pass it
    as ``stream_logger=`` to ``Runner.run_agent_team_streaming``; it writes one
    JSON line per stream chunk to ``stream-{session}.jsonl`` — the flat,
    uninterpreted SOURCE OF TRUTH. It also keeps the base class text dump. It
    never raises into the run.

2.  ``group_turns`` — an OFFLINE pass over that JSONL. It splits the stream by
    member (members run concurrently and interleave), then by LLM call within
    each member (each ``llm_usage`` chunk closes a call), and emits per-member
    "turns" bundling reasoning text, the actions taken, and the call's token
    usage. Grouping is derived from the flat log — if a heuristic is wrong, the
    flat log is still authoritative.

Action contents come from ``tracer_agent`` spans, which carry each tool
invocation's full ``inputs``/``outputs`` (for a write, that's the file path AND
the written content). Because there is one span per invocation, repeated writes
to the same file produce distinct records with distinct contents — captured at
write time, never overwritten. Tracer spans are NOT member-tagged on the wire,
so they're attributed to the most recent member active in the stream (a
proximity heuristic: spans arrive adjacent to the acting member's chunks).

File-content policy: full content inline under MAX_INLINE_CHARS; larger payloads
clip to a preview + true length + sha1. Set huge for always-full, small for
previews only.

CLI:  python team_stream_capture.py group  stream-<session>.jsonl
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
import time
from pathlib import Path
from typing import Any, Optional

try:
    from openjiuwen.agent_teams.monitor import TeamStreamLogger
except Exception:  # offline grouping doesn't need the SDK
    TeamStreamLogger = object  # type: ignore

MAX_INLINE_CHARS = 20_000

T_REASONING = "llm_reasoning"
T_OUTPUT = "llm_output"
T_ANSWER = "answer"
T_TOOL_CALL = "tool_call"
T_TOOL_RESULT = "tool_result"
T_TOOL_UPDATE = "tool_update"
T_MESSAGE = "message"
T_USAGE = "llm_usage"
T_TRACER = "tracer_agent"


def _clip(text: Optional[str]) -> dict[str, Any]:
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
    src = getattr(chunk, "source_member", None)
    role = getattr(chunk, "role", None)
    role_str = getattr(role, "value", role)
    role_str = str(role_str).lower() if role_str is not None else None
    if src:
        return str(src), role_str
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
    elif ctype == T_TRACER and isinstance(payload, dict):
        # OTel span flowing on the stream: per-invocation tool inputs (incl.
        # write content) and outputs. THIS is where action contents live.
        rec["data"] = {
            "name": payload.get("name"),
            "invoke_type": payload.get("invokeType"),
            "invoke_id": payload.get("invokeId"),
            "parent_invoke_id": payload.get("parentInvokeId"),
            "status": payload.get("status"),
            "elapsed_ms": payload.get("elapsedTime"),
            "inputs": _clip(json.dumps(payload.get("inputs"), ensure_ascii=False)
                            if payload.get("inputs") is not None else None),
            "outputs": _clip(json.dumps(payload.get("outputs"), ensure_ascii=False)
                             if payload.get("outputs") is not None else None),
        }
    elif ctype == T_USAGE and isinstance(payload, dict):
        rec["data"] = {
            "usage_metadata": payload.get("usage_metadata", {}),
            "result_type": payload.get("result_type"),
            "perf": {k: payload[k] for k in ("total_latency_ms", "ttft_ms", "tpot_ms") if k in payload},
        }
    else:
        rec["data"] = {"raw": _clip(_as_text(payload))}
    return rec


class TeamStreamCapture(TeamStreamLogger):  # type: ignore[misc]
    """Drop-in ``stream_logger`` that writes a flat per-event JSONL."""

    def __init__(self, jsonl_path: str, dump_path: str | None = None) -> None:
        super().__init__(file_path=dump_path or (jsonl_path + ".dump.txt"))
        self._jsonl = open(jsonl_path, "a", encoding="utf-8")
        self._seq = 0

    def feed(self, chunk: Any) -> None:
        try:
            rec = _record_for(chunk, self._seq)
            self._seq += 1
            self._jsonl.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self._jsonl.flush()
        except Exception:
            pass
        try:
            super().feed(chunk)
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
def _unclip(holder: Any) -> Optional[str]:
    if isinstance(holder, dict):
        return holder.get("text")
    return holder if isinstance(holder, str) else None


def _parse_inputs(inputs_text: Optional[str]) -> dict[str, Any]:
    """Parse a tracer ``inputs`` JSON string and unwrap the inner arg dict.

    Shape on this build: {"inputs": {"file_path": "...", "content": "..."}}.
    """
    if not inputs_text:
        return {}
    try:
        obj = json.loads(inputs_text)
    except Exception:
        return {}
    if isinstance(obj, dict) and isinstance(obj.get("inputs"), dict):
        return obj["inputs"]
    return obj if isinstance(obj, dict) else {}


def parse_tool_result(text: str) -> dict[str, Any]:
    """Fallback recovery from a tool_result repr string when no tracer exists."""
    out: dict[str, Any] = {}
    if not text:
        return out
    try:
        outer = ast.literal_eval(text)
    except Exception:
        m = re.search(r"'tool_name':\s*'([^']+)'", text)
        return {"tool": m.group(1)} if m else {"raw": text[:2000]}
    if not isinstance(outer, dict):
        return {"raw": text[:2000]}
    out["tool"] = outer.get("tool_name")
    result = outer.get("result")
    if isinstance(result, str):
        fp = re.search(r"'file_path':\s*'([^']*)'", result)
        if fp:
            out["file_path"] = fp.group(1)
        out["result_summary"] = result[:300]
    return out


def _action_from_tracer(merged: dict[str, Any]) -> dict[str, Any]:
    args = _parse_inputs(_unclip(merged.get("inputs")))
    action: dict[str, Any] = {
        "tool": merged.get("name"),
        "invoke_type": merged.get("invoke_type"),
        "elapsed_ms": merged.get("elapsed_ms"),
    }
    for key in ("file_path", "path", "content", "command", "query", "pattern"):
        if key in args:
            action[key] = args[key]
    action["inputs"] = args
    out_text = _unclip(merged.get("outputs"))
    if out_text:
        action["outputs"] = out_text[:1000]
    return action


def _collect_tracer_spans(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge tracer start/finish pairs by invoke_id into one span each.

    Returns a list of {seq, tool, file_path, content, inputs, outputs}, where
    content/inputs come from whichever half carried them. These are the
    content carriers; they're matched to member-tagged tool_results later.
    """
    by_id: dict[Any, dict[str, Any]] = {}
    order: list[Any] = []
    for r in records:
        if r.get("type") != T_TRACER:
            continue
        d = r.get("data") or {}
        if not (d.get("name") or _unclip(d.get("inputs")) or _unclip(d.get("outputs"))):
            continue
        key = d.get("invoke_id") or ("noid", r.get("seq"))
        if key not in by_id:
            by_id[key] = {"seq": r.get("seq"), "data": dict(d)}
            order.append(key)
        else:
            tgt = by_id[key]["data"]
            for f in ("inputs", "outputs", "elapsed_ms", "status", "name"):
                if not _unclip(tgt.get(f)) and d.get(f):
                    tgt[f] = d[f]
    spans = []
    for k in order:
        seq = by_id[k]["seq"]
        a = _action_from_tracer(by_id[k]["data"])
        # Prefer the resolved absolute path from the finish outputs if present.
        out = _unclip(by_id[k]["data"].get("outputs"))
        if out:
            try:
                od = json.loads(out)
                rp = (od.get("outputs", {}) or {}).get("data", {})
                if isinstance(rp, dict) and rp.get("file_path"):
                    a["resolved_path"] = rp["file_path"]
            except Exception:
                pass
        a["seq"] = seq
        spans.append(a)
    return spans


def group_turns(jsonl_path: str | Path) -> dict[str, list[dict[str, Any]]]:
    records = [json.loads(l) for l in Path(jsonl_path).read_text(encoding="utf-8").splitlines() if l.strip()]

    # Tracer spans (content carriers, no member) collected globally, in order.
    tracer_spans = _collect_tracer_spans(records)
    # Bucket tracer spans by tool, preserving seq order — for exact order-based
    # pairing with member-tagged tool_results (k-th call <-> k-th span). Both
    # streams are 1:1 per tool and globally ordered, so this is deterministic
    # even when one member calls the same tool twice in a turn.
    spans_by_tool: dict[str, list[dict[str, Any]]] = {}
    for sp in sorted(tracer_spans, key=lambda s: s.get("seq", 0)):
        spans_by_tool.setdefault(sp.get("tool"), []).append(sp)
    span_cursor: dict[str, int] = {}
    # Pre-count tool_results per tool so we can verify 1:1 and fall back safely.
    res_seqs_by_tool: dict[str, list[int]] = {}
    for r in records:
        if r.get("type") == T_TOOL_RESULT and r.get("member"):
            rt = (r.get("data") or {}).get("tool_result") or {}
            rtext = rt.get("text", "") if isinstance(rt, dict) else str(rt)
            tool = parse_tool_result(rtext).get("tool")
            if tool:
                res_seqs_by_tool.setdefault(tool, []).append(r.get("seq", 0))
    # A tool is order-safe iff #tool_results == #tracer spans for it.
    order_safe = {
        tool: len(res_seqs_by_tool.get(tool, [])) == len(spans_by_tool.get(tool, []))
        for tool in set(res_seqs_by_tool) | set(spans_by_tool)
    }

    def next_span_for(tool: str, seq: int) -> Optional[dict[str, Any]]:
        spans = spans_by_tool.get(tool)
        if not spans:
            return None
        if order_safe.get(tool):
            i = span_cursor.get(tool, 0)
            if i < len(spans):
                span_cursor[tool] = i + 1
                return spans[i]
            return None
        # Fallback: nearest unclaimed by seq (counts diverged for this tool).
        best, best_i = None, None
        for i, sp in enumerate(spans):
            if sp.get("_used"):
                continue
            if best is None or abs(sp["seq"] - seq) < abs(best["seq"] - seq):
                best, best_i = sp, i
        if best_i is not None:
            spans[best_i]["_used"] = True
        return best

    turns: dict[str, list[dict[str, Any]]] = {}
    cur: dict[str, dict[str, Any]] = {}

    def blank() -> dict[str, Any]:
        return {"reasoning": [], "answer": [], "actions": [], "usage": None}

    for r in records:
        t = r.get("type")
        d = r.get("data") or {}
        m = r.get("member")
        if not m:
            continue  # only member-tagged chunks define turns; tracer joined here
        turn = cur.setdefault(m, blank())

        if t == T_REASONING:
            turn["reasoning"].append(_unclip(d) or "")
        elif t in (T_OUTPUT, T_ANSWER):
            turn["answer"].append(_unclip(d) or "")
        elif t == T_TOOL_RESULT:
            rt = d.get("tool_result") or {}
            rtext = rt.get("text", "") if isinstance(rt, dict) else str(rt)
            pa = parse_tool_result(rtext)  # {tool, file_path, result_summary}
            tool = pa.get("tool")
            action = {"tool": tool, "seq": r.get("seq")}
            if pa.get("file_path"):
                action["file_path"] = pa["file_path"]
            sp = next_span_for(tool, r.get("seq")) if tool else None
            if sp:
                action["match"] = "order" if order_safe.get(tool) else "nearest_seq"
                for k in ("content", "path", "command", "query", "pattern",
                          "inputs", "outputs", "elapsed_ms", "resolved_path"):
                    if sp.get(k) is not None and k not in action:
                        action[k] = sp[k]
                if not action.get("file_path") and sp.get("file_path"):
                    action["file_path"] = sp["file_path"]
            turn["actions"].append(action)
        elif t == T_USAGE:
            um = d.get("usage_metadata", {}) or {}
            usage = {
                "model": um.get("model_name"),
                "input_tokens": int(um.get("input_tokens") or 0),
                "output_tokens": int(um.get("output_tokens") or 0),
                "cache_tokens": int(um.get("cache_tokens") or 0),
                "total_tokens": int(um.get("total_tokens") or 0),
                "perf": d.get("perf", {}),
            }
            prev = turns.get(m, [])
            prev_in = prev[-1]["usage"]["input_tokens"] if prev and prev[-1].get("usage") else 0
            turns.setdefault(m, []).append({
                "member": m,
                "turn": len(prev),
                "usage": usage,
                "input_delta": usage["input_tokens"] - prev_in,
                "reasoning_text": "".join(turn["reasoning"]),
                "answer_text": "".join(turn["answer"]),
                "actions": turn["actions"],
            })
            cur[m] = blank()
    return turns


def _summary(turns: dict[str, list[dict[str, Any]]]) -> None:
    from collections import Counter
    print(f"{'member':<24} {'turns':>5} {'out_tok':>8} {'billed_in':>11} {'actions':>8}")
    print("-" * 60)
    for m in sorted(turns):
        ts = turns[m]
        out = sum(t["usage"]["output_tokens"] for t in ts if t.get("usage"))
        bin_ = sum(t["usage"]["input_tokens"] for t in ts if t.get("usage"))
        acts = sum(len(t["actions"]) for t in ts)
        print(f"{m:<24} {len(ts):>5} {out:>8,} {bin_:>11,} {acts:>8}")
    tools: Counter = Counter()
    writes = 0
    for ts in turns.values():
        for t in ts:
            for a in t["actions"]:
                name = a.get("tool")
                if name:
                    tools[name] += 1
                if a.get("content") is not None and (a.get("file_path") or a.get("path")):
                    writes += 1
    if tools:
        print("\ntool usage (all members):")
        for name, n in tools.most_common():
            print(f"  {n:>3}  {name}")
    print(f"\nwrite-type actions with captured content: {writes}")


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


