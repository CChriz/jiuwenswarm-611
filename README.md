# Jiuwenswarm (6/11) - In-depth Stream Capture (to be updated for distributed mode compatibility)

## check `distributed-mode-fixes` branch for active changes

An addition to JiuwenSwarm "team mode" that reconstructs, for any completed team
run, a faithful per-agent record of what each agent did: its reasoning, the
messages it sent, the tools it invoked, the file changes it made (full write
contents *and* edit diffs), and its token usage per LLM call. All correctly
attributed to the agent that performed the action and grouped into turns.

The base framework emits this information transiently for live UI display and
never persists it in an inspectable, per-agent form (and deliberately excludes
tool-level events from its monitor stream). This layer closes that gap **without
changing the agent runtime's behavior**.

---

## Added

1. **`team_stream_capture.py`** — contains both halves of the pipeline:
   - `TeamStreamCapture` — a drop-in `TeamStreamLogger` subclass. Passed as the
     `stream_logger`, it writes one JSON object per stream chunk to a flat
     `stream-<session>.jsonl` file — the uninterpreted **source of truth**. It is
     exception-isolated (it can never perturb a run) and also keeps the base
     class's plain-text dump.
   - `group_turns` (offline) — reads that JSONL and reconstructs per-agent
     "turns": reasoning, answer text, actions taken (with file contents/diffs),
     and per-call token usage. Invoked via the CLI below.

2. **`per_member_tokens.py`** — an offline aggregator that reads the framework's
   built-in `history.json` and reports per-agent token totals (calls, output,
   billed input, peak context, cache, cost).

3. **Wiring (`team_helpers.py`)** — a small, non-destructive `_Tee` that forwards
   every stream chunk to both the original logger and `TeamStreamCapture`, so the
   base behavior is byte-for-byte unchanged. Capture is **always-on**, not gated
   behind the trace-debug flag.

> - `team_stream_capture.py` → `<jiuwenswarm/agents/harness/team/handlers/>`
> - `per_member_tokens.py` → `</scripts dir>`
> - `_Tee` + capture construction → inside `_consume_stream_with_query` in
>   `<jiuwenswarm/.../team_helpers.py>`

---

## Why it exists

The base framework gives **coordination** observability (who messaged whom, task
state) but not **behavioral** observability (what each agent reasoned, did,
produced, and cost). This layer adds the latter, note:

- **Cumulative input tokens.** `usage_metadata.input_tokens` is cumulative per
  conversation (each call re-sends the growing context) 
- **Content vs. Attribution split.** Tool-result chunks are correctly
  agent-tagged but carry no content, whereas tracer/OTel spans carry the content (the
  actual written bytes / edit diff) but no agent tag. The two must be **joined**.

---

## How it works

### Capture (live)

`TeamStreamCapture` records one record per chunk, preserving reasoning/answer
text, tool events, per-call `llm_usage`, and — critically — the `tracer_agent`
spans (which the base capture path discarded). Tracer spans carry each tool
invocation's full `inputs`/`outputs`, which is where action contents live.

File-content policy: full content inline under `MAX_INLINE_CHARS` (20,000);
larger payloads are clipped to a preview + true length + sha1.

### Grouping (offline)

`group_turns` splits the interleaved concurrent stream by agent, then by LLM call
(each `llm_usage` chunk closes a call), emitting per-agent turns. The
**attribution join** makes the agent-tagged tool-result the authoritative action
and enriches it with content from the matching tracer span. Matching is by
**exact per-tool order**: the k-th tool-result of a tool pairs with the k-th
tracer span of that tool (both in stream order), which is deterministic even when
one agent calls the same tool twice in a turn. A guard falls back to
nearest-sequence matching for any tool whose result/span counts diverge, tagging
those actions `match: "nearest_seq"` so they are auditable rather than silently
trusted.

---

## Usage

### Behavioral breakdown (per-agent turns, actions, file changes)

```bash
python team_stream_capture.py group \
  <instance>/.agent_teams/traces/stream-<session_id>.jsonl
```

Prints a per-member summary (turns, output tokens, billed input, action counts,
tool-usage breakdown, write/edit counts) and writes `turns_by_member.json` next
to the input file.

> Use the **`stream-…jsonl`** file, not `capture-…dump.txt`.
> `.jsonl` → tools; `.dump.txt` → human eyeballing only (the grouper NOT parse it).

### Token totals (per-agent)

```bash
python per_member_tokens.py \
  <instance>/agent/sessions/<session_id>/history.json
```

Writes `token_usage_by_member.json` and prints a per-agent table.

---

## Reading `turns_by_member.json`

Per agent, in order, each turn contains `reasoning_text`, `answer_text`,
`usage` (with `input_delta`), and `actions`. Each action has `tool`, `match`, and
the relevant content fields:

- **`write_file`** → `content` (the full new file body) + `file_path`.
- **`edit_file` / `str_replace`-style** → `old_string` + `new_string`
  (or `old_str`/`new_str`) — the **diff** of just the changed span + `file_path`.
- Other tools surface `path`, `command`, `query`, etc. as available, plus the
  full `inputs` dict and an `outputs` summary.

> **To find "what did this agent change to a file," check BOTH `content`
> (writes) AND `new_string` (edits).** Grepping only for `content` misses every
> edit. An edit gives you the diff, not the full post-edit file (isolates exactly what the agent changed).


### The `match` field

On a clean run, every action reads `match: "order"` (exact deterministic
pairing). Any `"nearest_seq"` (or a missing `match`) flags a tool whose
result/span counts diverged for that run, usable, but that action's content used
the weaker fallback. Quick scan:

```bash
python -c "import json; g=json.load(open('turns_by_member.json')); \
print([(t['member'],a['tool']) for ts in g.values() for t in ts for a in t['actions'] if a.get('match')!='order'])"
```

If empty list → everything paired exactly.

---

## Which file for which question

- **Token accounting** (who spent what) → `history.json` + `per_member_tokens.py`.
  This is the framework's own persisted billing record and is the better source
  for tokens.
- **Behavioral detail** (reasoning, file reads/writes/edits with content,
  action sequence) → `stream-…jsonl` + `team_stream_capture.py group`.
  `history.json` does **not** carry tracer spans, so it cannot give write content
  or the disambiguated action join.

---

## Notes 

- If a run produces no `stream-*.jsonl`, the patched `team_stream_capture.py` is
  not deployed or the `_Tee` wiring is missing — the run will not error, it just
  won't capture.
- The flat `stream-…jsonl` is always the ground truth. Re-running `group` is free
  and non-destructive (it only regenerates `turns_by_member.json`); you never need
  to re-run the team to fix a grouping question.
- An edit captures the diff, not the full post-edit file state. Full state is
  reconstructable offline (seed from the file's first `read_file` content, which
  is full, and replay subsequent edit diffs) but that helper is not built in.
