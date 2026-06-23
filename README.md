## Deterministic Distributed Team Composition (```team.mode == "predefined"```)

To ensure a fixed roster of roles with fixed personas, built identically every run, with no LLM involvement in team construction, and adopted by real remote teammate nodes.

Core edits live in one file:
```jiuwenswarm/agents/harness/team/remote_member_bootstrap.py```.

 Benchmark Requirements for Separation:
 
1. **Composition** — exact members exist every run
2. **Personas** — each member's role definition (including its `MUST` / `MUST NOT`
   boundary clauses) is byte-identical every run.
Out of the box, neither holds in distributed mode: the leader LLM decides who to
spawn, names them, and invents their personas. (the "prompt-only" arm of the
experiment, not a controlled roster??)

---

### Per-Run Cycle Template (TBU)

#### 0. syntax check after any edit to the harness 
```
cd ~/jiuwenswarm6/jiuwenswarm && \
  python3 -c "import ast; ast.parse(open('jiuwenswarm/agents/harness/team/remote_member_bootstrap.py').read()); print('OK')"
```

#### 1. CLEAN — mandatory; stale STARTING rows will silently refuse to restart
```
pkill -9 -f jiuwenswarm; pkill -9 -f a2x-registry; sleep 1
rm -f /home/<user>/team_shared.db
rm -rf /tmp/jiuwenswarm/shared_workspace/jiuwen_team/*
```

#### 2. T1 — registry
```
a2x-registry
```

#### 3. T2 — teammate (blank claw). Confirm "blank agent registered ... dataset=team_pool".
```
export API_BASE="https://api.deepseek.com"
export API_KEY="/"
export MODEL_NAME="deepseek-v4-pro"
export MODEL_PROVIDER="DeepSeek"
cd ~/jiuwenswarm6/jiuwenswarm
HOME=~/mate_home AGENT_SERVER_PORT=18093 GIT_AUTHOR_NAME=bot GIT_AUTHOR_EMAIL=bot@x.com GIT_COMMITTER_NAME=bot GIT_COMMITTER_EMAIL=bot@x.com python -m jiuwenswarm.server.app_agentserver
```

#### 4. T3 — leader (carry model env; persona map env not needed for predefined)
```
unset JIUWEN_TEAM_PERSONA_MAP
export API_BASE="https://api.deepseek.com"
export API_KEY="/"
export MODEL_NAME="deepseek-v4-pro"
export MODEL_PROVIDER="DeepSeek"
cd ~/jiuwenswarm6/jiuwenswarm
HOME=~/leader_home AGENT_SERVER_PORT=18092 GATEWAY_PORT=19001 WEB_PORT=19000 GIT_AUTHOR_NAME=bot GIT_AUTHOR_EMAIL=bot@x.com GIT_COMMITTER_NAME=bot GIT_COMMITTER_EMAIL=bot@x.com python -m jiuwenswarm.app
```

#### 5. T4 — frontend (optional, to drive via UI)
```
cd ~/jiuwenswarm6/jiuwenswarm/jiuwenswarm/channels/web/frontend
VITE_WS_BASE="ws://localhost:19000" npm run dev
```

#### 6. Prompt Initialisation

#### 7. Evaluate

---
 
The leader LLM chose the roster by calling the `spawn_member` tool with names and
`desc` (persona) of its own invention. Re-runs differed; personas were not under
experimental control. **Resolution (config, no code):** 
declare the roster in
`modes.team.jiuwen_team.predefined_members` and set `team_mode: predefined`.
- `build_team` instantiates predefined members itself (no LLM), writing each
  member's `persona` to the DB `desc` column:
  `spawn_member(desc=member_spec.persona, ...)`.
- `build_context_from_db` reads it back as `ctx.persona`
  (`persona = teammate.desc or ""`), which feeds the `team_persona` section of the
  teammate's system prompt.
- _`team_mode: predefined` is honored explicitly by `_resolve_team_mode`
  (`agent_configurator.py`); it **locks the roster and removes the leader's
  `spawn_member` tool**, so the LLM can only coordinate the roster it is given.
  A non-empty `predefined_members` list *without* `team_mode: predefined`
  auto-derives `hybrid` — the roster is built, but the leader keeps its spawn tools
  and can still add members. Must set `team_mode: predefined` explicitly to lock._

### Problem Encountered: predefined members never started in distributed mode
 
With the roster locked, the 4 members (test) were written to the shared DB but wedged at
status `STARTING` and never executed. Root cause, traced through source:
 
- Distributed remote adoption (reserve a blank teammate via the A2X registry, send a
  bootstrap envelope over ZMQ, teammate adopts the identity, ACK flips the row to
  `READY`) is driven by a wrapper on the **`spawn_member` tool**
  (`attach_spawn_member_remote_bootstrap_wrapper`). But `build_team` registers predefined members by calling the **backend**
  `spawn_member` method directly, **bypassing that tool wrapper**. Meanwhile `attach_distributed_local_spawn_guard` correctly suppresses *local*
  teammate creation in distributed leader mode ("jiuwenswarm owns remote bootstrap"). Net effect: predefined rows were created, local start was suppressed, and nothing
  drove remote adoption. `startup()` claimed each row `UNSTARTED → STARTING` and its
  `on_created` had nothing to hand off to, leaving them stuck at `STARTING`. **Resolution (code change):** a sweep that does for predefined rows what the tool
wrapper does for LLM-spawned ones.

### Changes

Two module-level functions added to `remote_member_bootstrap.py`:
 
- **`_predefined_remote_bootstrap_sweep(team_agent, *, session_id, channel_id)`**
  For each non-leader, not-yet-`READY` member row:
  1. force status to `UNSTARTED`;
  2. call `send_bootstrap_message(..., registry_reservation=None)` — which reserves a
     blank teammate from the A2X registry itself and delivers the bootstrap envelope;
  3. leave the row `UNSTARTED`; the existing **MESSAGE-ACK listener** transitions it
     to `READY` once the teammate adopts.
  It deliberately does **not** call `precheck_and_reserve_remote_spawn` — that
  helper's "member already exists" guard rejects the rows `build_team` already wrote
  (it is designed for the tool path, which reserves *before* the row is created).
- **`attach_predefined_remote_bootstrap_sweep(team_agent, *, session_id, channel_id)`**
  Wraps the backend's `_on_team_built` callback (preserving the original
  `_mark_team_built`) so the sweep runs immediately after `build_team` registers the
  predefined roster.
It is installed from the tail of `attach_distributed_local_spawn_guard`, which already
runs at runtime-ready in distributed-leader mode with the right arguments in scope.
 
_The change is additive and scoped to the distributed-leader + predefined path; local
and inprocess modes are unaffected._


## Persona / Status chain 
 
```
config.yaml  modes.team.jiuwen_team.predefined_members[].persona
  → _build_predefined_members           (config_loader.py; keeps `persona`)
  → TeamMemberSpec.persona
  → build_team predefined loop           (team.py: spawn_member(desc=persona, status=UNSTARTED))
  → team_member.desc                     (shared SQLite row)
  → _predefined_remote_bootstrap_sweep   (force UNSTARTED → send_bootstrap_message)
  → reserve blank teammate (A2X) + ZMQ bootstrap envelope
  → teammate adopts identity, sends MESSAGE ACK
  → ACK listener: UNSTARTED → READY
  → build_context_from_db: ctx.persona = teammate.desc
  → TeamPolicyRail `team_persona` section in the teammate's system prompt
```


#### Config

In `<LEADER_HOME>/.jiuwenswarm/config/config.yaml`, under
`modes.team.jiuwen_team` (the block the spec is built from — NOT the
top-level `team:` runtime marker):

    modes:
      team:
        jiuwen_team:
          team_mode: predefined          # locks roster, drops leader spawn tools
          predefined_members:
            - member_name: diagnostician
              display_name: study_diagnostician
              role_type: teammate
              persona: "ROLE: ... (full role spec incl. MUST / MUST NOT)"
            # explainer, practice-coach, consolidator ...

- `team_mode: predefined` is honored explicitly by `_resolve_team_mode`
  (agent_configurator); without it, a non-empty `predefined_members` list
  auto-derives `hybrid`, which leaves the leader able to spawn extra members.
- `persona` is written to the member row's `desc` by `build_team`
  (team.py: `spawn_member(desc=member_spec.persona)`), and read back as
  `ctx.persona` by `build_context_from_db`. Personas come from config; the
  `JIUWEN_TEAM_PERSONA_MAP` env var / bootstrap fallback is not used here.
- Generate the block from a persona map with `gen_predefined.py`; keep keys
  `member_name`, `display_name`/`name`, `persona`, `prompt_hint?`, `role_type`.

#### DB connection string

`team.storage.params.connection_string` must be a bare filesystem path
(`/home/<user>/team_shared.db`), shared and identical across all nodes —
NOT a `sqlite://` URL.

#### Remote adoption of predefined members

`build_team` registers predefined members as UNSTARTED rows via the backend
spawn path, which bypasses the spawn_member tool wrapper that normally drives
remote bootstrap. `attach_predefined_remote_bootstrap_sweep`
(remote_member_bootstrap.py, installed from `attach_distributed_local_spawn_guard`)
reserves a blank teammate and sends a bootstrap envelope for each UNSTARTED
predefined row; the ACK listener sets READY on adoption. One teammate node
adopts one member — run one teammate node per role for the full roster.

#### Verify

    sqlite3 $DB "select member_name,status,substr(desc,1,30) from team_member;"

Each adopted member should reach `ready` with its persona verbatim in `desc`.
