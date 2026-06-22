# Distributed team setup

Base: openJiuwen-ai/jiuwenswarm @ 80e25b36 (develop)

## Shared DB (required for distributed mode)
In BOTH ~/leader_home/.jiuwenswarm/config/config.yaml and
~/mate_home/.jiuwenswarm/config/config.yaml set:
  team.storage.params.connection_string: /home/cz776/team_shared.db   # bare path, not a sqlite:// URL

## Framework version evaluated
(run `pip show openjiuwen` and record the version here)
