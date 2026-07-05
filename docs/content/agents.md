## AI Agents

CloudCat is built to be driven by AI coding agents (Claude Code, Cursor, and
anything else that runs shell commands) as much as by humans. An agent can
answer "what's in this bucket/file?" in a couple of cheap commands instead of
writing pandas code or downloading files.

### Why agents work well with CloudCat

- **stdout is data only** — schema, progress, and warnings go to stderr, and
  color auto-disables when piped, so output parses cleanly
- **NDJSON output** (`-o json`) — one JSON object per line, trivially parseable
- **Non-interactive** — `-y` skips every confirmation prompt
- **Cheap by default** — `--where` streams and stops at `-n` matches, Parquet
  row groups are skipped via min/max statistics, and `--count` on
  Parquet/ORC reads only footer metadata
- **No credentials needed for local files** — agents can test recipes on
  disk before touching cloud data
- **Exit codes** — `0` on success, non-zero with a stderr message on failure

### The CloudCat skill

The repository ships an [Agent Skill](https://agentskills.io) —
[`skills/cloudcat/SKILL.md`](https://github.com/jonathansudhakar1/cloudcat/blob/main/skills/cloudcat/SKILL.md)
— a compact reference that teaches an agent the optimal scanning recipes
(schema discovery, filtered sampling, counts, column profiling) without a
`--help` roundtrip or trial-and-error.

Install it for **Claude Code**:

```bash
# For every project (personal skills directory)
mkdir -p ~/.claude/skills
curl -fsSL --create-dirs -o ~/.claude/skills/cloudcat/SKILL.md \
  https://raw.githubusercontent.com/jonathansudhakar1/cloudcat/main/skills/cloudcat/SKILL.md

# Or for a single project
mkdir -p .claude/skills
curl -fsSL --create-dirs -o .claude/skills/cloudcat/SKILL.md \
  https://raw.githubusercontent.com/jonathansudhakar1/cloudcat/main/skills/cloudcat/SKILL.md
```

Any other agent framework that supports the SKILL.md format (or plain
markdown instructions) can use the same file.

### What the skill teaches

```bash
# Structure, no data read
cloudcat s3://bucket/events/ -s schema_only -y

# Exact row count — metadata-only for Parquet/ORC
cloudcat s3://bucket/events/ --count -s schema_only -y

# Filtered sample as NDJSON (streams; stops at 5 matches)
cloudcat s3://bucket/events/ -w "type=purchase AND amount>250" -n 5 -o json -s dont_show -y

# Per-column nulls / distinct / min / max
cloudcat s3://bucket/events/ --stats -n 0 -s dont_show -y
```

In verification runs, an agent with the skill answered a four-part data
exploration task with zero failed commands and no `--help` call, using
NDJSON output throughout — the same task without the skill cost an extra
discovery roundtrip, a failed command (missing `-y`), and human-oriented
output formats.
