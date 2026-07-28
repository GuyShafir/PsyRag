# PsyRag as Claude Code memory (MCP)

`psyrag mcp` embeds the memory engine and speaks MCP over stdio — no daemon.
It builds a graph of your project's files from what the agent reads/edits, and
recall reorders itself by what actually proves useful.

## Install

    claude mcp add psyrag -- psyrag mcp

Memory is stored in `.psyrag/` at your repo root. Add it to your global
gitignore:

    echo ".psyrag/" >> ~/.config/git/ignore

## Hooks (the learning signal)

In Claude Code `settings.json`:

    {
      "hooks": {
        "PostToolUse": [
          {"matcher": "Read|Edit|Write",
           "hooks": [{"type": "command", "command": "psyrag mcp-send"}]}
        ],
        "PreCompact": [
          {"hooks": [{"type": "command", "command": "psyrag mcp-send"}]}
        ]
      }
    }

`mcp-send` is fire-and-forget: if no server is attached it exits silently.

## Concurrency

- Different projects run fully independently (own `.psyrag/`, own flock).
- Two Claude Code sessions on the **same** project: the second fails fast —
  single-writer is required for durability.

## Maintenance

- `PreCompact` triggers light consolidation.
- Sleep (heavy downscale) runs automatically on startup if >24h have passed.

## Limitations

- The unix socket lives at `.psyrag/mcp.sock`. On macOS/BSD, sockaddr_un caps
  the absolute path near ~104 bytes, so very deeply-nested repo checkouts are
  unsupported for now — move the repo to a shorter path if `psyrag mcp` fails
  to bind.
