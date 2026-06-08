# .skills/

A vendor-neutral, agent-agnostic home for reusable coding skills that any AI coding agent can read and follow.

## Purpose

Skills are step-by-step instructions for common, repeatable tasks in this project. Unlike IDE-specific rules files (e.g. `.cursor/rules/`) or tool-specific configs, every file here is plain Markdown — readable by any agent that can read files: Claude Code, Codex, Cursor, Copilot, Gemini, or a human.

## How agents should use this directory

1. **Discover** — list the subdirectories under `.skills/` to see what skills are available.
2. **Match** — when a task matches a skill name or description, read the corresponding `SKILL.md`.
3. **Follow** — execute the steps in `SKILL.md` in order, using the project-specific details it provides.

If no skill matches your task, proceed with your best judgment and the project's `AGENTS.md` as context.

## Available skills

| Skill | Description |
|-------|-------------|
| [add-datagen-generator](add-datagen-generator/SKILL.md) | Add a new Protobuf-backed data generator (proto + GenX class + CLI + tests). Use when adding a new entity type (e.g. Shipment, Payment) following the User/Order pattern. |

## Adding a new skill

Create a subdirectory named after the task (kebab-case) and add a `SKILL.md` inside it. Write it as plain Markdown — no frontmatter, no tool-specific syntax. Then add a row to the table above.
