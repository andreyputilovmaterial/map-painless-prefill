# Maintenance log

> Cross-project maintenance notes. Entries here capture **structural** changes
> that should be repeatable across sibling projects and their submodules.
>
> Repo-specific changes do **not** belong here; they stay in the commit history
> and `docs/project-context.md`. Anything recorded here is meant to be repeatable
> in other projects of the same structure.
>
> Entries are ordered **newest first**.

## How to read an entry

Each entry is a section whose heading reads like a commit subject: it states the
**intent** of the change and **where** the change happens (component/module).
The body states the **requirement first** — how the code should be — and only
then the change that reaches it. The point is that an agent can *verify* the
requirement against any project, even when its current code does not look exactly
like the `Input` example; anything that does not match should be updated.

- **When** — date/time requested and implemented.
- **What & why** — the request and the reasoning behind the chosen approach.
- **Input** — a sample of the code/file **before** the change (one of several
  possible shapes; the real "before" may differ).
- **Output** — a sample of the code/file **after** the change.
- **Repeat** — the requirement to verify in another project, each item stated
  declaratively ("… should be …"); if the code does not match, change it to do so.

---

## Cross-repo sync: pull updates, advance owned submodules, adopt commit rules, ignore `.roadmap-*`

- **Requested:** 2026-11-15
- **Implemented:** 2026-11-15
- **Where:** `.gitignore`, `.clinerules/`, `docs/maintenance-log.md`.

### What & why

Brought this repo in line with the shared cross-project maintenance conventions
and refreshed it from its remotes: pulled new updates; recursively updated
submodules to the recorded commits; advanced owned submodules
(`andreyputilovmaterial/*`, `The-city-not-present/pinliner`) to their latest
remote commit (other external submodules stay pinned to the recorded pointer);
added `.roadmap-*` to `.gitignore` for provisional planning notes; adopted the
`2x-` commit-message rules from `MDM-Diff-py/.clinerules`; and added the
submodule-sync-at-task-start rule.

### Repeat

1. `.gitignore` should ignore files starting with `.roadmap-` (`.roadmap-*`).
2. `.clinerules/` should contain `20-git-commit-messages-wording.md`,
   `21-git-submodules-commits-order.md`, `22-when-making-commits.md`,
   `29-github-issues.md`, and a submodule-sync-at-task-start rule; the older
   `08-commit-messages.md`, `09-github-issues.md`, `11-submodules-commits.md`
   should be replaced by the `2x-` equivalents.
3. Submodules owned by `andreyputilovmaterial` or `The-city-not-present`
   (`pinliner`, `myers-diff`) should be advanced to their latest remote commit;
   any other external submodule should stay pinned to the recorded pointer.
4. `docs/maintenance-log.md` should exist and list this entry newest-first.
