# CI and branch protection

This file records which workflows run where, and the exact branch-ruleset
configuration that makes "CI is green" a real merge precondition rather than
an honour-system one.

## Workflows and their triggers

| Workflow | File | Triggers |
| --- | --- | --- |
| `Test pg_ash` | `.github/workflows/test.yml` | push to `main` and `release/**`, tags `v*`, PRs into `main` and `release/**`, `workflow_dispatch` |
| `demo` | `.github/workflows/demo.yml` | every push, every PR, nightly cron, `workflow_dispatch` |
| CodeQL | code scanning **default setup** (no file in this repo) | push to the default branch and PRs into it, weekly |

`test.yml` originally filtered both `push` and `pull_request` to `branches:
[main]`. Release-branch PRs — the `release/2.0` line, for example — therefore
merged with the full PG 14–19 matrix never having run once. `demo.yml` has
always used unfiltered `push:`/`pull_request:`, so it needed no change.

CodeQL is configured through code-scanning *default setup*, which covers the
default branch only and has no branch knob. A PR into a `release/**` branch
will not get a `CodeQL` check run. That matters for the required-checks list
below: if the ruleset is ever extended beyond `~DEFAULT_BRANCH`, requiring
`CodeQL` would leave release-branch PRs blocked on a check that can never
arrive. Switching to advanced setup (a committed `codeql.yml`) is the way to
get CodeQL onto release branches, and is out of scope here.

## The `ci-required` aggregator

`test.yml` ends with a `ci-required` job: `if: always()`, `needs: [docs-lint,
test]`, failing if any needed job reports anything other than `success` or
`skipped`.

It exists because the matrix contexts embed PostgreSQL versions — `test (14,
on)`, `test (19beta2, on)` — and are renamed at every version bump. Requiring
those names directly means the ruleset silently stops covering a version the
moment the matrix changes, or blocks every PR on a context that no longer
exists. `ci-required` is one stable name that transitively covers the whole
matrix, whatever it contains next year.

It also converts one specific silent failure into a hard block. GitHub refuses
to create a run when a workflow file exceeds 512,000 bytes and reports nothing
at all (see #237). Today that shows up as *absent* checks, which a required
check cannot detect if the required names all come from that same file — but a
required check that never reports leaves the PR permanently pending, which is
visible. `test.yml` is large; keep an eye on its size.

## Required status checks (owner action)

The `main-protected` ruleset (id `13093534`) currently has `deletion`,
`non_fast_forward` and `pull_request` rules but **no `required_status_checks`
rule**. Nothing at the ruleset level requires any check to have run, so a PR
whose workflows never started — a fork PR pending workflow approval, a branch
the trigger filters excluded, a workflow file over the size limit — is
mergeable into `main` with zero evidence.

Applying the payload below adds that rule. It preserves the existing rules
verbatim; only the new `required_status_checks` entry is added.

```bash
gh api -X PUT repos/NikolayS/pg_ash/rulesets/13093534 \
  --input docs/ci/main-protected-ruleset.json
```

Verify afterwards with:

```bash
gh api repos/NikolayS/pg_ash/rulesets/13093534 \
  --jq '.rules[] | select(.type=="required_status_checks")'
```

### Which checks, and why those

| Context | App | Why |
| --- | --- | --- |
| `ci-required` | `github-actions` (15368) | Stable rollup of `docs-lint` + the whole PG matrix |
| `renderer (fixtures, no database)` | `github-actions` (15368) | Fast demo gate, runs on every PR, stable name |
| `capture + reel (real PostgreSQL)` | `github-actions` (15368) | Real-database demo gate; runs on every PR |
| `CodeQL` | `github-advanced-security` (57789) | Default setup covers the default branch, which is all this ruleset targets |

Deliberately **not** required: the individual `test (N, on)` matrix contexts
(version-coupled names, covered by `ci-required`), `Analyze (actions)` and
`Analyze (python)` (CodeQL's per-language runs, rolled up by `CodeQL`), and
`size` (only exists once #238 lands; add it then).

Two behaviours to keep in mind:

- **A job skipped by a job-level `if:` still reports**, with conclusion
  `skipped`, and GitHub treats that as satisfying a required check. So
  `capture + reel (real PostgreSQL)` being required does not mean it always
  really ran — its `if:` includes `github.event_name == 'pull_request'`, so on
  PRs it does. A workflow skipped *entirely* (branch or path filter, or a file
  over the size limit) reports nothing, and the required check stays pending —
  the PR is blocked, which is the behaviour we want.
- **Fork PRs need workflow approval.** With "require approval for all external
  contributors", a fork PR's workflows do not start until a maintainer
  approves them, so the required checks sit pending and the PR cannot be
  merged. That is the hole being closed: today those PRs are mergeable with
  nothing having run.

### `strict_required_status_checks_policy`

Recommended: `false` (as in the payload). `true` requires every PR branch to
be up to date with `main` before merging. With a dozen PRs open against `main`
at once, each merge invalidates all the others and forces a rebase plus a full
PG 14–19 matrix re-run — serialising the queue for little benefit, since the
matrix is a schema/behaviour test rather than a semantic-conflict detector.
Turn it on for a release line if a stale-merge incident ever justifies it.
