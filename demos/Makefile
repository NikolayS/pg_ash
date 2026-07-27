# demos/Makefile — the only entry point.
#
# Everything here is a thin wrapper around bin/ash-demo, which is a thin wrapper
# around lib/. If you find yourself adding logic to this file, it belongs in
# lib/ where it can be tested.
#
# GNU make 3.81 (the macOS system make) and GNU make 4.x both work. No
# .ONESHELL, no $(shell) at parse time, no bashisms in recipes.

SHELL := /bin/bash
ASH_DEMO := ./bin/ash-demo

.DEFAULT_GOAL := help
.PHONY: help doctor up seed preflight capture stills demo render all check \
        fixtures rot clean down open scenes

## help: what each target does
help:
	@echo 'pg_ash demo harness — regenerate every image and the reel from real data.'
	@echo
	@echo '  make doctor     probe dependencies by tier; says exactly what is missing'
	@echo '  make up         create/reach the demo database and install pg_ash'
	@echo '  make seed       up + seed a frozen incident window (~20s); asserts its shape'
	@echo '  make preflight  run every scene scripted and gate it on the column budget'
	@echo '  make capture    seed + preflight + scripted capture -> out/raw, out/ansi'
	@echo '  make stills     capture + render -> assets/*.svg (+ *.png)'
	@echo '  make demo       seed + record the reel -> assets/ash_demo.gif + .mp4'
	@echo '  make render     re-render the LAST recording (skips the 75s tmux pass)'
	@echo '  make all        stills + demo, from ONE seed and ONE frozen window'
	@echo '  make check      NO DATABASE: re-render the committed fixtures and diff'
	@echo '  make rot        did a reader change its columns? (needs a fresh capture)'
	@echo '  make fixtures   refresh fixtures/ from the current capture (manual, reviewable)'
	@echo '  make clean      remove out/'
	@echo '  make down       drop the demo database / remove the container'
	@echo '  make open       open the produced GIF (macOS)'
	@echo
	@echo 'What `make check` proves, and what it does not:'
	@echo '  It proves the RENDERER still turns known bytes into the same SVG.'
	@echo '  It CANNOT notice that those bytes no longer reflect what pg_ash does —'
	@echo '  a renamed reader or a moved column keeps the fixtures green.'
	@echo '  `make rot` is the other half: it re-captures against a real database'
	@echo '  and compares the READER CONTRACT (column names, count, report keys)'
	@echo '  with fixtures/shape.tsv. Measured values are excluded, because they'
	@echo '  legitimately change on every seed — a check that is always red is a'
	@echo '  check nobody reads.'
	@echo
	@echo 'Useful knobs (all documented in lib/env.sh):'
	@echo '  ASH_BACKEND=local|docker|remote   default local; no Docker required'
	@echo '  ASH_SKIP_SEED=1                   reuse a warm seed — the fast iteration loop'
	@echo '  ASH_SVG_ONLY=1                    skip PNG/GIF rasterisation'
	@echo '  ASH_REAL_TIME=1                   no time compression (slow, honest)'
	@echo '  ASH_COLS=100 ASH_VMIN=24 ASH_SPM=12'

## doctor: dependency probe by tier + which backends work here
doctor:
	@$(ASH_DEMO) doctor

## up: database + pg_ash
up:
	@$(ASH_DEMO) up

## seed: the frozen incident window
seed:
	@$(ASH_DEMO) seed

## preflight: the hard column-budget gate, on live data
preflight:
	@$(ASH_DEMO) preflight

## capture: scripted capture of every scene, no rendering
capture:
	@$(ASH_DEMO) capture

## stills: assets/*.svg (+ *.png). Rewrites assets/ WHOLESALE.
stills:
	@$(ASH_DEMO) stills

## demo: assets/ash_demo.gif + .mp4
demo:
	@$(ASH_DEMO) demo

## render: redo agg -> chrome -> gif/mp4 from the cast that is already on disk.
#
# The tmux recording is 75 of the ~155 seconds `make demo` costs, and it is the
# part that does NOT change when you adjust the theme, the chrome plate, the
# palette or the size budget. This target is how those get iterated on.
render:
	@./bin/record-demo.sh --render-only

## all: both halves, from ONE seed and ONE frozen window.
#
# The ordering matters and the ASH_SKIP_SEED=1 on the second half is the whole
# point: `demo` must NOT reseed, or the reel would show different numbers from
# the stills and the frozen-window contract would be decorative.
all:
	@$(ASH_DEMO) stills
	@ASH_SKIP_SEED=1 $(ASH_DEMO) demo

## check: no database, no browser, no ffmpeg — just python3 + fontTools + brotli
check:
	@$(ASH_DEMO) check

## fixtures: refresh the frozen renderer inputs and outputs
#
# Depends on `capture`, deliberately: refreshing a fixture from a stale
# out/ansi/ freezes the previous scene list and the drift alarm goes quiet
# exactly when it should be loudest.
fixtures: capture
	@$(ASH_DEMO) fixtures

## scenes: print the parsed scene table (debugging)
scenes:
	@$(ASH_DEMO) scenes

## rot: has a reader's contract changed since the fixtures were taken?
#
# Needs a fresh capture (`make capture` or `make stills`) and fixtures/shape.tsv.
# This is the check `make check` cannot make: `check` proves the RENDERER still
# works on frozen bytes, `rot` proves the BYTES still have the shape they had.
rot:
	@./bin/rot-check.sh

## clean: remove the working directory
clean:
	@rm -rf out
	@echo 'removed demos/out'

## down: drop the database / remove the container
down:
	@$(ASH_DEMO) down

## open: look at the result (macOS convenience, kept from the original harness)
open:
	@os=$$(uname -s); \
	if [ "$$os" != "Darwin" ]; then \
	  echo "make open is macOS-only (detected $$os)" >&2; \
	  exit 1; \
	elif [ ! -f ../assets/ash_demo.gif ]; then \
	  echo 'no ../assets/ash_demo.gif yet — run `make demo`' >&2; \
	  exit 1; \
	fi; \
	open ../assets/ash_demo.gif
