# demos/Makefile — orchestrate the pg_ash investigation demo GIF.
#
#   make record    Full pipeline: docker up → pg_ash install → workload →
#                  tmux/asciinema capture → agg render → demos/ash_demo.gif
#   make clean     Tear down the container, remove .cast + .gif
#   make open      Open the produced GIF (macOS + Linux best-effort)
#
# See demos/README.md for prerequisites.

HERE := $(shell cd "$(dir $(lastword $(MAKEFILE_LIST)))" && pwd)
GIF  := $(HERE)/ash_demo.gif
CAST := $(HERE)/ash_demo.cast

.PHONY: record clean open check

record: check
	bash $(HERE)/record.sh

clean:
	bash $(HERE)/record.sh clean

open:
	@if [ ! -f $(GIF) ]; then echo "no $(GIF) — run 'make record' first"; exit 1; fi
	@case "$$(uname -s)" in \
	  Darwin) open $(GIF) ;; \
	  Linux)  xdg-open $(GIF) 2>/dev/null || echo "install xdg-utils or open $(GIF) manually" ;; \
	  *)      echo "open $(GIF) manually" ;; \
	esac

check:
	@for t in docker tmux asciinema agg python3; do \
	  command -v $$t >/dev/null 2>&1 || { echo "missing: $$t (see demos/README.md)"; exit 1; }; \
	done
	@command -v gifsicle >/dev/null 2>&1 || echo "note: gifsicle not installed — final GIF will be larger (still usable)"
	@echo "prerequisites ok"
