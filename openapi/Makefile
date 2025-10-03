.PHONY: all check-cli rust typescript python java golang clean outdir

CLI ?= openapi-generator-cli
SPEC := $(CURDIR)/cas.openapi.yaml
OUT_ROOT := $(CURDIR)/generated

.DEFAULT_GOAL := all

all: rust typescript python java golang

define ensure_cli
	@bash -c 'set -euo pipefail; \
	  if command -v $(CLI) >/dev/null 2>&1; then \
	    echo "$(CLI) found: '$$(command -v $(CLI))'"; \
	    exit 0; \
	  fi; \
	  echo "$(CLI) not found; installing via npm..." >&2; \
	  if ! command -v npm >/dev/null 2>&1; then \
	    echo "npm is not installed. Please install Node.js/npm and re-run." >&2; \
	    exit 1; \
	  fi; \
	  npm install @openapitools/openapi-generator-cli -g; \
	  export PATH="$$PATH:$$(npm bin -g)"; \
	  if ! command -v $(CLI) >/dev/null 2>&1; then \
	    echo "$(CLI) still not found after npm installation. Ensure npm global bin is in PATH." >&2; \
	    exit 1; \
	  fi; \
	  echo "$(CLI) installed: '$$(command -v $(CLI))'";'
endef

check-cli:
	$(ensure_cli)

outdir:
	mkdir -p "$(OUT_ROOT)"

define gen
	@echo "Generating $(2) client -> $(OUT_ROOT)/$(2)"
	@rm -rf "$(OUT_ROOT)/$(2)"
	$(CLI) generate -i "$(SPEC)" -g "$(1)" -o "$(OUT_ROOT)/$(2)" $(3)
endef

rust: check-cli outdir
	$(call gen,rust,rust,--additional-properties=packageName=xet_cas_client,packageVersion=0.1.0,library=reqwest,preferUnsignedInt=true)

typescript: check-cli outdir
	$(call gen,typescript-fetch,typescript,--additional-properties=npmName=@xet/cas-client,npmVersion=0.1.0,typescriptThreePlus=true)

python: check-cli outdir
	$(call gen,python,python,--additional-properties=packageName=xet_cas_client,projectName=xet_cas_client,packageVersion=0.1.0)

java: check-cli outdir
	$(call gen,java,java,--additional-properties=artifactId=xet-cas-client,groupId=ai.huggingface.xet,artifactVersion=0.1.0,library=webclient,datetimeLibrary=java8)

golang: check-cli outdir
	$(call gen,go,golang,--additional-properties=packageName=casclient,enumClassPrefix=true,isGoSubmodule=false,withGoCodegenComment=true)

clean:
	rm -rf "$(OUT_ROOT)"

