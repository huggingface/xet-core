# Build the C example against the hf_xet C API.
#
# It links the self-contained shared library (target/release/libxet_capi.dylib
# on macOS, .so on Linux); no need to pull in Rust's transitive native deps.
#
#   make        # build ./upload_download
#   make run    # build and run (requires $HF_TOKEN)
#   make clean

# Repo root, relative to this directory (xet_capi/examples/c).
ROOT    := ../../..
INCLUDE := ../../include
LIBDIR  := $(ROOT)/target/release

CC      ?= cc
CFLAGS  += -std=c11 -Wall -Wextra -I$(INCLUDE)
LDFLAGS += -L$(LIBDIR) -lxet_capi -Wl,-rpath,$(LIBDIR)

BIN := upload_download

$(BIN): upload_download.c $(INCLUDE)/hf_xet.h
	$(CC) $(CFLAGS) upload_download.c -o $@ $(LDFLAGS)

.PHONY: run clean
run: $(BIN)
	./$(BIN)

clean:
	rm -f $(BIN) downloaded.bin
