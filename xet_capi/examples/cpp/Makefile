# Build the C++ example against the hf_xet C API.
#
#   make        # build ./upload_download
#   make run    # build and run (requires $HF_TOKEN)
#   make clean

ROOT    := ../../..
INCLUDE := ../../include
LIBDIR  := $(ROOT)/target/release

CXX      ?= c++
CXXFLAGS += -std=c++17 -Wall -Wextra -I$(INCLUDE)
LDFLAGS  += -L$(LIBDIR) -lxet_capi -Wl,-rpath,$(LIBDIR)

BIN := upload_download

$(BIN): upload_download.cpp xet_session.hpp $(INCLUDE)/hf_xet.h
	$(CXX) $(CXXFLAGS) upload_download.cpp -o $@ $(LDFLAGS)

.PHONY: run clean
run: $(BIN)
	./$(BIN)

clean:
	rm -f $(BIN) downloaded.bin
