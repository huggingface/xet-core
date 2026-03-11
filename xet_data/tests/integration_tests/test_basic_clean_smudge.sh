#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]:-$0}")" &>/dev/null && pwd 2>/dev/null)"
. "$SCRIPT_DIR/initialize.sh"

# Test small binary file clean & smudge
create_data_file small.dat 1452

x clean -d small.pft small.dat
assert_is_pointer_file small.pft
assert_pointer_file_size small.pft 1452

x smudge -f small.pft small.dat.2
assert_files_equal small.dat small.dat.2

# Test big binary file clean & smudge
create_data_file large.dat 4621684 # 4.6 MB

x clean -d large.pft large.dat
assert_is_pointer_file large.pft
assert_pointer_file_size large.pft 4621684

x smudge -f large.pft large.dat.2
assert_files_equal large.dat large.dat.2

# Test small text file clean
create_text_file small.txt key1 100 1

x clean -d small.pft small.txt
assert_is_pointer_file small.pft
x smudge -f small.pft small.txt.2
assert_files_equal small.txt small.txt.2
