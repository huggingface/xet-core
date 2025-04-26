#!/usr/bin/env bash

# With these, Log the filename, function name, and line number when showing where we're executing.
set -o xtrace
export PS4='+($(basename ${BASH_SOURCE}):${LINENO}): ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'

die() {
  echo >&2 "ERROR:>>>>> $1 <<<<<"
  return 1
}
export -f die

# support both Mac OS and Linux for these scripts
if hash md5 2>/dev/null; then
  checksum() {
    md5 -q $1
  }
  checksum_string() {
    echo $1 | md5 -q
  }
else
  checksum() {
    md5sum $1 | head -c 32
  }
  checksum_string() {
    echo $1 | md5sum | head -c 32
  }
fi

export -f checksum
export -f checksum_string

create_data_file() {
  f="$1"
  len=$2

  printf '\xff' >$f # Start with this to ensure utf-8 encoding fails quickly.
  cat /dev/random | head -c $(($2 - 1)) >>$f
  echo $(checksum $f)
}
export -f create_data_file

append_data_file() {
  f="$1"
  len=$2

  printf '\xff' >>$f # Start with this to ensure utf-8 encoding fails quickly.
  cat /dev/random | head -c $(($2 - 1)) >>$f
  echo $(checksum $f)
}
export -f append_data_file

assert_files_equal() {
  # Use fastest way to determine content equality.
  cmp --silent $1 $2 || die "Assert Failed: Files $1 and $2 not equal."
}
export -f assert_files_equal

assert_files_not_equal() {
  # Use fastest way to determine content equality.
  cmp --silent $1 $2 && die "Assert Failed: Files $1 and $2 should not be equal." || echo >&2 "Files $1 and $2 not equal."
}
export -f assert_files_not_equal

assert_is_pointer_file() {
  file=$1
  match=$(cat $file | grep -F 'hash' | grep -F "file_size" || echo "")
  [[ ! -z "$match" ]] || die "File $file does not appear to be a pointer file."
}
export -f assert_is_pointer_file

assert_pointer_file_size() {
  file=$1
  size=$2

  assert_is_pointer_file $file

  filesize=$(cat $file | grep -F file_size | sed -E 's|.*\"file_size\":([0-9]+).*|\1|' || echo "")

  [[ $filesize == $size ]] || die "Pointer file $file gives incorrect size; $filesize, expected $size."
}
export -f assert_pointer_file_size

pseudorandom_stream() {
  key=$1

  while true; do
    key=$(checksum_string $key)
    echo "$(echo $key | xxd -r -p)" 2>/dev/null || exit 0
  done
}
export -f pseudorandom_stream

create_csv_file() {
  local set_x_status=$([[ "$-" == *x* ]] && echo 1)
  set +x

  csv_file="$1"
  key="$2"
  n_lines="$3"
  n_repeats="${4:-1}"
  n_lines_p_1=$((n_lines + 1))

  pseudorandom_stream "$key" | hexdump -v -e '5/1 "%02x""\n"' |
    awk -v OFS='\t' 'NR == 1 { print "foo", "bar", "baz" }
    { print "S"substr($0, 1, 4), substr($0, 5, 2), substr($0, 7, 2)"."substr($0, 9, 1), 6, 3}' |
    head -n $((n_lines + 1)) | tr 'abcdef' '123456' >$csv_file.part

  cat $csv_file.part >$csv_file

  for i in {0..n_repeats}; do
    tail -n $n_lines $csv_file.part >>$csv_file
  done

  rm $csv_file.part
  [[ $set_x_status != "1" ]] || set -x
}
export -f create_csv_file

create_random_csv_file() {
  f="$1"
  n_lines="$2"
  n_repeats="${3:-1}"
  n_lines_p_1=$((n_lines + 1))

  cat /dev/random | hexdump -v -e '5/1 "%02x""\n"' |
    awk -v OFS='\t' 'NR == 1 { print "foo", "bar", "baz" }
    { print "S"substr($0, 1, 4), substr($0, 5, 2), substr($0, 7, 2)"."substr($0, 9, 1), 6, 3}' |
    head -n $((n_lines + 1)) | tr 'abcdef' '123456' >$f.part

  cat $f.part >$f

  for i in {0..n_repeats}; do
    tail -n $n_lines $f.part >>$f
  done

  rm $f.part
}
export -f create_random_csv_file

create_text_file() {
  local set_x_status=$([[ "$-" == *x* ]] && echo 1)
  set +x

  text_file="$1"
  key="$2"
  n_lines="$3"
  n_repeats="${4:-1}"

  create_csv_file "$text_file.temp" "$key" "$n_lines" "$n_repeats"

  cat "$text_file.temp" | tr ',0123456789' 'ghijklmnopq' >$text_file
  rm "$text_file.temp"
  [[ $set_x_status != "1" ]] || set -x
}
export -f create_text_file

random_tag() {
  cat /dev/random | head -c 64 | checksum_string
}
export -f random_tag

raw_file_size() {
  if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    stat --printf="%s" $1
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    stat -f%z $1
  fi
}
