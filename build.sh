#!/usr/bin/env bash

errcheck() {
  exitcd=$1
  if [[ "$exitcd" != "0" ]]; then
    exit $exitcd
  fi
}

clean() {
  go clean --cache
  errcheck $?
}

format() {
  go fmt ./...
  errcheck $?
}

compile() {
  go vet ./...
  errcheck $?
  go build ./...
  errcheck $?
}

test() {
  go test -v $(go list ./... | grep -v /benchmark)
  errcheck $?
}

unit() {
  go test -v -run $1 $(go list ./... | grep -v /benchmark)
  errcheck $?
}

cover() {
  mkdir -p coverage
  errcheck $?
  go test -coverprofile=coverage/cover.out $(go list ./... | grep -v /benchmark)
  errcheck $?
  go tool cover -html=coverage/cover.out -o coverage/cover.html
  errcheck $?
}

bench() {
  local dir=$1
  if [[ "$dir" == "" ]]; then
    dir="."
  fi
  pushd $dir
  go test -bench . --benchmem
  errcheck $?
  popd
}

if [[ "$#" == "0" ]]; then
  clean
  format
  compile
  test
  cover

elif [[ "$1" == "unit" ]]; then
  unit $2

elif [[ "$1" == "bench" ]]; then
  bench $2

else
  for a in "$@"; do
    case "$a" in
    clean)
      clean
      ;;
    format)
      format
      ;;
    compile)
      compile
      ;;
    test)
      test
      ;;
    cover)
      cover
      ;;
    '')
      compile
      ;;
    *)
      echo "Bad task: $a"
      exit 1
      ;;
    esac
  done
fi
