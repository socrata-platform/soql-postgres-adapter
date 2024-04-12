set positional-arguments

export JUSTFILE := justfile()

[private]
@default:
    just --list --justfile="$JUSTFILE"

build:
    m4 < define_functions.sql.m4 > define_functions.sql

test *args:
    cargo test "$@"
