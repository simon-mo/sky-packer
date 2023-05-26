list:
    @just --list

test:
    #!/usr/bin/env bash
    rm -rf ./test-files/untar-stage || true
    mkdir -p ./test-files/untar-stage
    cat ./test-files/my-dir.tar | cargo run -- --compression none --split-to ./test-files/untar-stage/split.tar --split-size 5M
    echo
    for f in ./test-files/untar-stage/split.tar*; do
        ls -lh $f
        tar -tf $f
        echo
    done