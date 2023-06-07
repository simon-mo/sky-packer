list:
    @just --list

make-test-tar:
    cd ./test-files/ && tar cf my-dir.tar my-dir

build-release:
    cargo build --release

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

unpack:
    rm -rf ./test-files/unpacked || true
    cargo run -- --compression none --unpack-from ./test-files/untar-stage --unpack-to ./test-files/unpacked

verify:
    md5sum ./test-files/unpacked/my-dir/20M.file
    md5sum ./test-files/my-dir/20M.file

    md5sum ./test-files/unpacked/my-dir/10M.file
    md5sum ./test-files/my-dir/10M.file

tree:
    @echo
    @tree --du -h test-files/my-dir
    @echo
    @tree --du -h test-files/untar-stage
    @echo
    @tree --du -h test-files/unpacked/my-dir
    @echo

generate-image:
    mkdir -p /tmp/sky-uploader-split
    # python generate-split.py --docker-tag localhost:5001/image --split-size=50M --cache-location /tmp/sky-uploader-split
    python generate-split.py --docker-tag vicuna:latest --split-size=500M --cache-location /tmp/sky-uploader-split