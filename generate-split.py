import subprocess
import time
import click
import json
import sys

packer_binary = "/home/ubuntu/workspace/target/release/sky-packer"


@click.command()
@click.option("--docker-tag", help="Docker tag to use for the image", required=True)
@click.option("--split-size", help="Size of the split in human readable format", required=True)
@click.option("--cache-location", help="Location of the cache", required=True)
def main(docker_tag: str, split_size: str, cache_location: str):
    # run docker image inspect to get the output
    docker_image_inspect = subprocess.run(["docker", "image", "inspect", docker_tag], capture_output=True)
    image_metadata = json.loads(docker_image_inspect.stdout)
    graph_drivers = image_metadata[0]["GraphDriver"]["Data"]
    dirs_to_split = [graph_drivers["UpperDir"]] + graph_drivers["LowerDir"].split(":")
    print(dirs_to_split)

    procs = []
    for d in dirs_to_split:
        dir_tag = d.split("/")[-2]
        tar_process = subprocess.Popen(
            [
                "bash",
                "-c",
                f"sudo tar cfP - -C {d} . | sudo {packer_binary} --compression=zstd --split-size={split_size} --split-to={cache_location}/{dir_tag} --tar-source-from={d}",
            ],
            # stdout=subprocess.PIPE,
            # stderr=subprocess.PIPE,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        procs.append(tar_process)

    for p in procs:
        p.wait()

    # stream and multiplex the output from procs
    # while True:
    #     for p in procs:
    #         out, err = (p.stdout.read(), p.stderr.read())
    #         if not out and not err:
    #             continue
    #         sys.stdout.buffer.write(out)
    #         sys.stderr.buffer.write(err)
    #         sys.stdout.flush()
    #         sys.stderr.flush()

    #     if all(p.poll() is not None for p in procs):
    #         break

    #     time.sleep(0.5)


if __name__ == "__main__":
    main()
