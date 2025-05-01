#!/usr/bin/env python3
import os
import subprocess
import shutil


def main():
    print("Start TPC-C benchmark")

    if os.path.exists("results"):
        shutil.rmtree("results")
    os.makedirs("results", exist_ok=True)
    dest = os.path.join(os.getcwd(), "results")

    protocols = ["silo", "silo2", "polaris"]
    threads = [1, 4, 8]
    num_sec = 5
    no_wait = [1]
    polaris_t = 16
    polaris_s = 4
    silo2_threshold = 1

    # protocols = ["silo", "silo2", "polaris"]
    # threads = [1, 16, 32, 48, 64]
    # num_sec = 30
    # no_wait = [1]
    # polaris_t = 16
    # polaris_s = 4
    # silo2_threshold = 1

    for protocol in protocols:
        for n in no_wait:
            dir_path = f"{dest}"
            os.makedirs(dir_path, exist_ok=True)

            if protocol == "silo" or protocol == "silo2" or protocol == "polaris":
                log_file = f"{dir_path}/{protocol}-nowait{n}.log"
            else:
                log_file = f"{dir_path}/{protocol}.log"

            with open(log_file, "w") as f:
                f.write(f"Benchmark results for protocol={protocol}, "
                        f"no_wait={n}\n")
                f.write("=" * 80 + "\n\n")

            for thread in threads:
                num_wh = thread

                if protocol == "silo" or protocol == "silo2" or protocol == "polaris":
                    cmd = [f"./build/{protocol}/tpcc_{protocol}.exe", f"--extime={num_sec}",
                           f"--thread-num={thread}", f"--no_wait={n}"]
                else:
                    cmd = [f"./build/{protocol}/tpcc_{protocol}.exe", f"--extime={num_sec}",
                           f"--thread-num={thread}"]

                if protocol == "polaris":
                    cmd += [f"--polaris_t={polaris_t}", f"--polaris_s={polaris_s}"]
                elif protocol == "silo2":
                    cmd += [f"--threshold={silo2_threshold}"]

                cmd += [f"--tpcc_num_wh={num_wh}"]

                cmd_str = " ".join(cmd)
                print(f"{cmd_str} {log_file}")

                with open(log_file, "a") as f:
                    f.write(f"\n--- Thread Count: {thread} ---\n")
                    f.write(f"Executing: {cmd_str}\n")

                try:
                    with open(log_file, "a") as f:
                        result = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, check=True)
                except subprocess.CalledProcessError:
                    error_msg = f"Error: Command failed - {cmd_str}"
                    print(error_msg)
                    with open(log_file, "a") as f:
                        f.write(f"{error_msg}\n")


if __name__ == "__main__":
    main()
