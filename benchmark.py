#!/usr/bin/env python3
import os
import subprocess
import shutil


def main():
    print("Start benchmark")

    if os.path.exists("results"):
        shutil.rmtree("results")
    os.makedirs("results", exist_ok=True)
    dest = os.path.join(os.getcwd(), "results")

    protocols = ["silo", "silo2", "polaris"]
    threads = [1, 16, 32, 48, 64]
    read_rates = [0, 1, 10, 50, 90, 99, 100]
    num_recs = [1000, 1000000]
    num_ops = 10
    num_sec = 5
    skews = [0, 0.99]
    no_wait = [0, 1]

    for protocol in protocols:
        for rratio in read_rates:
            for num_rec in num_recs:
                for skew in skews:
                    for n in no_wait:
                        dir_path = f"{dest}/ycsb-r{rratio}-{num_rec}records-{num_ops}ops-skew{skew}"
                        os.makedirs(dir_path, exist_ok=True)

                        if protocol == "silo" or protocol == "silo2":
                            log_file = f"{dir_path}/{protocol}-nowait{n}.log"
                        else:
                            log_file = f"{dir_path}/{protocol}.log"

                        with open(log_file, "w") as f:
                            f.write(
                                f"Benchmark results for protocol={protocol}, read_rate={rratio}, records={num_rec}, "
                                f"operations={num_ops}, skew={skew}, no_wait={n}\n")
                            f.write("=" * 80 + "\n\n")

                        for thread in threads:

                            print_latencies = 0

                            if protocol == "silo" or protocol == "silo2":
                                cmd = [f"./build/{protocol}/ycsb_{protocol}.exe", f"--ycsb-rratio={rratio}",
                                       f"--extime={num_sec}", f"--ycsb-max-ope={num_ops}",
                                       f"--ycsb-tuple-num={num_rec}", f"--print-latencies={print_latencies}",
                                       f"--ycsb-zipf_skew={skew}", f"--thread-num={thread}", f"--no_wait={n}"]
                            else:
                                cmd = [f"./build/{protocol}/ycsb_{protocol}.exe", f"--ycsb-rratio={rratio}",
                                       f"--extime={num_sec}", f"--ycsb-max-ope={num_ops}",
                                       f"--ycsb-tuple-num={num_rec}", f"--print-latencies={print_latencies}",
                                       f"--ycsb-zipf_skew={skew}", f"--thread-num={thread}"]

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
