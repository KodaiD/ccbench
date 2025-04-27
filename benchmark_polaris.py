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

    protocols = ["polaris"]
    threads = [8]
    read_rates = [50]
    num_recs = [1000]
    num_ops = 10
    num_sec = 1
    skews = [0.99]
    no_wait = [1]
    polaris_t_array = [0]
    polaris_s_array = [1, 2, 4, 8, 16, 24, 32, 40, 48, 56, 64]

    # protocols = ["polaris"]
    # threads = [64]
    # read_rates = [50]
    # num_recs = [1000000]
    # num_ops = 10
    # num_sec = 5
    # skews = [0.99]
    # no_wait = [1]
    # polaris_t_array = [0]
    # polaris_s_array = [1, 2, 4, 8, 16, 32, 64]

    for protocol in protocols:
        for rratio in read_rates:
            for num_rec in num_recs:
                for skew in skews:
                    for n in no_wait:
                        for polaris_t in polaris_t_array:
                            for polaris_s in polaris_s_array:
                                dir_path = f"{dest}/ycsb-r{rratio}-{num_rec}records-{num_ops}ops-skew{skew}"
                                os.makedirs(dir_path, exist_ok=True)

                                log_file = f"{dir_path}/{protocol}-t{polaris_t}-s{polaris_s}-nowait{n}.log"

                                with open(log_file, "w") as f:
                                    f.write(
                                        f"Benchmark results for protocol={protocol}, read_rate={rratio}, records={num_rec}, "
                                        f"operations={num_ops}, skew={skew}, polaris_t={polaris_t}, polaris_s={polaris_s}, no_wait={n}\n")
                                    f.write("=" * 80 + "\n\n")

                                for thread in threads:

                                    print_latencies = 0

                                    cmd = [f"./build/{protocol}/ycsb_{protocol}.exe", f"--ycsb-rratio={rratio}",
                                           f"--extime={num_sec}", f"--ycsb-max-ope={num_ops}",
                                           f"--ycsb-tuple-num={num_rec}", f"--print-latencies={print_latencies}",
                                           f"--ycsb-zipf_skew={skew}", f"--thread-num={thread}", f"--polaris_t={polaris_t}",
                                           f"--polaris_s={polaris_s}", f"--no_wait={n}"]

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
