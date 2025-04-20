import re
import csv
import os


def make_csv(input_file_path, output_file_path):
    csv_header = ["# of threads", "Throughput[tps]", "Abort rate[%]", "p50 latency[us]", "p90 latency[us]",
                  "p99 latency[us]", "p999 latency[us]", "p9999 latency[us]"]
    with open(input_file_path, 'r') as log_file, open(output_file_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(csv_header)
        log_entry = {}
        for line in log_file:
            line = line.strip()
            if line.startswith("#FLAGS_thread_num:"):
                log_entry["# of threads"] = line.split(":\t")[1].strip()
            elif line.startswith("abort_rate:"):
                log_entry["Abort rate[%]"] = line.split(":\t")[1]
            elif line.startswith("throughput[tps]:"):
                log_entry["Throughput[tps]"] = line.split(":\t")[1]
            elif line.startswith("p50 latency[us]:"):
                log_entry["p50 latency[us]"] = line.split(":\t")[1]
            elif line.startswith("p90 latency[us]:"):
                log_entry["p90 latency[us]"] = line.split(":\t")[1]
            elif line.startswith("p99 latency[us]:"):
                log_entry["p99 latency[us]"] = line.split(":\t")[1]
            elif line.startswith("p99 latency[us]:"):
                log_entry["p99 latency[us]"] = line.split(":\t")[1]
            elif line.startswith("p999 latency[us]:"):
                log_entry["p999 latency[us]"] = line.split(":\t")[1]
            elif line.startswith("p9999 latency[us]:"):
                log_entry["p9999 latency[us]"] = line.split(":\t")[1]
                csv_writer.writerow([log_entry.get(key, "") for key in csv_header])
                log_entry = {}
    print("A new CSV file is created:", output_file_path)


if __name__ == "__main__":
    target_dir = "results/"
    output_dir = "results_csv/"
    dirs = [d for d in os.listdir(target_dir) if os.path.isdir(os.path.join(target_dir, d))]
    for dir_name in dirs:
        dir_path = os.path.join(target_dir, dir_name)
        files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]
        for file_name in files:
            if file_name.endswith(".log"):
                input_file = os.path.join(dir_path, file_name)
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                output_dir_path = os.path.join(output_dir, dir_name)
                if not os.path.exists(output_dir_path):
                    os.makedirs(output_dir_path)
                output_file = os.path.join(output_dir_path, file_name.replace(".log", ".csv"))
                make_csv(input_file, output_file)
