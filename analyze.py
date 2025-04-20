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


def make_summary_csv(input_dir, output_dir):
    files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    files.sort()
    protocol_data = {}
    for file in files:
        protocol_name = os.path.splitext(file)[0]
        protocol_data[protocol_name] = {"# of threads": [], "Throughput[tps]": [], "Abort rate[%]": [],
                                        "p50 latency[us]": [], "p90 latency[us]": [], "p99 latency[us]": [],
                                        "p999 latency[us]": [], "p9999 latency[us]": []}
        csv_file_path = os.path.join(input_dir, file)
        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                protocol_data[protocol_name]["# of threads"].append(row["# of threads"])
                protocol_data[protocol_name]["Throughput[tps]"].append(row["Throughput[tps]"])
                protocol_data[protocol_name]["Abort rate[%]"].append(row["Abort rate[%]"])
                protocol_data[protocol_name]["p50 latency[us]"].append(row["p50 latency[us]"])
                protocol_data[protocol_name]["p90 latency[us]"].append(row["p90 latency[us]"])
                protocol_data[protocol_name]["p99 latency[us]"].append(row["p99 latency[us]"])
                protocol_data[protocol_name]["p999 latency[us]"].append(row["p999 latency[us]"])
                protocol_data[protocol_name]["p9999 latency[us]"].append(row["p9999 latency[us]"])

    output_file_path = os.path.join(output_dir, "throughput.csv")
    with open(output_file_path, 'w', newline='') as summary_file:
        csv_writer = csv.writer(summary_file)
        csv_writer.writerow(["# of threads"] + list(protocol_data.keys()))
        num_threads = protocol_data[list(protocol_data.keys())[0]]["# of threads"]
        for i in range(len(num_threads)):
            row_data = [num_threads[i]]
            for protocol_name in protocol_data:
                row_data.append(protocol_data[protocol_name]["Throughput[tps]"][i])
            csv_writer.writerow(row_data)
    print(f"Summary: A new CSV file is created: {output_file_path}")

    output_file_path = os.path.join(output_dir, "abort_rate.csv")
    with open(output_file_path, 'w', newline='') as summary_file:
        csv_writer = csv.writer(summary_file)
        csv_writer.writerow(["# of threads"] + list(protocol_data.keys()))
        num_threads = protocol_data[list(protocol_data.keys())[0]]["# of threads"]
        for i in range(len(num_threads)):
            row_data = [num_threads[i]]
            for protocol_name in protocol_data:
                row_data.append(protocol_data[protocol_name]["Abort rate[%]"][i])
            csv_writer.writerow(row_data)
    print(f"Summary: A new CSV file is created: {output_file_path}")

    output_file_path = os.path.join(output_dir, "p50-latency.csv")
    with open(output_file_path, 'w', newline='') as summary_file:
        csv_writer = csv.writer(summary_file)
        csv_writer.writerow(["# of threads"] + list(protocol_data.keys()))
        num_threads = protocol_data[list(protocol_data.keys())[0]]["# of threads"]
        for i in range(len(num_threads)):
            row_data = [num_threads[i]]
            for protocol_name in protocol_data:
                row_data.append(protocol_data[protocol_name]["p50 latency[us]"][i])
            csv_writer.writerow(row_data)
    print(f"Summary: A new CSV file is created: {output_file_path}")

    output_file_path = os.path.join(output_dir, "p90-latency.csv")
    with open(output_file_path, 'w', newline='') as summary_file:
        csv_writer = csv.writer(summary_file)
        csv_writer.writerow(["# of threads"] + list(protocol_data.keys()))
        num_threads = protocol_data[list(protocol_data.keys())[0]]["# of threads"]
        for i in range(len(num_threads)):
            row_data = [num_threads[i]]
            for protocol_name in protocol_data:
                row_data.append(protocol_data[protocol_name]["p90 latency[us]"][i])
            csv_writer.writerow(row_data)
    print(f"Summary: A new CSV file is created: {output_file_path}")

    output_file_path = os.path.join(output_dir, "p99-latency.csv")
    with open(output_file_path, 'w', newline='') as summary_file:
        csv_writer = csv.writer(summary_file)
        csv_writer.writerow(["# of threads"] + list(protocol_data.keys()))
        num_threads = protocol_data[list(protocol_data.keys())[0]]["# of threads"]
        for i in range(len(num_threads)):
            row_data = [num_threads[i]]
            for protocol_name in protocol_data:
                row_data.append(protocol_data[protocol_name]["p99 latency[us]"][i])
            csv_writer.writerow(row_data)
    print(f"Summary: A new CSV file is created: {output_file_path}")

    output_file_path = os.path.join(output_dir, "p999-latency.csv")
    with open(output_file_path, 'w', newline='') as summary_file:
        csv_writer = csv.writer(summary_file)
        csv_writer.writerow(["# of threads"] + list(protocol_data.keys()))
        num_threads = protocol_data[list(protocol_data.keys())[0]]["# of threads"]
        for i in range(len(num_threads)):
            row_data = [num_threads[i]]
            for protocol_name in protocol_data:
                row_data.append(protocol_data[protocol_name]["p999 latency[us]"][i])
            csv_writer.writerow(row_data)
    print(f"Summary: A new CSV file is created: {output_file_path}")

    output_file_path = os.path.join(output_dir, "p9999-latency.csv")
    with open(output_file_path, 'w', newline='') as summary_file:
        csv_writer = csv.writer(summary_file)
        csv_writer.writerow(["# of threads"] + list(protocol_data.keys()))
        num_threads = protocol_data[list(protocol_data.keys())[0]]["# of threads"]
        for i in range(len(num_threads)):
            row_data = [num_threads[i]]
            for protocol_name in protocol_data:
                row_data.append(protocol_data[protocol_name]["p9999 latency[us]"][i])
            csv_writer.writerow(row_data)
    print(f"Summary: A new CSV file is created: {output_file_path}")


if __name__ == "__main__":
    target_dir = "results/"
    output_dir = "results_csv/"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    dirs = [d for d in os.listdir(target_dir) if os.path.isdir(os.path.join(target_dir, d))]
    for dir_name in dirs:
        dir_path = os.path.join(target_dir, dir_name)
        files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]
        for file_name in files:
            if file_name.endswith(".log"):
                input_file = os.path.join(dir_path, file_name)
                output_dir_path = os.path.join(output_dir, dir_name)
                if not os.path.exists(output_dir_path):
                    os.makedirs(output_dir_path)
                output_file = os.path.join(output_dir_path, file_name.replace(".log", ".csv"))
                make_csv(input_file, output_file)
    target_dir = output_dir
    output_dir = "summary_csv/"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    dirs = [d for d in os.listdir(target_dir) if os.path.isdir(os.path.join(target_dir, d))]
    for dir_name in dirs:
        dir_path = os.path.join(target_dir, dir_name)
        output_dir_path = os.path.join(output_dir, dir_name)
        if not os.path.exists(output_dir_path):
            os.makedirs(output_dir_path)
        make_summary_csv(dir_path, output_dir_path)
