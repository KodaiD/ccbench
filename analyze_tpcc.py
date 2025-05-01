import re
import csv
import os


def make_csv(input_file_path, output_file_path):
    csv_header = ["# of threads", "Throughput[tps]", "Abort rate[%]"]
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
                csv_writer.writerow([log_entry.get(key, "") for key in csv_header])
                log_entry = {}
    print("A new CSV file is created:", output_file_path)


def make_summary_csv(input_dir, output_dir):
    files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    files.sort()
    protocol_data = {}
    for file in files:
        protocol_name = os.path.splitext(file)[0]
        protocol_data[protocol_name] = {"# of threads": [], "Throughput[tps]": [], "Abort rate[%]": []}
        csv_file_path = os.path.join(input_dir, file)
        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                protocol_data[protocol_name]["# of threads"].append(row["# of threads"])
                protocol_data[protocol_name]["Throughput[tps]"].append(row["Throughput[tps]"])
                protocol_data[protocol_name]["Abort rate[%]"].append(row["Abort rate[%]"])

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

if __name__ == "__main__":
    target_dir = "results/"
    output_dir = "results_csv/"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    files = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]
    for file_name in files:
        if file_name.endswith(".log"):
            input_file = os.path.join(target_dir, file_name)
            output_file = os.path.join(output_dir, file_name.replace(".log", ".csv"))
            make_csv(input_file, output_file)

    target_dir = output_dir
    output_dir = "summary_csv/"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_dir_path = os.path.join(output_dir, target_dir)
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    make_summary_csv(target_dir, output_dir_path)
