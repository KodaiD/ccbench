import numpy as np
import matplotlib.pyplot as plt
import math
import os
import random

base_directory = './results/'

data_file_prefix = 'latency_'

max_samples_per_file = 10000000

if not os.path.isdir(base_directory):
    print(f"Error: Specified base directory '{base_directory}' not found.")
    exit()


def plot_cdf(data, label):
    sorted_data = np.sort(data)
    n = len(sorted_data)
    if n == 0:
        print(f"Warning: Skipping plot for label '{label}' as data is empty.")
        return

    p = np.arange(n) / n

    y_values = -np.log10(1 - p)

    plt.semilogx(sorted_data, y_values, label=label, linestyle='-')


subdirectories = [d for d in os.listdir(base_directory) if os.path.isdir(os.path.join(base_directory, d))]

if not subdirectories:
    print(f"Warning: No subdirectories found in the base directory '{base_directory}'.")

for subdir_name in subdirectories:
    subdir_path = os.path.join(base_directory, subdir_name)
    print(f"\nProcessing subdirectory '{subdir_name}'...")

    subdirectory_latencies = {}
    all_subdir_latencies_samples = []

    subdir_file_list = [f for f in os.listdir(subdir_path) if
        os.path.isfile(os.path.join(subdir_path, f)) and f.startswith(data_file_prefix)]

    if not subdir_file_list:
        print(
            f"Warning: No data files starting with prefix '{data_file_prefix}' found in subdirectory '{subdir_name}'. Skipping.")
        continue

    for file_name in subdir_file_list:
        file_path = os.path.join(subdir_path, file_name)

        reservoir = []
        total_lines_read = 0

        print(f"  Starting to read file '{file_name}'...")

        try:
            with open(file_path, 'r') as f:
                for line in f:
                    total_lines_read += 1
                    try:
                        latency = float(line.strip())

                        if len(reservoir) < max_samples_per_file:
                            reservoir.append(latency)
                        else:
                            j = random.randint(0, total_lines_read - 1)
                            if j < max_samples_per_file:
                                reservoir[j] = latency

                    except ValueError:
                        pass

            data_samples = reservoir
            all_subdir_latencies_samples.extend(data_samples)

            if data_samples:
                label = os.path.splitext(file_name[len(data_file_prefix):])[0]
                if not label:
                    label = os.path.splitext(file_name)[0]
                subdirectory_latencies[label] = data_samples
                print(
                    f"  Read {len(data_samples)} sample data points from file '{file_name}' (processed {total_lines_read} lines).")
            else:
                print(f"  No valid or sufficient data found in file '{file_name}' (subdirectory '{subdir_name}').")
        except Exception as e:
            print(f"Error: An error occurred while reading file '{file_name}' (subdirectory '{subdir_name}'): {e}")

    if not subdirectory_latencies:
        print(
            f"Warning: No valid latency data matching prefix '{data_file_prefix}' was read from subdirectory '{subdir_name}'. No plot will be generated.")
        continue

    fig, ax = plt.subplots(figsize=(10, 6))
    plt.rcParams["font.size"] = 18
    plt.rcParams['lines.linewidth'] = 3
    fig.tight_layout()

    for label, data in subdirectory_latencies.items():
        plot_cdf(data, label)

    ax.set_xlabel('Latency (ms)')
    ax.set_ylabel('Percentile')

    ax.set_yticks([-math.log10(0.5), 1, 2, 3, 4], ["p50", "p90", "p99", "p999", "p9999"])
    ax.set_ylim([0, 4])

    if all_subdir_latencies_samples:
        min_latency = min(all_subdir_latencies_samples)
        max_latency = max(all_subdir_latencies_samples)
        x_min = max(0.001, min_latency * 0.9 if min_latency > 0 else 0.001)
        x_max = max_latency * 1.1
        if x_min >= x_max:
            x_max = x_min + 1.0
            x_min = max(0.001, x_min * 0.9)

        ax.set_xlim([x_min, x_max])
    else:
        ax.set_xlim([0.001, 10])

    ax.legend()

    for spine in ax.spines.values():
        spine.set_color('black')
    ax.tick_params(axis='x', direction='in', length=10, width=1, colors='black')
    ax.tick_params(axis='y', direction='in', length=10, width=1, colors='black')
    ax.grid(True)

    output_filename = f"cdf_{subdir_name}.svg"
    try:
        plt.savefig(output_filename, format='svg')
        print(f"Saved graph '{output_filename}'.")
    except Exception as e:
        print(f"Error: An error occurred while saving graph '{output_filename}': {e}")

    plt.close(fig)

print("\nFinished processing all subdirectories.")
