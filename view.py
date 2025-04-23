import os
import pandas as pd
import matplotlib.pyplot as plt
import glob


def process_csv(csv_path):
    path_parts = csv_path.split('/')
    config = path_parts[-2]
    metric = os.path.basename(csv_path).replace('.csv', '')

    metric_dir = f'images/{metric}'
    os.makedirs(metric_dir, exist_ok=True)

    df = pd.read_csv(csv_path)

    plt.figure(figsize=(10, 6))

    systems = df.columns[1:]

    for system in systems:
        plt.plot(df['# of threads'], df[system], marker='o', label=system)

    plt.xlabel('# of threads')
    plt.ylabel(metric.replace('-', ' ').capitalize())
    plt.title(f'{metric.capitalize()} for {config}')

    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)

    plt.xticks(df['# of threads'])

    output_file = f'{metric_dir}/{config}.svg'
    plt.savefig(output_file, format='svg')
    plt.close()

    return output_file


os.makedirs('images', exist_ok=True)

csv_files = glob.glob('summary_csv/*/*.csv')

for csv_file in csv_files:
    output_file = process_csv(csv_file)
    print(f"Created {output_file}")

print("All SVG files have been generated in the 'images' directory with metric-specific subdirectories.")
