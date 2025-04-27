import os
import pandas as pd
import matplotlib.pyplot as plt
import re

csv_dir = 'summary_csv/'
output_dir = 'images_polaris/'

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

processed_files_count = 0

for dirpath, dirnames, filenames in os.walk(csv_dir):
    for filename in filenames:
        if filename.endswith('.csv'):
            filepath = os.path.join(dirpath, filename)
            print(f"Processing {filepath}...")

            try:
                df = pd.read_csv(filepath, header=0)

                if df.empty:
                    print(f"Warning: {filename} is empty. Skipping.")
                    continue

                if len(df) > 0:
                    data_row = df.iloc[0]
                else:
                    print(f"Warning: No data rows found in {filename}. Skipping.")
                    continue

                if '# of threads' in data_row:
                    num_threads = data_row['# of threads']
                else:
                    num_threads = 'N/A'

                plot_data = data_row.drop('# of threads', errors='ignore')


                # 's' の値を抽出してソートキーとする
                def extract_s_value(label):
                    match = re.search(r'-s(\d+)-', label)
                    if match:
                        return int(match.group(1))
                    return -1  # 抽出できない場合は負の値などでソート順を最後にする


                # データのインデックスを 's' の値でソート
                sorted_indices = sorted(plot_data.index, key=extract_s_value)
                sorted_plot_data = plot_data.reindex(sorted_indices)

                plt.figure(figsize=(15, 7))

                # ソート済みのデータをプロット
                sorted_plot_data.plot(kind='bar')

                plt.title(
                    f'Performance metrics for # of threads = {num_threads}\n(Data from {os.path.relpath(filepath, csv_dir)})')
                plt.xlabel('s value')
                plt.ylabel('Metric Value')

                # ソート済みのデータのインデックスから 's' の値を再抽出してラベルとする
                s_values_sorted = [str(extract_s_value(label)) if extract_s_value(label) != -1 else '' for label in
                                   sorted_plot_data.index]

                plt.xticks(range(len(s_values_sorted)), s_values_sorted, rotation=90, ha='right')

                plt.tight_layout()

                relative_dir = os.path.relpath(dirpath, csv_dir)
                output_subdir = os.path.join(output_dir, relative_dir)

                if not os.path.exists(output_subdir):
                    os.makedirs(output_subdir)

                output_filename = f"{os.path.splitext(filename)[0]}_graph.png"
                output_filepath = os.path.join(output_subdir, output_filename)

                plt.savefig(output_filepath)
                print(f"Saved graph to {output_filepath}")

                plt.close()
                processed_files_count += 1


            except FileNotFoundError:
                print(f"Error: {filepath} not found. Skipping.")
            except pd.errors.EmptyDataError:
                print(f"Warning: {filepath} is empty or does not contain valid data. Skipping.")
            except Exception as e:
                print(f"An error occurred while processing {filepath}: {e}")

if processed_files_count == 0:
    print(f"No CSV files found for processing in {csv_dir} or its subdirectories.")
else:
    print(f"Finished processing {processed_files_count} CSV file(s).")
