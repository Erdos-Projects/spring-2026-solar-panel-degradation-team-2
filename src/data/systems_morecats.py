'''Download the main metadata source,
clean up the metadata a little bit,
and determine which solar installations are
(likely to) have useful data.'''

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import json

# prepare for future pandas 3.0 usage
pd.options.mode.copy_on_write = True


def search_for_fragment(df: pd.DataFrame, fragment: str):
    fragment = fragment.lower()
    pre_subscript = '_'+fragment
    return df[
        (df.loc[:, 'sensor_name'].str.contains(fragment, case=False))
        | (df.loc[:, 'common_name'].str.contains(fragment, case=False))
        | (df.loc[:, 'common_name'].str.contains(fRagment))
    ]


if __name__ == '__main__':
    # reload data
    permanent_systems_cleaned_path = Path(
        '../../data/core/systems_cleaned.csv'
    )
    systems_cleaned = pd.read_csv(permanent_systems_cleaned_path)
    num_sources = systems_cleaned.shape[0]
    # put starting date as date type
    systems_cleaned['first_timestamp']\
        = systems_cleaned['first_timestamp'].astype('datetime64[s]')
    # add the new columns
    systems_cleaned.loc[:, 'has_power_data']\
        = pd.Series([False]*num_sources, dtype='boolean')
    systems_cleaned.loc[:, 'has_ambient_temp_data']\
        = pd.Series([False]*num_sources, dtype='boolean')
    systems_cleaned.loc[:, 'has_some_temp_data']\
        = pd.Series([False]*num_sources, dtype='boolean')
    systems_id_set = set(systems_cleaned['system_id'].unique())
    print("Proceeding to load data from prize data.")
    # by manual inspection, there are 5 sites in the prize data,
    prize_system_ids = [2105, 2107, 7333, 9068, 9069]
    for system_id in prize_system_ids:
        # load the data
        metadata_filepath = Path(
                '../../data/raw/prize-metadata/'
                + f'{system_id}_system_metadata.json'
            )
        with open(metadata_filepath) as json_reader:
            local_metadata = json.load(json_reader)
            system_metrics = local_metadata['Metrics']
            relevant_rows = systems_cleaned.loc[
                systems_cleaned.loc[:, 'system_id'] == system_id
            ]
            for ind in relevant_rows.index:
                systems_cleaned.loc[ind, 'is_prize_data'] = True
                for key in system_metrics.keys():
                    # Power
                    if ('pow' in key) or ('Pow' in key):
                        systems_cleaned.loc[ind, 'has_power_data'] = True
                    if ('mbient' in key):
                        systems_cleaned.loc[ind,
                                            'has_ambient_temp_data'] = True
                    if ('temp' in key) or ('Temp' in key):
                        systems_cleaned.loc[ind,
                                            'has_some_temp_data'] = True

    print("Proceeding to load data from parquet data.")
    metrics_dir = Path("../../data/raw/parquet-metrics/")
    metrics_pq = pq.ParquetDataset(metrics_dir)
    metrics_df = metrics_pq.read().to_pandas()
    parquet_metrics_set = set(metrics_df['system_id'].unique())
    # We first look for power
    metrics_with_pow = search_for_fragment(metrics_df, 'pow')
    parquet_pow_set = set(metrics_with_pow['system_id'].unique())
    for system_id in parquet_pow_set.intersection(systems_id_set):
        sys_relevant_rows = systems_cleaned.loc[
            systems_cleaned.loc[:, 'system_id'] == system_id
        ]
        for ind in sys_relevant_rows.index:
            systems_cleaned.loc[ind, 'has_pow_data'] = True
    metrics_with_ambient = search_for_fragment(metrics_df, 'ambient')
    parquet_amb_set = set(metrics_with_ambient['system_id'].unique())
    for system_id in parquet_amb_set.intersection(systems_id_set):
        sys_relevant_rows = systems_cleaned.loc[
            systems_cleaned.loc[:, 'system_id'] == system_id
        ]
        for ind in sys_relevant_rows.index:
            systems_cleaned.loc[ind, 'has_ambient_temp_data'] = True
    metrics_with_temp = search_for_fragment(metrics_df, 'temp')
    parquet_temp_set = set(metrics_with_temp['system_id'].unique())
    for system_id in parquet_temp_set.intersection(systems_id_set):
        sys_relevant_rows = systems_cleaned.loc[
            systems_cleaned.loc[:, 'system_id'] == system_id
        ]
        for ind in sys_relevant_rows.index:
            systems_cleaned.loc[ind, 'has_some_temp_data'] = True
    print("Proceeding to load metadata from csv data.")
    # We begin by downloading metadata.
    # So let's drop it!
    csv_metadata_dir = Path('../../data/raw/csv_metadata/')

    # now grab the json files, infer the system_id, and
    # check for metadata
    jsons = csv_metadata_dir.glob("*_system_metadata.json")
    for file_path in jsons:
        system_id = int(
            f'{file_path}'.replace('_system_metadata.json', '')
        )
        with open(file_path) as reader:
            local_metadata = json.load(file_path)
            metrics = local_metadata['Metrics']
            relevant_rows = systems_cleaned.loc[
                systems_cleaned.loc[:, 'system_id'] == system_id
            ]
            for ind in relevant_rows.index:
                systems_cleaned.loc[ind, 'is_lake_csv_data'] = True
                for key in metrics.keys():
                    if ('pow' in key) or ('Pow' in key):
                        systems_cleaned.loc[ind, 'has_power_data'] = True
                    if ('mbient' in key):
                        systems_cleaned.loc[ind,
                                            'has_ambient_temp_data'] = True
                    if ('temp' in key) or ('Temp' in key):
                        systems_cleaned.loc[ind,
                                            'has_some_temp_data'] = True
    # save and quit!
    systems_cleaned.to_csv(permanent_systems_cleaned_path,
                           index=False)
