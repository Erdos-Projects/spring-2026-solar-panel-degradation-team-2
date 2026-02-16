'''Download the main metadata source,
clean up the metadata a little bit,
and determine which solar installations are
(likely to) have useful data.'''

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import os
import boto3
import botocore
from botocore.handlers import disable_signing
import time

# prepare for future pandas 3.0 usage
pd.options.mode.copy_on_write = True

s3 = boto3.resource("s3")
s3.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
bucket = s3.Bucket("oedi-data-lake")

# initialize downloads documentation
downloads_cols = ['Filename', 'Source', 'Access Time']
downloads_list = []
downloads_path = Path('../../data_inventory.csv')
if downloads_path.is_file():
    downloads_df = pd.read_csv(downloads_path)
else:
    downloads_df = pd.DataFrame(data=None, columns=downloads_cols)


def downloader(path_to_dir_local: str, path_to_dir_online: str,
               warn_empty=False):
    '''Download a file or collection of files from the
    OEDI PVDAQ Data Lake.
    More granular control than the pvdaq_access package.'''
    global bucket, downloads_list
    if path_to_dir_local[-1] != '/' and path_to_dir_local[-1] != '\\':
        raise ValueError('Local path does not end in "/" or "\\",'
                         + 'is not a possible directory!')
    my_local_dir = Path(path_to_dir_local)
    if not my_local_dir.is_dir():
        my_local_dir.mkdir()
    objects = bucket.objects.filter(
        Prefix=path_to_dir_online
    )
    if len(list(objects)) == 0:
        if warn_empty:
            print('No such files!')
    else:
        for obj in objects:
            # horrible mix of os.path and pathlib.Path, but it works
            online_file_path = Path(
                os.path.join(
                    my_local_dir, os.path.basename(obj.key)
                )
            )
            # avoid re-downloading file
            # may have to delete this check if multiprocessing necessary
            if not online_file_path.is_file():
                download_time = time.time()
                bucket.download_file(
                    obj.key, online_file_path
                )
                sources_download = {
                    "Filename": str(online_file_path),
                    "Source": str(obj),
                    "Access Time": download_time
                }
                downloads_list.append(sources_download)
    return


if __name__ == '__main__':
    # download the sources_file
    downloader(
        '../../data/raw/',
        'pvdaq/csv/systems_20250729.csv'
    )
    # load sources
    systems_cleaned = pd.read_csv('../../data/raw/systems_20250729.csv')
    # put starting date as date type
    systems_cleaned['first_timestamp'] = pd.to_datetime(
        systems_cleaned['first_timestamp'], format='%m/%d/%Y %H:%M'
    ).astype('datetime64[s]')
    systems_cleaned.loc[:, 'first_year']\
        = systems_cleaned.loc[:, 'first_timestamp'].dt.year
    num_sources = systems_cleaned.shape[0]
    systems_cleaned.loc[:, 'is_prize_data']\
        = pd.Series([False]*num_sources, dtype='boolean')
    systems_cleaned.loc[:, 'is_lake_parquet_data']\
        = pd.Series([False]*num_sources, dtype='boolean')
    systems_cleaned.loc[:, 'is_lake_csv_data']\
        = pd.Series([False]*num_sources, dtype='boolean')
    systems_cleaned.loc[:, 'has_irrad_data']\
        = pd.Series([False]*num_sources, dtype='boolean')
    systems_cleaned.loc[:, 'has_enough_irrad_data']\
        = pd.Series([False]*num_sources, dtype='boolean')
    systems_id_set = set(systems_cleaned['system_id'].unique())
    print("Proceeding to load data from prize data.")
    # by manual inspection of the 5 sites in the prize data,
    # all of them satisfy the criteria
    prize_system_ids = [2105, 2107, 7333, 9068, 9069]
    for system_id in prize_system_ids:
        relevant_rows = systems_cleaned.loc[
            systems_cleaned.loc[:, 'system_id'] == system_id
        ]
        for ind in relevant_rows.index:
            systems_cleaned.loc[ind, 'is_prize_data'] = True
            systems_cleaned.loc[ind, 'has_irrad_data'] = True
            systems_cleaned.loc[ind, 'has_enough_irrad_data'] = True
    print("Proceeding to load data from parquet data.")
    # We begin by downloading metadata.
    downloader(
        "../../data/raw/parquet-metrics/",
        "pvdaq/parquet/metrics/"
    )
    downloader(
        "../../data/raw/parquet-sites/",
        "pvdaq/parquet/site/"
    )
    downloader(
        "../../data/raw/parquet-systems/",
        "pvdaq/parquet/system/"
    )
    metrics_dir = Path("../../data/raw/parquet-metrics/")
    metrics_pq = pq.ParquetDataset(metrics_dir)
    metrics_df = metrics_pq.read().to_pandas()
    parquet_metrics_set = set(metrics_df['system_id'].unique())
    systems_dir = Path('../../data/raw/parquet-systems/')
    systems_pq = pq.ParquetDataset(systems_dir)
    systems_df = systems_pq.read().to_pandas()
    parquet_systems_set = set(systems_df['system_id'].unique())
    # At first, I was worried, because there were 4 items in
    # parquet_metrics_set that were not in systems_id_set.
    # We now demonstrate that it is pointless to worry.
    # First, we only allow metrics data
    # that also has system data, or else it is just too
    # incomplete.
    parquet_full_data_set = parquet_metrics_set.intersection(
        parquet_systems_set
    )
    parquet_not_original_list = list(
        parquet_full_data_set.difference(systems_id_set)
    )
    if (len(parquet_not_original_list) != 1) or (
        int(parquet_not_original_list[0]) != 2045
    ):
        raise RuntimeError('Additional terms in set difference!')
    # Assuming no trouble, the only system_id remaining is 2045.
    # From actually reading the parquet-systems file, system 2045
    # is an irradiance-measuring tool disconnected from any solar cells.
    # Also, it is in Golden, CO where many other solar facilities are.
    # So, it can be ignored!

    # We first populate the 'is_lake_parquet_data' flag
    for system_id in parquet_metrics_set.intersect(systems_id_set):
        # first, can definitely flag the 'is_lake_parquet_data' flag
        sys_relevant_rows = systems_cleaned.loc[
            systems_cleaned.loc[:, 'system_id'] == system_id
        ]
        for ind in sys_relevant_rows.index:
            systems_cleaned.loc[ind, 'is_lake_parquet_data'] = True

    # We continue by filtering out the systems that do not collect
    # irradiance data.
    # Just look for a common name with 'rrad'
    # to avoid testing capital vs. lowercase i.
    metrics_with_irrad = metrics_df.loc[
        metrics_df.loc[:, 'common_name'].str.contains('rrad')
    ]
    parquet_metrics_irrad_set = set(metrics_with_irrad['system_id'].unique())
    # now we check for having enough data
    for system_id in parquet_metrics_irrad_set.intersect(systems_id_set):
        # first, can definitely flag the 'has_irrad_data' flag
        sys_relevant_rows = systems_cleaned.loc[
            systems_cleaned.loc[:, 'system_id'] == system_id
        ]
        for ind in sys_relevant_rows.index:
            systems_cleaned.loc[ind, 'has_irrad_data'] = True
        # it was an unpleasant surprise to learn for the parquet data
        # that the first year was calculated incorrectly.
        # Our simple strategy is to start with the hinted year
        # and increment until we actually have a good starting year.
        # We query the database, but only for the count of hits.
        first_ind = sys_relevant_rows.index[0]
        first_year = int(sys_relevant_rows.loc[first_ind, 'first_year'])
        # As seen with sites 1283, 1284, 1289,
        # systems start date != first Parquet data unit.
        # start with first_year from hint and increment to right first year
        good_first_year = False
        while not good_first_year:
            prefix = "pvdaq/parquet/pvdata/"\
                + f"system_id={system_id}/year={first_year}"
            # recall the s3 Bucket object, bucket
            # our access point to the data set.
            objects = bucket.objects.filter(
                Prefix=prefix
            )
            if (objects is None) or (len(list(objects)) == 0):
                first_year += 1
                if first_year >= 2024:
                    print(system_id)
                    print('Breaking to avoid infinite loop,'
                          + 'but not enough data.')
                    good_first_year = True
            else:
                good_first_year = True
        # correct the first year
        sys_relevant_rows.loc[first_ind, 'first_year'] = first_year
        # continue with "enough data check" tomorrow.
        
        
    
