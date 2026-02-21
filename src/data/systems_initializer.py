'''Download the main metadata source,
clean up the metadata a little bit,
and determine which solar installations are
(likely to) have useful data.'''

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import os
import boto3
from botocore.handlers import disable_signing
import time
import datetime
import json

# prepare for future pandas 3.0 usage
pd.options.mode.copy_on_write = True

s3 = boto3.resource("s3")
s3.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
bucket = s3.Bucket("oedi-data-lake")


def downloader(path_to_dir_local: str, path_to_dir_online: str,
               warn_empty=False, is_specific_file_type=False,
               specific_file_type='',
               log_path='../../logs/logs.csv',
               data_directory_description=''):
    '''Download a file or collection of files from the
    OEDI PVDAQ Data Lake.
    More granular control than the pvdaq_access package,
    and included some logging.

    Parameters
    ------------
    path_to_dir_local: str
        A string representing the desired path to the file storage
        on the local system.  Must be a valid directory [end in / or \\]
    path_to_dir_online: str
        A string directing the prefix of the filenames of the files
        to access online.
        Despite the name, this does not need to be a directory.
    warn_empty: bool
        Print if there are no items to download.
    is_specific_file_type: bool
        Print if you want to restrict to a particular file_type
    specific_file_type: str
        The specific file type you want.
    log_path: str
        The path to the log file you want.
    data_directory_description: str
        The describing text you want in the data file.
    '''
    global bucket
    downloads_list = []
    if path_to_dir_local[-1] != '/' and path_to_dir_local[-1] != '\\':
        raise ValueError('Local path does not end in "/" or "\\",'
                         + ' and hence is not a possible directory!')
    my_local_dir = Path(path_to_dir_local)
    if not my_local_dir.is_dir():
        my_local_dir.mkdir()
    objects = bucket.objects.filter(
        Prefix=path_to_dir_online
    )
    # sometimes objects goofs and gives a no-continuation prefix
    # to the online directory/filepath
    # in addition to the other objects, so some workarounds
    # are necessary.
    if len(list(objects)) == 0:
        if warn_empty:
            print('No such files!')
        return False
    else:
        for obj in objects:
            # horrible mix of os.path and pathlib.Path, but it works
            file_path = Path(
                os.path.join(
                    my_local_dir, os.path.basename(obj.key)
                )
            )
            # Sometimes filter messes up and just gives us a directory back.
            # if it's the same as the starting directory, ok,
            # harmless error, skip it
            # if it's not the same as the starting directory, flag it.
            if file_path.is_dir():
                if file_path != my_local_dir:
                    print(file_path)
                    raise ValueError('Somehow we got a new directory back!')
            elif not file_path.is_file():  # time to download!
                # check for file type if asked
                suffix_len = len(specific_file_type)
                type_valid = False
                if not is_specific_file_type:
                    type_valid = True
                elif f'{obj.key}'[-suffix_len:] == specific_file_type:
                    type_valid = True
                if type_valid:
                    download_time = time.time()
                    bucket.download_file(
                        obj.key, file_path
                    )
                    sources_download = {
                        "Filename": str(file_path),
                        "Source": str(obj.key),
                        "Access Time": download_time
                    }
                    downloads_list.append(sources_download)
        if len(downloads_list) > 0:
            # save download logs
            log_path = Path(log_path)
            try:
                with open(log_path, mode='a') as log_adder:
                    log_adder.writelines(
                        [f'{inst["Filename"]},'
                         + f'{inst["Source"]},'
                         + f'{inst["Access Time"]}\n'
                         for inst in downloads_list]
                    )
            except FileNotFoundError:
                log_path.touch()
                with open(log_path, mode='w') as log_adder:
                    log_adder.writelines(
                        [f'{inst["Filename"]},'
                         + f'{inst["Source"]},'
                         + f'{inst["Access Time"]}\n'
                         for inst in downloads_list]
                    )
            except BaseException as e:
                raise e

            # append new notes to data_inventory.csv
            data_inventory_path = '../../data_inventory.csv'
            try:
                with open(data_inventory_path, 'a') as data_adder:
                    data_adder.writelines(
                        [f'{inst["Filename"]},'
                         + data_directory_description + '\n'
                         for inst in downloads_list]
                    )
            except FileNotFoundError:
                data_inventory_path.touch()
                with open(log_path, mode='w') as data_adder:
                    data_adder.writelines(
                        [f'Filename: {inst["Filename"]},'
                         + data_directory_description + '\n'
                         for inst in downloads_list]
                    )
            except BaseException as e:
                raise e
        return True


if __name__ == '__main__':
    # download the sources_file
    downloader(
        '../../data/raw/',
        'pvdaq/csv/systems_20250729.csv'
    )
    # load sources
    systems_cleaned = pd.read_csv('../../data/raw/systems_20250729.csv')
    # drop some empty unnamed columns [coming from extra commas in the csv]
    unnamed_columns = [
        col_name for col_name in systems_cleaned.columns.array
        if "Unnamed:" in col_name
    ]
    systems_cleaned = systems_cleaned.drop(
        columns=unnamed_columns
    )
    # put starting date as date type
    systems_cleaned['first_timestamp'] = pd.to_datetime(
        systems_cleaned['first_timestamp'], format='%m/%d/%Y %H:%M'
    ).astype('datetime64[s]')
    systems_cleaned.loc[:, 'first_year']\
        = systems_cleaned['first_timestamp'].dt.year
    num_sources = systems_cleaned.shape[0]
    systems_cleaned.loc[:, 'is_prize_data']\
        = pd.Series([False]*num_sources, dtype='boolean')
    systems_cleaned.loc[:, 'is_lake_parquet_data']\
        = pd.Series([False]*num_sources, dtype='boolean')
    systems_cleaned.loc[:, 'is_lake_csv_data']\
        = pd.Series([False]*num_sources, dtype='boolean')
    systems_cleaned.loc[:, 'has_irrad_data']\
        = pd.Series([False]*num_sources, dtype='boolean')
    systems_id_set = set(systems_cleaned['system_id'].unique())
    print("Proceeding to load data from prize data.")
    # by manual inspection, there are 5 sites in the prize data,
    prize_system_ids = [2105, 2107, 7333, 9068, 9069]
    for system_id in prize_system_ids:
        # download the metadata
        downloader(
            '../../data/raw/prize-metadata/',
            f'pvdaq/2023-solar-data-prize/{system_id}_OEDI/metadata/'
        )
        # load the data
        metadata_filepath = Path(
                '../../data/raw/prize-metadata/'
                + f'{system_id}_system_metadata.json'
            )
        with open(metadata_filepath) as json_reader:
            local_metadata = json.load(json_reader)
            system_metrics = local_metadata['Metrics']
            # for reasons to get into later, we override the 'first_year' line
            first_timestamp = local_metadata['System']['first_timestamp']
            first_year = datetime.datetime.strptime(
                first_timestamp, "%Y-%m-%d %H:%M:%S"
            ).year
            # assign the data to the chart.
            relevant_rows = systems_cleaned.loc[
                systems_cleaned.loc[:, 'system_id'] == system_id
            ]
            for ind in relevant_rows.index:
                systems_cleaned.loc[ind, 'is_prize_data'] = True
                for key in system_metrics.keys():
                    # Avoid Irrad vs. irrad as follows.
                    if 'rrad' in key:
                        systems_cleaned.loc[ind, 'has_irrad_data'] = True
                        break
    # Note that the metadata files include both "started_on"
    # and "first_timestamp" properties;
    # we manually checked that first_timestamp is more accurate.
    # lastly, we note that 7333 is a really-fast-reporting location,
    # and has downsampled its data in a different folder.
    # We grab the metadata for reference
    downloader(
        '../../data/raw/prize-metadata/',
        'pvdaq/2023-solar-data-prize/7333_5_min_OEDI/metadata/',
        warn_empty=True
    )

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
    for system_id in parquet_metrics_set.intersection(systems_id_set):
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
    for system_id in parquet_metrics_irrad_set.intersection(systems_id_set):
        # first, can definitely flag the 'has_irrad_data' flag
        sys_relevant_rows = systems_cleaned.loc[
            systems_cleaned.loc[:, 'system_id'] == system_id
        ]
        for ind in sys_relevant_rows.index:
            systems_cleaned.loc[ind, 'has_irrad_data'] = True
        # it was an unpleasant surprise to learn for the parquet data
        # that the first year was calculated incorrectly.
        # (see systems 1283, 1284, 1289)
        # Our simple strategy is to start with the hinted year
        # and increment until we actually have a good starting year.
        first_ind = sys_relevant_rows.index[0]
        first_year = int(sys_relevant_rows.loc[first_ind, 'first_year'])
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

    print("Proceeding to load metadata from csv data.")
    # We begin by downloading metadata.
    downloader(
        "../../data/raw/csv-metadata/",
        "pvdaq/csv/system_metadata/",
        warn_empty=False,
        is_specific_file_type=True,
        specific_file_type='.json'
    )
    downloader(
        "../../data/raw/csv-metadata/",
        "pvdaq/csv/system_metadata/",
        warn_empty=False,
        is_specific_file_type=True,
        specific_file_type='.pdf'
    )
    csv_metadata_dir = Path('../../data/raw/csv-metadata/')
    # now grab the json files, infer the system_id, and
    # check for metadata
    jsons = csv_metadata_dir.glob("*_system_metadata.json")
    for file_path in jsons:
        system_id = int(
            file_path.parts[-1].replace('_system_metadata.json', '')
        )
        with open(file_path) as reader:
            local_metadata = json.load(reader)
            has_metrics = True
            try:
                system_metrics = local_metadata['Metrics']
            except KeyError:
                has_metrics = False
            except BaseException as e:
                raise e
            # we again override the 'first_year' data
            first_timestamp = local_metadata['System']['started_on']
            first_year = datetime.datetime.strptime(
                first_timestamp, "%Y-%m-%d %H:%M:%S"
            ).year
            # assign the data to the chart.
            relevant_rows = systems_cleaned.loc[
                systems_cleaned.loc[:, 'system_id'] == system_id
            ]
            for ind in relevant_rows.index:
                systems_cleaned.loc[ind, 'is_lake_csv_data'] = True
                if has_metrics:
                    for key in system_metrics.keys():
                        # Avoid Irrad vs. irrad as follows.
                        if 'rrad' in key:
                            systems_cleaned.loc[ind, 'has_irrad_data'] = True
                            break
                # otherwise, "standard" outputs do not contain irradiance,
                # so do nothing.
                systems_cleaned.loc[ind, 'first_year'] = first_year
    # finally, save the data!
    permanent_systems_cleaned_path = Path(
        '../../data/core/systems_cleaned.csv'
    )
    systems_cleaned.to_csv(permanent_systems_cleaned_path,
                           index=False)
