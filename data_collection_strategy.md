# Data Collection Strategy

## Sample Data

The pre-existing data set with up-to-2022 degradation rates calculated is available at https://datahub.duramat.org/dataset/metadata/photovoltaic-fleet-degradation-insights-data.

It uses the package we will use, `RdTools`, to calculate degradation rate estimates.  It does not record the site, only a vague climate band, but it is indicative of what the target variable will look like.

## OEDI PVDAQ Data 

The OEDI PVDAQ database has three major sub-collections, with corresponding differences in data-collection strategy.  Moreover, we are still working on automated.

All data is ultimately from https://data.openei.org/submissions/4568; that is to say:
Deline, C., Perry, K., Deceglie, M., Muller, M., Sekulic, W., & Jordan, D. (2021). Photovoltaic Data Acquisition (PVDAQ) Public Datasets. [Data set]. Open Energy Data Initiative (OEDI). NREL. https://doi.org/10.25984/1846021
Metadata at most of the solar installations so tracked are in the “Available Systems Information” link, aka systems_20250729.csv.

We ultimately need data on irradiance for at least 2 consecutive years, or else the RdTools package which estimates the degradation rates assumes there isn’t enough data and drops it.  [Probably a safe assumption, given the noisiness of the data.]

### Collection 1: 2023 Solar Data Prize

See https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2F2023-solar-data-prize%2F 
Each site is numbered as in “Available Systems Information.”
There are only 6 sites in this group, but *lots* of data for each, so we try to minimize the downloads.

#### Early cleaning step

For each site, collect the irradiance data -- usually looking like `nnnn_irradiance_data.csv` where `nnnn` is the site number.  If no such file exists, we can safely drop it.  With only 6 sites, we can even do this by hand.
Then we look to see if there are 2 consecutive years of data, for RdTools requirements.  If not, we cannot use the data.  For example, System 2107 has data for the years 2017 and 2024, but nothing in-between, and that is not enough.

#### Downloads of all relevant data
We collect environment and irradiance data for each site, prefixed by the number, but with some adjustments (e.g., for site 2105, the data is 2105_environment_1_data.csv, 2105_environment_2_data.csv and 2015_irradiance_data.csv).  Crucially, our current project permits us to skip most of the files on electrical generation data (which is huge).
We also collect the metadata for each site.  This *should* be redundant, but we cannot afford to be careless.  
These are few enough to download “by hand.”
Then inspect the data.  If there are 2 consecutive years of data [for RdTools requirements], we keep it.  Otherwise, we have to drop it.
For example, System 2107 has data for the years 2017 and 2024, but nothing in-between, and that is not enough.

### Collection 2: PVDAQ Public Data Lake – Parquet

https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2Fparquet%2F 
Sadly, the older installations use parquet data rather than .csv data.  Collection strategy:
1. Download all the metrics pages. “metrics__system_1199__part000.parquet”.
2. If the metrics page does NOT have irradiance in some form, drop it.  Note that the name changes from site to site.  For example, system 1201 has it listed as `"metric_id":2789,"sensor_name":"poa_irradiance","common_name":"Irradiance POA"`, whereas system 1308 has it listed as `"metric_id":105,"sensor_name":"IntSolIrrad","common_name":"Irradiance POA"`  If we see it, we should record the `metric_id` number.
3. If we proceed from step 2, for this set we have daily entries.  Hence, we can use the `boto3` API and see if we (probably) have enough days by just counting the number of files that *could be* downloaded.  
4.  Go ahead and download the whole file to online storage.

### Collection 3: PVDAQ Public Data Lake – CSV

From https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2Fcsv%2F 

Fortunately, the (mostly) newer installations do allow .csv uploads.  They still do daily data uploads, though, so we still need to use the pvdaq_access API calls.  

1. For each system, go the the folder for it, e.g., pvdata/system_id=10020
2. Download one year’s csv file “manually”.  See if it has an irradiance column.  If not, drop that system_id.
3. If it does, and if there are two years of data, use the pvdaq_access routine to access all years of the data.  

*Note*: System 10 has data (though it is logged daily); none of the next 5 or 6 do.

## Notes past initial cleaning step.
Only the five prize sites and 61 of the 106 parquet-data sites have irradiance data.  We will have to determine if we have enough to proceed.

