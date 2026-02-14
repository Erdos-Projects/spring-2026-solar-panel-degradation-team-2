# Data Collection Strategy

## Sample Data

The pre-existing data set with up-to-2022 degradation rates calculated is available at https://datahub.duramat.org/dataset/metadata/photovoltaic-fleet-degradation-insights-data.

It uses the package we will use, `RdTools`, to calculate degradation rate estimates.  It does not record the site, only a vague climate band, but it is indicative of what the target variable will look like.

## OEDI PVDAQ Data 

The OEDI PVDAQ database has three major sub-collections, with corresponding differences in data-collection strategy.  Moreover, none of the three can be completely automated at this time.

All data is ultimately from https://data.openei.org/submissions/4568.  Metadata at all solar installations so tracked are in the “Available Systems Information” link, aka systems_20250729.csv.

We ultimately need data on irradiance for at least 2 consecutive years, or else the RdTools package which estimates the degradation rates assumes there isn’t enough data and drops it.  [Probably a safe assumption, given the noisiness of the data.]

### Collection 1: 2023 Solar Data Prize

See https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2F2023-solar-data-prize%2F 
Each site is numbered as in “Available Systems Information.”
We collect environment and irradiance data for each site, prefixed by the number, but with some adjustments (e.g., for site 2105, the data is 2105_environment_1_data.csv and 2105_environment_2_data.csv and 2015_irradiance_data.csv).
These are few enough to download “by hand.”
Then inspect the data.  If there are 2 consecutive years of data [for RdTools requirements], we keep it.  Otherwise, we have to drop it.
For example, System 2107 has data for the years 2017 and 2024, but nothing in-between, and that is not enough.

### Collection 2: PVDAQ Public Data Lake – Parquet

https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2Fparquet%2F 
Sadly, the older installations use parquet data rather than .csv data.  Collection strategy:
1. For each (numbered) site, go to the corresponding metrics page, e.g., “metrics__system_1199__part000.parquet”.
2. If the metrics page does NOT have irradiance in some form, drop it.  Note that the name changes from site to site.  For example, system 1201 has it listed as `"metric_id":2789,"sensor_name":"poa_irradiance","common_name":"Irradiance POA"`
Whereas system 1308 has it listed as `"metric_id":105,"sensor_name":"IntSolIrrad","common_name":"Irradiance POA"`  If we see it, we should record the `metric_id` number.
3. If we proceed from step 2, go ahead and use the `pvdaq_access` package (encapsulating API calls) to download all the parquet files.  There is one parquet File for each day of data collection, so this takes up to about 20 minutes per location.  [Yes, we need to make a Colab of it eventually.]

4. Go ahead and use the `parquet` package to load the dataframe.  Check: Is there a ‘value’ column?  If so, great, we can add it to the pile.  If not, then the value column was omitted because it was always 0, so it was never actually collected or was accidentally erased.  [This was the case for Systems 1201 and 1308 both, at least checking a sample of the daily parquet files.]

*Note:* Of the 4 systems checked so far (1199, 1201, 1202, 1308), the first had no irradiance factor whatsoever, and the latter three had consistently-0 entries for it.  It is not quite clear if we will ever get many data points here.

### Collection 3: PVDAQ Public Data Lake – CSV

From https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2Fcsv%2F 

Fortunately, the (mostly) newer installations do allow .csv uploads.  They still do daily data uploads, though, so we still need to use the pvdaq_access API calls.  

1. For each system, go the the folder for it, e.g., pvdata/system_id=10020
2. Download one year’s csv file “manually”.  See if it has an irradiance column.  If not, drop that system_id.
3. If it does, and if there are two years of data, use the pvdaq_access routine to access all years of the data.  

*Note*: System 10 has data (though it is logged daily); none of the next 5 or 6 do.

## Conclusions:
 We admit that the number of sites with “good” data is limited.  We hope that it will be enough to complete the project.

