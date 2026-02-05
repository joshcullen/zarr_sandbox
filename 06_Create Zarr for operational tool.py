
### Create multi-variable Zarr for CEG_operationalization and upload to GCS bucket ###

import xarray as xr
import zarr
import gcsfs
import matplotlib.pyplot as plt
import datetime as dt
import glob
import pandas as pd
import os
from pathlib import Path

# Grab all .nc files, but only if '2026' is NOT in the name
path = Path("/Users/joshcullen/Documents/UCSC_NOAA_Projects/Projects/CEG_operationalization/data_processing/TopPredatorWatch/rasters")
filtered_files = [str(f) for f in path.glob("*.nc") if "2026" not in f.name]

ds = xr.open_mfdataset(filtered_files, combine="by_coords")
print(ds)

# Chunk DataArray
# ds = ds.chunk({
#     "time": 31  # uniform chunks except last chunk (Zarr requirement)
# })
# ds.chunks
# print(ds)


# Function to preprocess NPP files (that don't have time stored in file)
def add_time_from_filename(ds):
    # Extract the filename from the source attribute xarray adds automatically
    filepath = ds.encoding["source"]
    filename = os.path.basename(filepath)
    
    # Logic to extract date: Adjust the slice/logic based on your filename format
    # Example: "weather_2023-10-01.nc" -> "2023-10-01"
    date_str = filename.split('_')[1].split('.')[0]
    file_time = pd.to_datetime(date_str)
    
    # Expand dims and assign the coordinate
    # This turns a 2D (lat, lon) into 3D (time, lat, lon)
    return ds.expand_dims(time=[file_time])



# Load in netCDFs from leatherback iSDM project
file_path1 = "/Users/joshcullen/Documents/UCSC_NOAA_Projects/Projects/CEG_operationalization/data_processing/TopPredatorWatch/rasters/*_2026-*.nc"
files1 = glob.glob(file_path1)
# ds = xr.open_mfdataset(file_path)

exclude_list = ["PPupper200m"]
filtered_files1 = [f for f in files1 if not any(x in f for x in exclude_list)]

# Combine daily files together
ds1 = xr.open_mfdataset(filtered_files1)
print(ds1)

#npp
ds_npp = xr.open_mfdataset("/Users/joshcullen/Documents/UCSC_NOAA_Projects/Projects/CEG_operationalization/data_processing/TopPredatorWatch/rasters/*PPupper200m_2026-*.nc",
                           preprocess=add_time_from_filename)
print(ds_npp)

# Merge daily data
ds_daily = xr.merge([ds1, ds_npp])
print(ds_daily)


# Merge daily w/ large files
ds_full = xr.merge([ds, ds_daily])


# View size of dataset
print(f"Size in TB: {ds.nbytes / 1e12:.2f} TB")
ds.sizes
print(ds)


ds_full['CHL'].isel(time=398).plot()
plt.close()



# Chunk DataArray
ds_full = ds_full.chunk({
    "time": 31  # uniform chunks except last chunk (Zarr requirement)
})
ds_full.chunks



# Define where to store Zarr file in bucket (and here, I'm telling it the new "zarr_cmems" folder to create and write all subfolders)
zarr_path = "gs://esd-climate-ecosystems-dev/zarr_cmems"

## Need to make sure that an application_default_credentials.json is stored in the "~/.config/gcloud/" path
## If not, need to run `gcloud auth application-default login` assuming gcloud SDK already installed


# Write to cloud bucket
%%time
ds_full.to_zarr(
    zarr_path,
    mode="w",
    consolidated=False,
    storage_options={"token": "google_default"}
)
#took 13 min to run (for 16 GB dataset)



# Now try reading this Zarr file stored in the bucket
ds_cloud = xr.open_zarr(zarr_path, 
                        consolidated=False,
                        storage_options={"token": "google_default"})  # W/ current permissions, requires creds
print(ds_cloud)
ds_cloud.chunks

# Subset Zarr data from cloud and plot
ds_cloud['analysed_sst'].sel(
  latitude=slice(50, 20), 
  longitude=slice(220, 260)
  ).isel(time=-1).plot(aspect="equal", size=7)

ds_cloud.time.values
