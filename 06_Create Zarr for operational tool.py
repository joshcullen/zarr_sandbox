
### Create multi-variable Zarr for CEG_operationalization and upload to GCS bucket ###

import xarray as xr
import zarr
import gcsfs
import matplotlib.pyplot as plt
import datetime as dt


# Load in netCDFs from leatherback iSDM project
file_path = "/Users/joshcullen/Documents/UCSC_NOAA_Projects/Projects/CEG_operationalization/data_processing/TopPredatorWatch/rasters/*.nc"
ds = xr.open_mfdataset(file_path)

# View size of dataset
print(f"Size in TB: {ds.nbytes / 1e12:.2f} TB")
ds.sizes
print(ds)


ds['analysed_sst'].isel(time=-1).plot()
plt.close()



# Chunk DataArray
ds = ds.chunk({
    "time": 31  # uniform chunks except last chunk (Zarr requirement)
})
ds.chunks



# Define where to store Zarr file in bucket (and here, I'm telling it the new "zarr_cmems" folder to create and write all subfolders)
zarr_path = "gs://esd-climate-ecosystems-dev/zarr_cmems"

## Need to make sure that an application_default_credentials.json is stored in the "~/.config/gcloud/" path
## If not, need to run `gcloud auth application-default login` assuming gcloud SDK already installed


# Write to cloud bucket
%%time
ds.to_zarr(
    zarr_path,
    mode="w-",
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
