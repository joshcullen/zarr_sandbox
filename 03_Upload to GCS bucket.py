
### Example code for uploading a full Zarr file to a GCS bucket ###

import xarray as xr
import zarr
import gcsfs
import matplotlib.pyplot as plt

# Define where the Zarr file resides locally
zarr_local = "zarr_monthly/"

# Define where to store Zarr file in bucket (and here, I'm telling it the new "zarr_monthly" folder to create and write all subfolders)
zarr_path = "gs://esd-climate-ecosystems-dev/zarr_monthly"

## Need to make sure that an application_default_credentials.json is stored in the "~/.config/gcloud/" path
## If not, need to run `gcloud auth application-default login` assuming gcloud SDK already installed

# Open the locally saved Zarr file
ds_store = xr.open_zarr(zarr_local, 
                        consolidated=False) # zarr v3: use consolidated=False 
print(ds_store)

# Write to cloud bucket
%%time
ds_store.to_zarr(
    zarr_path,
    mode="w-",
    consolidated=False,
    storage_options={"token": "google_default"}
)
#took 4.25 hrs to run (383 chunks of ~100 MB; 220 GB)




# Now try reading this Zarr file stored in the bucket
ds_cloud = xr.open_zarr(zarr_path, 
                        consolidated=False,
                        storage_options={"token": "google_default"})  # W/ current permissions, requires creds
print(ds_cloud)
ds_cloud.chunks

# Subset Zarr data from cloud and plot
ds_cloud['thetao'].sel(
  latitude=slice(-20, 50), 
  longitude=slice(220, 260)
  ).isel(time=-1).plot()
