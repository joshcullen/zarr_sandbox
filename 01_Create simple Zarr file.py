
### Example code to write a small Zarr file based on a netCDF ###

import xarray as xr
import matplotlib.pyplot as plt
import datetime as dt
import fsspec


# Load previously downloaded CMEMS data
base = "https://coastwatch.pfeg.noaa.gov/erddap/griddap"
dataset_id = "erdQCwindproducts3day"   # 3-day composite (daily time steps)
url = f"{base}/{dataset_id}"

ds = xr.open_dataset(url, decode_times=True)
ds

# Define time period of interest
t0 = "2024-01-01"
t1 = dt.date.today().strftime("%Y-%m-%d")  #today's date

# Subset data over time and space
dc = ds['ekman_upwelling'].sel(
  latitude=slice(50, 30),  #ymax needs to be listed first
  longitude=slice(-130, -115), 
  time=slice(t0, t1))

dc.sel(time=dc.time.max()).plot()
plt.close()
dc.size

dz = dc.chunk({
    "time": 365  # uniform chunks except last chunk (Zarr requirement)
})

# Print chunk sizes per dim (time, altitude, lat, long)
print(dz)
dz.chunks
dz.sizes

# Write Zarr file
zarr_path = "./zarr_simple"
%%time
dz.to_zarr(
    zarr_path,
    mode="w",
    consolidated=False
)
#took 16 sec to run



# Try to read in Zarr file now (locally)
new_ds = xr.open_zarr(zarr_path, consolidated=False)
new_ds.chunks
print(new_ds)

new_ds2 = new_ds['ekman_upwelling'].sel(time=new_ds.time.max())
new_ds2.sizes

new_ds2.plot()
print(new_ds2)



## Try to read Zarr file from GitHub repo

# Define the repository details
user = "joshcullen"
repo = "zarr_sandbox"
path = "zarr_simple" # Path inside the repo to the .zarr folder
branch = "main"            # Branch or commit SHA

# Create a filesystem object
fs = fsspec.filesystem("github", org=user, repo=repo, sha=branch)

# Create a mapper (a key-value view of the files)
zarr_url = fs.get_mapper(path)
# zarr_url = "https://github.com/joshcullen/zarr_sandbox/tree/main/zarr_simple"
ds_gh = xr.open_zarr(zarr_url, consolidated=False)
print(ds_gh)
