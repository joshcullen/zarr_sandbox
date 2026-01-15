
### Example code to benchmark reading from Zarr vs netCDF file in cloud bucket ###

import xarray as xr
import time
from dask.distributed import Client

# 1. Setup Dask Client
# This spins up a local cluster on your machine to handle parallel tasks.
# In a Jupyter environment, this also provides a link to the Dask Dashboard.
client = Client()
print(f"Dask Dashboard link: {client.dashboard_link}")

# 2. Setup Paths and Storage Options
zarr_path = "gs://esd-climate-ecosystems-dev/zarr_monthly"
nc_path = "gs://esd-climate-ecosystems-dev/sst.nc"
gcs_opts = {"token": "google_default"}

# 3. Open Datasets with Dask (Note the `chunks` argument)
# chunks={} tells xarray to use the file's internal chunking scheme.
ds_zarr = xr.open_zarr(zarr_path, 
                       consolidated=False, 
                       storage_options=gcs_opts,
                       chunks={}) 

ds_nc = xr.open_dataset(nc_path, 
                        engine="h5netcdf", 
                        chunks={})

# 4. Define Identical Selection Parameters
subset_params = dict(
    latitude=slice(-20, 50),
    longitude=slice(220, 260),
    time=slice("2000-01-01", "2004-12-31")
)

def run_dask_benchmark(ds, label):
    print(f"--- Testing {label} with Dask ---")
    
    # Create the lazy subset
    subset = ds['thetao'].sel(**subset_params)
    
    # Calculate size (metadata only operation)
    size_mb = subset.nbytes / 1e6
    print(f"Data size to load: {size_mb:.2f} MB")

    # Start Timer
    start_time = time.perf_counter()
    
    # .compute() triggers the Dask graph to execute
    # This downloads chunks in parallel and returns a numpy array
    _ = subset.compute()
    
    # End Timer
    end_time = time.perf_counter()
    duration = end_time - start_time
    
    print(f"Time taken: {duration:.4f} seconds")
    print(f"Throughput: {size_mb / duration:.2f} MB/s\n")
    return duration

# 5. Run the Benchmarks
# Zarr usually outperforms here because Dask can fetch multiple Zarr objects simultaneously.
zarr_duration = run_dask_benchmark(ds_zarr, "Zarr")  #took 18 min (throughput of 5.5 Mbps)
nc_duration = run_dask_benchmark(ds_nc, "NetCDF")  #took 24 min (throughput of 2 Mbps)

# Close client to free up resources
client.close()

if zarr_duration < nc_duration:
    print(f"Result: Zarr was {nc_duration / zarr_duration:.1f}x faster")
else:
    print(f"Result: NetCDF was {zarr_duration / nc_duration:.1f}x faster")
