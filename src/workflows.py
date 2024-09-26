import os

import geopandas as gpd
import xarray as xr
from stgrid2area import DistributedDaskProcessor, geodataframe_to_areas
from dask.distributed import Client

from util import leave_container

def workflow_eobs(parameters: dict, data: dict, dask_scheduler_file: str) -> None:
    """
    Run the E-OBS workflow. This workflow calculates the mean, min, 10th percentile, 
    median, max, 90th percentile, and standard deviation of the precipitation data 
    from E-OBS for each input area.

    """
    # Read E-OBS data
    eobs = xr.open_mfdataset(data["eobs_stgrid"])

    # TODO: Remove this slice, it is only for testing purposes
    eobs = eobs.sel(time=slice("2009", "2012"))

    # E-OBS data is in EPSG:4326
    eobs = eobs.rio.write_crs("EPSG:4326")

    # Read the areas
    gdf_areas = gpd.read_file(data["areas"])

    # Convert the geodataframe to stgrid2area areas
    areas = geodataframe_to_areas(areas=gdf_areas, id_column=parameters["areas_id_column"], output_dir="/out/eobs/")

    # Initialize the DistributedDaskProcessor
    processor = DistributedDaskProcessor(
        areas=areas,
        stgrid=eobs,
        variable="pp",
        operations=["mean", "min", "quantile(q=0.1)", "median", "max", "quantile(q=0.90)", "stdev"],
        n_workers=None, # will automatically be os.cpu_count()
        log_file="/out/eobs/report.log",
        skip_exist=True
    )

    # Run the processor, if a dask scheduler file is provided, use it
    dask_scheduler_file = parameters.get("dask_scheduler_file", None)
    if dask_scheduler_file:
        client = Client(scheduler_file=dask_scheduler_file)
        processor.run(client=client)
    else:
        processor.run()

    # alter file permissions (from docker) and delete unused tool-runner files
    leave_container()

def workflow_hyras(parameters: dict, data: dict) -> None:
    """
    Run the HYRAS workflow. This workflow calculates the mean, min, 10th percentile, 
    median, max, 90th percentile, and standard deviation of the precipitation data 
    from HYRAS for each input area.

    """
    # Read HYRAS data
    hyras = xr.open_mfdataset(data["hyras_stgrid"], combine="by_coords", chunks="auto").unify_chunks()

    # HYRAS data is in EPSG:3034
    hyras = hyras.rio.write_crs("EPSG:3034")

    # HYRAS: drop variable all variables that do not have all 3 dimensions x, y, time (time_bnds, x_bnds, y_bnds), only having the temporal dimension is fine (number_of_stations)
    hyras = hyras.drop_vars(["time_bnds", "x_bnds", "y_bnds"])
    
    # Remove the grid_mapping key from the variable's attributes (problems with xarray)
    hyras["rsds"].attrs.pop("grid_mapping", None)
    
    # Read the areas
    gdf_areas = gpd.read_file(data["areas"])

    # Convert the geodataframe to stgrid2area areas
    areas = geodataframe_to_areas(areas=gdf_areas, id_column=parameters["areas_id_column"], output_dir="/out/hyras/")

    # Initialize the DistributedDaskProcessor
    processor = DistributedDaskProcessor(
        areas=areas,
        stgrid=hyras,
        variable="rsds",
        operations=["mean", "min", "quantile(q=0.1)", "median", "max", "quantile(q=0.90)", "stdev"],
        n_workers=None, # will automatically be os.cpu_count()
        log_file="/out/hyras/report.log",
        skip_exist=True
    )

    # Run the processor, if a dask scheduler file is provided, use it
    dask_scheduler_file = parameters.get("dask_scheduler_file", None)
    if dask_scheduler_file:
        client = Client(scheduler_file=dask_scheduler_file)
        processor.run(client=client)
    else:
        processor.run()

    # alter file permissions (from docker) and delete unused tool-runner files
    leave_container()