#!/usr/bin/env python3

from glob import glob
import logging
from pathlib import Path
import json
import os

import geopandas as gpd
import xarray as xr
from stgrid2area import MPIDaskProcessor, geodataframe_to_areas
from dask.distributed import Client
from dask_mpi import initialize
import dask
from dask import delayed, compute


@delayed
def open_icond2_file(filepath):
    # Temporarily silence cfgrib and cfgrib.dataset loggers, as processing.log would otherwise be flooded with warnings from cfgrib
    cfgrib_logger = logging.getLogger("cfgrib")
    cfgrib_dataset_logger = logging.getLogger("cfgrib.dataset")
    
    # Store original logging levels
    original_cfgrib_level = cfgrib_logger.level
    original_cfgrib_dataset_level = cfgrib_dataset_logger.level
    
    # Set to CRITICAL to suppress the unwanted error messages
    cfgrib_logger.setLevel(logging.CRITICAL)
    cfgrib_dataset_logger.setLevel(logging.CRITICAL)

    # Open dataset
    ds = xr.open_dataset(filepath, engine="cfgrib", chunks="auto", decode_timedelta=True).unify_chunks()

    # Set CRS and rename dimensions
    ds = ds.rio.write_crs("EPSG:4236")
    ds = ds.rename({"time": "timestep", "step": "time"})

    # Add initialization time to time dimension (to have it in the output csv)
    ds = ds.assign_coords(time=ds.timestep + ds.time)

    # Validation check (check that time[0] is the initialization time)
    if ds.time[0].values != ds.timestep.values:
        logger.warning(f"Initialization time mismatch in file: {filepath}")

    # Restore original logging levels
    cfgrib_logger.setLevel(original_cfgrib_level)
    cfgrib_dataset_logger.setLevel(original_cfgrib_dataset_level)
    
    return ds
    

def workflow_icond2_deterministic(parameters: dict, data: dict, variables: list[str], client: Client, logger: logging.Logger) -> None:
    """
    Run the ICON-D2 deterministic run workflow for all variables. This workflow calculates the following spatial statistics:
    - min
    - mean
    - max
    - standard deviation
    of the variable data from ICON-D2 data for each input area.  
    Additionally, the clipped netCDF files can be saved in the output directory.
    """
    # Find all ICON-D2 files
    icond2 = []
    icond2_files = sorted(glob(data[f"icond2_stgrid"]))

    # Remove .idx files which are created by cfgrib
    icond2_files = [f for f in icond2_files if not f.endswith(".idx")]

    # exclude timestamps in January 2021 and February 2021, as forecasts every 3 hours start after these tstamps
    exclude_tstamps = ["2021012900", "2021012912", "2021013000", "2021013012", 
                       "2021020100", "2021020200", "2021020300", "2021020400", "2021020500", "2021020600", "2021020700", "2021020800", "2021020900", "2021021000"]
    icond2_files = sorted([f for f in icond2_files if f.split("_")[-1] not in exclude_tstamps])

    if len(icond2_files) == 0:
        raise ValueError (f"No ICON-D2 data found at {data[f'icond2_stgrid']}.")
    
    # Read the ICON-D2 data in chunks (same chunks as the original data from DWD)
    logger.info(f"Start opening {len(icond2_files)} ICON-D2 grib files.")
    
    # Create parallel tasks for opening ICON-D2 files
    tasks = [open_icond2_file(f) for f in sorted(icond2_files)]

    # Run file opening in parallel ("processes" was fastest)
    icond2 = compute(*tasks, scheduler="processes")

    # Make sure icond2 is a list
    icond2 = list(icond2)

    logger.info(f"Sucessfully loaded [{len(icond2)} / {len(icond2_files)}] ICON-D2 files.")
    
    # Read the areas
    gdf_areas = gpd.read_file(data["areas"])

    # Reproject the areas to the CRS of the ICON-D2 data
    gdf_areas = gdf_areas.to_crs("EPSG:4236")

    # Convert the geodataframe to stgrid2area areas
    areas = geodataframe_to_areas(areas=gdf_areas, id_column=parameters["areas_id_column"], output_dir=parameters["output_dir"], sort_by_proximity=True)

    # Initialize the LocalDaskProcessor
    processor = MPIDaskProcessor(
        areas=areas,
        stgrid=icond2,
        variables=variables,
        method="fallback_xarray",
        operations=["min", "mean", "max", "stdev"],
        skip_exist=parameters["skip_exist"],
        batch_size=parameters.get("batch_size", None),
        save_nc=parameters["save_nc"],
        save_csv=parameters["save_csv"],
        logger=logger
    )

    # Log
    logger.info(f"Starting processing ICON-D2 data.")

    # Run the processor
    processor.run(client=client)

    logger.info(f"Finished processing ICON-D2 data.")


if __name__ == "__main__":
    # Initialize dask-mpi cluster on all processes.
    initialize(local_directory=os.environ.get("TMPDIR"))

    # Create a Dask client (all processes join the cluster)
    client = Client() 

    # Set parameters and data.
    parameters = {
        "areas_id_column": "gauge_id",
        "variables": ["tp", "t2m", "avg_snswrf", "prmsl", "sd"],
        "skip_exist": True,
        "save_nc": False,
        "save_csv": True,
        "batch_size": 550,
        "output_dir": "/pfs/data6/home/ka/ka_iwu/ka_qt7760/workspaces/pfs5wor7/qt7760-s2a/out/icond2_deterministic_extracted"
    }
    data = {
        "areas": "/pfs/data6/home/ka/ka_iwu/ka_qt7760/datasets/CAMELS_DE/CAMELS_DE_catchment_boundaries/catchments/CAMELS_DE_catchments.gpkg",
        "icond2_stgrid": "/pfs/data6/home/ka/ka_iwu/ka_qt7760/workspaces/pfs5wor7/qt7760-s2a/in/icon_d2_deterministic_extracted/*/*/*"
    }
    
    # create output directory
    output_dir = Path(parameters["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    # Set up logger
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        filename=f"{parameters['output_dir']}/processing.log",
        encoding='utf-8',
        level=logging.INFO,
        format='[%(asctime)s] - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger.info(f"\nParameters: {parameters}\nData: {data}\n")
    
    workflow_icond2_deterministic(parameters, data, variables=parameters["variables"], client=client, logger=logger)
    
    client.close()