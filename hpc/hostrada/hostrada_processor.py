#!/usr/bin/env python3

from glob import glob
from typing import Tuple
import concurrent.futures
import logging
from pathlib import Path
import os

import geopandas as gpd
import xarray as xr
import pandas as pd
from stgrid2area import MPIDaskProcessor, geodataframe_to_areas
from dask.distributed import Client
from dask_mpi import initialize


def workflow_hostrada_variable(parameters: dict, data: dict, variable: str, client: Client, logger: logging.Logger) -> None:
    """
    Run the Hostrada workflow for a specific variable. This workflow calculates the following spatial statistics:
    - min
    - mean
    - max
    - standard deviation
    - 10th, 20th, 30th, 40th, 50th, 60th, 70th, 80th, 90th percentiles
    of the variable data from Hostrada data for each input area.  
    Additionally, the clipped netCDF files can be saved in the output directory.

    """
    # create output directory
    output_dir = Path(parameters["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Variable name mapping (air_temperature_max  air_temperature_mean  air_temperature_min  global_radiation  humidity  precipitation)
    variable_mapping = {
        "cloud_cover": "clt",
        "wind_speed": "sfcWind",
        "wind_direction": "sfcWind_direction",
        "air_temperature_mean": "tas",
        "dew_point_temperature": "tdew",
        "relative_humidity": "hurs",
        "water_vapor_mixing_ratio": "mixr",
        "air_pressure_sea_level": "psl",
        "air_pressure_surface": "ps",
        "global_shortwave_radiation": "rsds",
        "urban_heat_island_intensity": "uhi"
    }

    var_abbr = variable_mapping[variable]

    hostrada = []
    hostrada_files = glob(data[f"hostrada_stgrid_{variable}"])

    if len(hostrada_files) == 0:
        raise ValueError (f"No Hostrada data found at {data[f'hostrada_stgrid_{variable}']}.")

    # Read the Hostrada data in chunks (same chunks as the original data from DWD)
    for hostrada_file in hostrada_files:
        hostrada_chunk = xr.open_dataset(hostrada_file, chunks="auto").unify_chunks()

        # Remove the time_bnds variable which exists in the global_radiation data
        if variable == "global_shortwave_radiation":
            hostrada_chunk = hostrada_chunk.drop_vars("time_bnds")
        
        # Hostrada data is in EPSG:3034
        hostrada_chunk.rio.write_crs("EPSG:3034", inplace=True)

        hostrada.append(hostrada_chunk)

    
    # Try reading stgrid as one file
    # hostrada = xr.open_mfdataset(data[f"hostrada_stgrid_{variable}"], chunks="auto").unify_chunks()

    # Remove the time_bnds variable which exists in the global_radiation data
    # if variable == "global_shortwave_radiation":
    #     hostrada_chunk = hostrada_chunk.drop_vars("time_bnds")

    # Hostrada data is in EPSG:3034
    # hostrada_chunk.rio.write_crs("EPSG:3034", inplace=True)
    
    # Read the areas
    gdf_areas = gpd.read_file(data["areas"])

    # Reproject the areas to the CRS of the Hostrada data
    gdf_areas = gdf_areas.to_crs("EPSG:3034")

    # Convert the geodataframe to stgrid2area areas
    areas = geodataframe_to_areas(areas=gdf_areas, id_column=parameters["areas_id_column"], output_dir=output_dir/"hostrada"/variable, sort_by_proximity=True)

    # Initialize the LocalDaskProcessor
    processor = MPIDaskProcessor(
        areas=areas,
        stgrid=hostrada,
        variables=var_abbr,
        method="fallback_xarray",
        operations=["min", "mean", "max", "stdev", "quantile(q=0.1)", "quantile(q=0.2)", "quantile(q=0.3)", "quantile(q=0.4)", "quantile(q=0.5)", "quantile(q=0.6)", "quantile(q=0.7)", "quantile(q=0.8)", "quantile(q=0.9)"],
        skip_exist=parameters["skip_exist"],
        batch_size=parameters.get("batch_size", None),
        save_nc=parameters["save_nc"],
        save_csv=parameters["save_csv"],
        logger=logger
    )

    # Log
    logger.info(f"Starting processing variable {variable} from Hostrada data.")

    # Run the processor
    processor.run(client=client)

    # Log
    logger.info(f"Merging the clipped and aggregated files for each area.")

    timeout = 3600 # 1 hour timeout per area

    # Merge the clipped and aggregated files for each area in parallel    
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = []

        future_to_area = {executor.submit(merge_output_single_area, area, parameters["save_nc"], parameters["save_csv"], len(hostrada_files)): area for area in areas}

        for future in concurrent.futures.as_completed(future_to_area, timeout=timeout):
            try:
                area_id, success = future.result()
                results.append((area_id, success))
            except concurrent.futures.TimeoutError:
                area = future_to_area[future]
                logger.error(f"Timeout processing area {area.id} after {timeout} seconds.")
                results.append((area.id, False))

    # Log summary
    success_count = sum(1 for _, success in results if success)
    logger.info(f"Successfully merged {success_count}/{len(areas)} areas.")

    # Log
    logger.info(f"Finished processing.")

    return None

def workflow_hostrada(parameters: dict, data: dict, client: Client, logger: logging.Logger) -> None:
    """
    Run the Hostrada workflow. This workflow calculates the following spatial statistics:
    - min
    - mean
    - max
    - standard deviation
    - 10th, 20th, 30th, 40th, 50th, 60th, 70th, 80th, 90th percentiles
    of the Hostrada data for each input area.  
    Additionally, the clipped netCDF files can be saved in the output directory.

    """
    # Parse the Hostrada variables
    variables = parameters["variables"]

    # Check if the variables are a list
    if not isinstance(variables, list):
        variables = [variables]

    if variables == ["all"]:
        variables = ["cloud_cover", "wind_speed", "wind_direction", "air_temperature_mean", "dew_point_temperature", "relative_humidity", "water_vapor_mixing_ratio", "air_pressure_sea_level", "air_pressure_surface", "global_shortwave_radiation", "urban_heat_island_intensity"]
        
    # Run the workflow for each variable
    for variable in variables:
        workflow_hostrada_variable(parameters, data, variable, client, logger)

def merge_output_single_area(area, merge_nc: bool, merge_csv: bool, n_files_expected: int) -> Tuple[str, bool]:
    """
    Merge clipped and aggregated files for a single area.  
    With merge_nc=True, the clipped files are merged into a single NetCDF file.  
    With merge_csv=True, the aggregated files are merged into a single CSV file.  
    The parameter n_files_expected is the number of files that are expected to be merged. If 
    there are less files, an error is logged.
    
    """
    try:
        if merge_nc:
            # Process NetCDF files
            clipped_files = sorted([f for f in area.output_path.glob(f"{area.id}_*.nc")])
            if len(clipped_files) == n_files_expected:
                with xr.open_mfdataset(clipped_files) as clipped_merged:
                    clipped_merged.to_netcdf(area.output_path / f"{area.id}_clipped.nc")
                # Only remove after successful save
                for f in clipped_files:
                    f.unlink()
            else:
                logger.error(f"{area.id} --- Expected {n_files_expected} clipped files, but found {len(clipped_files)}.")
        
        if merge_csv:
            # Process CSV files
            agg_files = sorted([f for f in area.output_path.glob(f"{area.id}_*.csv")])
            if len(agg_files) == n_files_expected:
                chunks = []
                for f in agg_files:
                    chunks.append(pd.read_csv(f))
                agg_merged = pd.concat(chunks, ignore_index=True)
                agg_merged = agg_merged.sort_values("time")
                agg_merged.to_csv(area.output_path / f"{area.id}_aggregated.csv", index=False)
                # Only remove after successful save
                for f in agg_files:
                    f.unlink()
            else:
                logger.error(f"{area.id} --- Expected {n_files_expected} aggregated files, but found {len(agg_files)}.")
            
        return area.id, True
    except Exception as e:
        logger.error(f"{area.id} --- Error merging files: {e}")
        return area.id, False
    finally:
        # Ensure memory cleanup
        if 'clipped_merged' in locals():
            del clipped_merged
        if 'agg_merged' in locals():
            del agg_merged


if __name__ == "__main__":
    # Initialize dask-mpi cluster on all processes.
    initialize()

    # Create a Dask client (all processes join the cluster)
    client = Client() 

    # Set parameters and data.
    parameters = {
        "areas_id_column": "gauge_id",
        "variables": os.environ.get("HOSTRADA_VARIABLE", "all"), # variable can be set via environment variable (set in SLURM script)
        "skip_exist": True,
        "save_nc": False,
        "save_csv": True,
        #"batch_size": 400, # find a value that gives you a good performance
        "output_dir": "/pfs/work7/workspace/scratch/qt7760-s2a/out/"
    }
    data = {
        "areas": "/home/kit/iwu/qt7760/datasets/CAMELS_DE/CAMELS_DE_catchment_boundaries/catchments/CAMELS_DE_catchments.gpkg",
        "hostrada_stgrid_air_temperature_mean": "/pfs/work7/workspace/scratch/qt7760-s2a/in/hostrada/air_temperature_mean/*/*.nc",
        "hostrada_stgrid_dew_point_temperature": "/pfs/work7/workspace/scratch/qt7760-s2a/in/hostrada/dew_point_temperature/*/*.nc",
        "hostrada_stgrid_relative_humidity": "/pfs/work7/workspace/scratch/qt7760-s2a/in/hostrada/relative_humidity/*/*.nc",
        "hostrada_stgrid_water_vapor_mixing_ratio": "/pfs/work7/workspace/scratch/qt7760-s2a/in/hostrada/water_vapor_mixing_ratio/*/*.nc",
        "hostrada_stgrid_air_pressure_sea_level": "/pfs/work7/workspace/scratch/qt7760-s2a/in/hostrada/air_pressure_sea_level/*/*.nc",
        "hostrada_stgrid_air_pressure_surface": "/pfs/work7/workspace/scratch/qt7760-s2a/in/hostrada/air_pressure_surface/*/*.nc",
        "hostrada_stgrid_global_shortwave_radiation": "/pfs/work7/workspace/scratch/qt7760-s2a/in/hostrada/global_shortwave_radiation/*/*.nc",
        "hostrada_stgrid_cloud_cover": "/pfs/work7/workspace/scratch/qt7760-s2a/in/hostrada/cloud_cover/*/*.nc",
        "hostrada_stgrid_wind_speed": "/pfs/work7/workspace/scratch/qt7760-s2a/in/hostrada/wind_speed/*/*.nc",
        "hostrada_stgrid_wind_direction": "/pfs/work7/workspace/scratch/qt7760-s2a/in/hostrada/wind_direction/*/*.nc",
        "hostrada_stgrid_urban_heat_island_intensity": "/pfs/work7/workspace/scratch/qt7760-s2a/in/hostrada/urban_heat_island_intensity/*/*.nc",
    }
    
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
    
    workflow_hostrada(parameters, data, client, logger)
    
    client.close()