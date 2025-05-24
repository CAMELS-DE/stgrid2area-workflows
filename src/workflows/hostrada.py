import os
from glob import glob
from typing import Tuple
import concurrent.futures

import geopandas as gpd
import xarray as xr
import pandas as pd
from stgrid2area import LocalDaskProcessor, geodataframe_to_areas

from json2args import logger


def workflow_hostrada_variable(parameters: dict, data: dict, variable: str) -> None:
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
    
    # Read the areas
    gdf_areas = gpd.read_file(data["areas"])

    # Reproject the areas to the CRS of the Hostrada data
    gdf_areas = gdf_areas.to_crs("EPSG:3034")

    # Convert the geodataframe to stgrid2area areas
    areas = geodataframe_to_areas(areas=gdf_areas, id_column=parameters["areas_id_column"], output_dir=f"/out/hostrada/{variable}", sort_by_proximity=True)

    # Initialize the LocalDaskProcessor
    processor = LocalDaskProcessor(
        areas=areas,
        stgrid=hostrada,
        variables=var_abbr,
        method="fallback_xarray",
        operations=["min", "mean", "max", "stdev", "quantile(q=0.1)", "quantile(q=0.2)", "quantile(q=0.3)", "quantile(q=0.4)", "quantile(q=0.5)", "quantile(q=0.6)", "quantile(q=0.7)", "quantile(q=0.8)", "quantile(q=0.9)"],
        n_workers=None, # will automatically be os.cpu_count()
        skip_exist=parameters["skip_exist"],
        batch_size=parameters.get("batch_size", None),
        save_nc=parameters["save_nc"],
        save_csv=parameters["save_csv"],
        logger=logger
    )

    # Log
    logger.info(f"Starting processing variable {variable} from Hostrada data.")

    # Run the processor
    processor.run()

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

    # alter file permissions (from docker)
    os.system("chmod -R 777 /out/radklim/precipitation")

    return None

def workflow_hostrada(parameters: dict, data: dict) -> None:
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
        workflow_hostrada_variable(parameters, data, variable)

    # alter file permissions (from docker)
    os.system("chmod -R 777 /out/hostrada")

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
    