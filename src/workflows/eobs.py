import os

import geopandas as gpd
import xarray as xr
from stgrid2area import LocalDaskProcessor, geodataframe_to_areas

from json2args import logger


def workflow_eobs_variable(parameters: dict, data: dict, variable: str) -> None:
    """
    Run the E-OBS workflow for a specific variable. This workflow calculates the following spatial statistics:
    - min
    - mean
    - max
    - standard deviation
    - 10th, 20th, 30th, 40th, 50th, 60th, 70th, 80th, 90th percentiles
    of the variable data from E-OBS data for each input area.  
    Additionally, the clipped netCDF files are saved in the output directory.

    """
    # Variable name mapping
    variable_mapping = {
        "precipitation": "rr",
        "radiation": "qq",
        "humidity": "hu",
        "mean_temperature": "tg",
        "max_temperature": "tx",
        "min_temperature": "tn",
        "air_pressure_sea_level": "pp",
        "wind_speed": "fg"       
    }

    var_abbr = variable_mapping[variable]

    # Read E-OBS data
    eobs = xr.open_mfdataset(data[f"eobs_stgrid_{variable}"], combine="by_coords", chunks="auto").unify_chunks()

    # E-OBS data is in EPSG:4326
    eobs = eobs.rio.write_crs("EPSG:4326")

    # Read the areas
    gdf_areas = gpd.read_file(data["areas"])

    # Reproject the areas to the CRS of the E-OBS data
    gdf_areas = gdf_areas.to_crs(eobs.rio.crs)

    # Convert the geodataframe to stgrid2area areas
    areas = geodataframe_to_areas(areas=gdf_areas, id_column=parameters["areas_id_column"], output_dir=f"/out/eobs/{variable}", sort_by_proximity=True)

    # Initialize the LocalDaskProcessor
    processor = LocalDaskProcessor(
        areas=areas,
        stgrid=eobs,
        variable=var_abbr,
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
    logger.info(f"Starting processing variable {variable} from E-OBS data.")

    # Run the processor
    processor.run()

    # alter file permissions (from docker)
    os.system("chmod -R 777 /out/eobs")

def workflow_eobs(parameters: dict, data: dict) -> None:
    """
    Run the E-OBS workflow. This workflow calculates the following spatial statistics:
    - min
    - mean
    - max
    - standard deviation
    - 10th, 20th, 30th, 40th, 50th, 60th, 70th, 80th, 90th percentiles
    of the E-OBS data for each input area.  
    Additionally, the clipped netCDF files are saved in the output directory.

    """
    # Parse the E-OBS variables
    variables = parameters["variables"]

    # Check if the variables are a list
    if not isinstance(variables, list):
        variables = [variables]

    if variables == ["all"]:
        variables = ["precipitation", "radiation", "humidity", "mean_temperature", "max_temperature", "min_temperature", "air_pressure_sea_level", "wind_speed"]

    # Run the workflow for each variable
    for variable in variables:
        workflow_eobs_variable(parameters, data, variable)