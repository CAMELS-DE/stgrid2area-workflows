import os

import geopandas as gpd
import xarray as xr
from stgrid2area import LocalDaskProcessor, geodataframe_to_areas
from dask.distributed import Client

from json2args import logger


def workflow_hyras_variable(parameters: dict, data: dict, variable: str) -> None:
    """
    Run the HYRAS workflow for a specific variable. This workflow calculates the following spatial statistics:
    - min
    - mean
    - max
    - standard deviation
    - 10th, 20th, 30th, 40th, 50th, 60th, 70th, 80th, 90th percentiles
    of the variable data from HYRAS data for each input area.  
    Additionally, the clipped netCDF files are saved in the output directory.

    """
    # Variable name mapping (air_temperature_max  air_temperature_mean  air_temperature_min  global_radiation  humidity  precipitation)
    variable_mapping = {
        "air_temperature_max": "tasmax",
        "air_temperature_mean": "tas",
        "air_temperature_min": "tasmin",
        "global_radiation": "rsds",
        "humidity": "hurs",
        "precipitation": "pr"
    }

    var_abbr = variable_mapping[variable]

    # Read HYRAS data
    hyras = xr.open_mfdataset(data[f"hyras_stgrid_{variable}"], combine="by_coords", chunks="auto").unify_chunks()

    # HYRAS data is in EPSG:3034
    hyras = hyras.rio.write_crs("EPSG:3034")

    # HYRAS: drop all variables that do not have all 3 dimensions x, y, time (time_bnds, x_bnds, y_bnds), only having the temporal dimension is fine
    hyras = hyras.drop_vars(["time_bnds", "x_bnds", "y_bnds"])
    
    # Remove the grid_mapping key from the variable's attributes (problems with xarray)
    hyras[var_abbr].attrs.pop("grid_mapping", None)
    
    # Read the areas
    gdf_areas = gpd.read_file(data["areas"])

    # Reproject the areas to the CRS of the HYRAS data
    gdf_areas = gdf_areas.to_crs(hyras.rio.crs)

    # Convert the geodataframe to stgrid2area areas
    areas = geodataframe_to_areas(areas=gdf_areas, id_column=parameters["areas_id_column"], output_dir=f"/out/hyras/{variable}")

    # Initialize the LocalDaskProcessor
    processor = LocalDaskProcessor(
        areas=areas,
        stgrid=hyras,
        variables=var_abbr,
        method="fallback_xarray",
        operations=["min", "mean", "max", "stdev", "quantile(q=0.1)", "quantile(q=0.2)", "quantile(q=0.3)", "quantile(q=0.4)", "quantile(q=0.5)", "quantile(q=0.6)", "quantile(q=0.7)", "quantile(q=0.8)", "quantile(q=0.9)"],
        n_workers=None, # will automatically be os.cpu_count()
        skip_exist=True,
        logger=logger
    )

    # Log
    logger.info(f"Starting processing variable {variable} from HYRAS data.")

    # Run the processor
    processor.run()

    # alter file permissions (from docker)
    os.system("chmod -R 777 /out/hyras")

def workflow_hyras(parameters: dict, data: dict) -> None:
    """
    Run the HYRAS workflow. This workflow calculates the following spatial statistics:
    - min
    - mean
    - max
    - standard deviation
    - 10th, 20th, 30th, 40th, 50th, 60th, 70th, 80th, 90th percentiles
    of the HYRAS data for each input area.  
    Additionally, the clipped netCDF files are saved in the output directory.

    """
    # Parse the HYRAS variables
    variables = parameters["variables"]

    # Check if the variables are a list
    if not isinstance(variables, list):
        variables = [variables]

    if variables == ["all"]:
        variables = ["air_temperature_max", "air_temperature_mean", "air_temperature_min", "global_radiation", "humidity", "precipitation"]

    # Run the workflow for each variable
    for variable in variables:
        workflow_hyras_variable(parameters, data, variable)