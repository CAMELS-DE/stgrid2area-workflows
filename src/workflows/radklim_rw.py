import os

import geopandas as gpd
import pandas as pd
import xarray as xr
from stgrid2area import LocalDaskProcessor, geodataframe_to_areas
from pyproj import CRS

from json2args import logger

def workflow_radklim_rw(parameters: dict, data: dict) -> None:
    """
    Run the RADKLIM-RW workflow. This workflow calculates the following spatial statistics:
    - min
    - mean
    - max
    - standard deviation
    - 10th, 20th, 30th, 40th, 50th, 60th, 70th, 80th, 90th percentiles
    of the RADKLIM-RW data for each input area.  
    Additionally, the clipped netCDF files are saved in the output directory.

    """
    # Read RADKLIM data
    radklim = xr.open_mfdataset(data[f"radklim_rw_stgrid"], combine="by_coords", chunks="auto").unify_chunks()

    # this is the RADOLAN grid wkt
    wkt_radolan = 'PROJCS["Stereographic_North_Pole",GEOGCS["GCS_unnamed ellipse",DATUM["D_unknown",SPHEROID["Unknown",6370040,0]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Stereographic_North_Pole"],PARAMETER["standard_parallel_1",60],PARAMETER["central_meridian",10],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["Meter",1]]'
    radklim.rio.write_crs(CRS.from_wkt(wkt_radolan), inplace=True)

    # Remove the grid_mapping key from the variable's attributes (problems with xarray)
    radklim["RR"].attrs.pop("grid_mapping", None)

    # Group RADKLIM data by month to avoid memory issues when processing big xaaray datasets
    radklim = [ds[1] for ds in radklim.groupby("time.month")]

    # Read the areas
    gdf_areas = gpd.read_file(data["areas"])

    # Reproject the areas to the CRS of the E-OBS data
    gdf_areas = gdf_areas.to_crs(wkt_radolan)

    # Convert the geodataframe to stgrid2area areas
    areas = geodataframe_to_areas(areas=gdf_areas, id_column=parameters["areas_id_column"], output_dir=f"/out/radklim/precipitation", sort_by_proximity=True)

    # Initialize the LocalDaskProcessor
    processor = LocalDaskProcessor(
        areas=areas,
        stgrid=radklim,
        variable="RR",
        method="fallback_xarray",
        operations=["min", "mean", "max", "stdev", "quantile(q=0.1)", "quantile(q=0.2)", "quantile(q=0.3)", "quantile(q=0.4)", "quantile(q=0.5)", "quantile(q=0.6)", "quantile(q=0.7)", "quantile(q=0.8)", "quantile(q=0.9)"],
        n_workers=None, # will automatically be os.cpu_count()
        skip_exist=parameters["skip_exist"],
        batch_size=parameters.get("batch_size", None),
        logger=logger
    )

    # Log
    logger.info(f"Starting processing RADKLIM-RW precipitation data.")

    # Run the processor
    processor.run()

    # Log
    logger.info(f"Merging the clipped and aggregated files for each area.")

    # Merge the clipped and aggregated files (result of grouping the READKLIM data by month)
    for area in areas:
        # find the clipped netcdf files
        clipped_files = sorted([f for f in area.output_path.glob(f"{area.id}_*.nc")])

        # merge the clipped files
        clipped_merged = xr.open_mfdataset(clipped_files)

        # save the merged file
        clipped_merged.to_netcdf(area.output_path / f"{area.id}_clipped.nc")

        # remove the individual files
        for f in clipped_files:
            f.unlink()

        # find the aggregated csv files
        agg_files = sorted([f for f in area.output_path.glob(f"{area.id}_*.csv")])

        # merge the csv files
        agg_merged = pd.concat([pd.read_csv(f) for f in agg_files], ignore_index=True)

        # sort the values by time
        agg_merged = agg_merged.sort_values("time")

        # save the merged file
        agg_merged.to_csv(area.output_path / f"{area.id}_aggregated.csv", index=False)

        # remove the individual files
        for f in agg_files:
            f.unlink()

    # Log
    logger.info(f"Finished processing.")

    # alter file permissions (from docker)
    os.system("chmod -R 777 /out/radklim/precipitation")