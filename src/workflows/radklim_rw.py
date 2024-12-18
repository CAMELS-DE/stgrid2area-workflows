import os
from glob import glob
import concurrent.futures
from typing import Tuple

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
    # this is the RADOLAN grid wkt
    wkt_radolan = 'PROJCS["Stereographic_North_Pole",GEOGCS["GCS_unnamed ellipse",DATUM["D_unknown",SPHEROID["Unknown",6370040,0]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Stereographic_North_Pole"],PARAMETER["standard_parallel_1",60],PARAMETER["central_meridian",10],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["Meter",1]]'

    # radklim = []
    # radklim_files = glob(data[f"radklim_rw_stgrid"])

    # for radklim_file in radklim_files:
    #     radklim_chunk = xr.open_dataset(radklim_file, chunks="auto").unify_chunks()
    #     radklim_chunk.rio.write_crs(CRS.from_wkt(wkt_radolan), inplace=True)

    #     # Remove the grid_mapping key from the variable's attributes (problems with xarray)
    #     radklim_chunk["RR"].attrs.pop("grid_mapping", None)

    #     radklim.append(radklim_chunk)

    radklim = xr.open_mfdataset(data[f"radklim_rw_stgrid"], chunks="auto", combine="by_coords").unify_chunks()
    radklim.rio.write_crs(CRS.from_wkt(wkt_radolan), inplace=True)
    # Remove the grid_mapping key from the variable's attributes (problems with xarray)
    radklim["RR"].attrs.pop("grid_mapping", None)

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

    timeout = 3600 # 1 hour timeout per area

    # Merge the clipped and aggregated files for each area in parallel    
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = []

        future_to_area = {executor.submit(merge_output_single_area, area, len(radklim_files)): area for area in areas}

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

def merge_output_single_area(area, n_files_expected: int) -> Tuple[str, bool]:
    """
    Merge clipped and aggregated files for a single area.  
    The parameter n_files_expected is the number of files that are expected to be merged. If 
    there are less files, an error is logged.
    
    """
    try:
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
    