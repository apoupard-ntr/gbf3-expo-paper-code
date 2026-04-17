"""
Master script for processing agricultural data scenarios
Consolidates all processing steps: reprojection, masking, intersection, and data manipulation
"""

import geopandas as gpd
import rasterio
from rasterio.mask import mask
from rasterio.crs import CRS
from rasterio.warp import reproject, Resampling
import rasterio as rio
import rasterio.features as rio_features
import numpy as np
import pandas as pd
import os
import re
from pathlib import Path
from shapely.geometry import mapping
from shapely.ops import unary_union
from shapely.validation import make_valid

# ============================================================================
# CONFIGURATION
# ============================================================================

# Get SCENARIO from command-line argument or use default
SCENARIO = "Global_30_0.4"  # Default scenario

# Define directory structure based on scenario (scenarios are in multiscenarios folder)
SCENARIO_DIR = f"multiscenarios/{SCENARIO}"
MASKS_DIR = f"{SCENARIO_DIR}/masksShen"
MASKS_CROP_DIR = f"{MASKS_DIR}/crop"
MASKS_LIVESTOCK_DIR = f"{MASKS_DIR}/livestock"
MASKS_CROP_COUNTRIES_DIR = f"{MASKS_CROP_DIR}/countries"
MASKS_LIVESTOCK_COUNTRIES_DIR = f"{MASKS_LIVESTOCK_DIR}/countries"
SHENRECOMPUTE_DIR = f"{MASKS_DIR}/shenrecompute"
RESULT_CROP_DIR = f"{SCENARIO_DIR}/resultShen"
RESULT_CROP_MERGED_DIR = f"{RESULT_CROP_DIR}/merged_data_crop"
RESULT_LS_DIR = f"{SCENARIO_DIR}/resultslsShen"
RESULT_LS_MERGED_DIR = f"{RESULT_LS_DIR}/merged_data_ls"

# Source raster
SOURCE_RASTER = f"data/Shen_masks/{SCENARIO}.tif"
TARGET_RASTER_CROP = "data/Shen_masks/redimension_maps/Allanetal2022_scen1_crop.tif"
TARGET_RASTER_LS = "data/Shen_masks/redimension_maps/Allanetal2022_scen1_livestock.tif"

# Output rasters after reprojection
REPROJECTED_CROP_RASTER = f"{MASKS_CROP_DIR}/{SCENARIO}_reprojected.tif"
REPROJECTED_LS_RASTER = f"{MASKS_LIVESTOCK_DIR}/{SCENARIO}_reprojected.tif"

# Data files
FOLLOW_STATUS_CROP = f"{SCENARIO_DIR}/follow_status.csv"
FOLLOW_STATUS_LS = f"{SCENARIO_DIR}/follow_statusls.csv"
LIST_CROP_FILE = f"{SCENARIO_DIR}/listcropShen.csv"

WORLD_BOUNDARIES_PATH = r"data/worldbound/WAB/world-administrative-boundaries.shp"

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def ensure_directory_exists(directory):
    """Create directory if it doesn't exist"""
    Path(directory).mkdir(parents=True, exist_ok=True)
    print(f"✓ Directory ready: {directory}")

def reproject_and_convert_values_to_one(source_path, target_path, output_path):
    """
    Reproject source raster to match target grid and convert values (1 and 2 → 1)
    """
    print(f"  Reprojecting {source_path}...")
    source_crs = CRS.from_string("ESRI:54009")  # Mollweide
    target_crs = CRS.from_epsg(4326)  # WGS84
    
    with rasterio.open(target_path) as target:
        target_transform = target.transform
        target_height = target.height
        target_width = target.width
        
        with rasterio.open(source_path) as src:
            src_data = src.read(1)
            dst_data = np.zeros((target_height, target_width), dtype=src.dtypes[0])
            
            reproject(
                source=src_data,
                destination=dst_data,
                src_transform=src.transform,
                src_crs=source_crs,
                dst_transform=target_transform,
                dst_crs=target_crs,
                resampling=Resampling.nearest
            )
            
            # Convert values (1 and 2 → 1)
            dst_data = np.where((dst_data == 1) | (dst_data == 2), 1, dst_data)
            
            with rasterio.open(
                output_path, 'w',
                driver='GTiff',
                height=target_height,
                width=target_width,
                count=1,
                dtype=dst_data.dtype,
                crs=target_crs,
                transform=target_transform,
                nodata=src.nodata if src.nodata else None
            ) as dst:
                dst.write(dst_data, 1)
    
    print(f"  ✓ Saved: {output_path}")

def clip_raster_to_country(country_code, mask_path, output_path):
    """
    Clip raster to country boundaries
    """
    countries = gpd.read_file(WORLD_BOUNDARIES_PATH)
    country_shape = countries[countries['iso3'] == country_code]
    
    if country_shape.empty:
        raise ValueError(f"Le code ISO '{country_code}' n'est pas valide ou introuvable.")

    with rasterio.open(mask_path) as src:
        if country_shape.crs != src.crs:
            country_shape = country_shape.to_crs(src.crs)

        out_image, out_transform = mask(src, country_shape.geometry, crop=True)

        out_meta = src.meta.copy()
        out_meta.update({
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform
        })

        with rasterio.open(output_path, "w", **out_meta) as dest:
            dest.write(out_image)

def sum_tiff_pixels(file_path):
    """Calculate sum of all pixels in a TIFF file"""
    with rio.open(file_path) as src:
        band1 = src.read(1)
        band1 = np.nan_to_num(band1, nan=0.0)
        band1[band1 < 0] = 0
        pixel_sum = np.sum(band1)
    return pixel_sum

def multiply_and_sum_rasters(original_tif_path, mask_tif_path):
    """Multiply two rasters and sum the result"""
    with rio.open(original_tif_path) as src_orig:
        with rio.open(mask_tif_path) as src_mask:
            if src_orig.shape != src_mask.shape or src_orig.transform != src_mask.transform:
                mask_data_reprojected = np.empty(src_orig.shape, dtype=np.float32)
                reproject(
                    source=rio.band(src_mask, 1),
                    destination=mask_data_reprojected,
                    src_transform=src_mask.transform,
                    src_crs=src_mask.crs,
                    dst_transform=src_orig.transform,
                    dst_crs=src_orig.crs,
                    resampling=Resampling.nearest
                )
            else:
                mask_data_reprojected = src_mask.read(1)

            original_data = src_orig.read(1)
            original_data = np.nan_to_num(original_data, nan=0.0)
            original_data[original_data < 0] = 0
            mask_data_reprojected = np.nan_to_num(mask_data_reprojected, nan=0.0)
            mask_data_reprojected[mask_data_reprojected < 0] = 0

            multiplied_data = original_data * mask_data_reprojected
            total_sum = multiplied_data.sum()

    return total_sum

# ============================================================================
# STEP 1: REPROJECTION
# ============================================================================

def step1_reproject_rasters():
    """
    Step 1: Reproject the scenario raster into crop and livestock versions
    """
    print("\n" + "="*70)
    print("STEP 1: REPROJECTING RASTERS")
    print("="*70)
    
    ensure_directory_exists(MASKS_CROP_DIR)
    ensure_directory_exists(MASKS_LIVESTOCK_DIR)
    
    print(f"Source raster: {SOURCE_RASTER}")
    print(f"Target (crop): {TARGET_RASTER_CROP}")
    print(f"Target (livestock): {TARGET_RASTER_LS}")
    
    # Reproject to crop
    reproject_and_convert_values_to_one(SOURCE_RASTER, TARGET_RASTER_CROP, REPROJECTED_CROP_RASTER)
    
    # Reproject to livestock
    reproject_and_convert_values_to_one(SOURCE_RASTER, TARGET_RASTER_LS, REPROJECTED_LS_RASTER)

# ============================================================================
# STEP 2: CREATE COUNTRY MASKS
# ============================================================================

def step2_create_country_masks():
    """
    Step 2: Clip reprojected rasters by country
    """
    print("\n" + "="*70)
    print("STEP 2: CREATING COUNTRY-LEVEL MASKS")
    print("="*70)
    
    ensure_directory_exists(MASKS_CROP_COUNTRIES_DIR)
    ensure_directory_exists(MASKS_LIVESTOCK_COUNTRIES_DIR)
    
    # Get country list from Allan masks (pattern: Allan2022_mask_crop_XXX.tif)
    allan_mask_dir = "allan/masksAllan/crop/countries"
    pattern = re.compile(r"Allan2022_mask_crop_([A-Z]{3})\.tif")
    
    country_codes = []
    if os.path.exists(allan_mask_dir):
        for filename in os.listdir(allan_mask_dir):
            match = pattern.match(filename)
            if match:
                country_codes.append(match.group(1))
    else:
        print(f"⚠ Allan mask directory not found: {allan_mask_dir}")
        print("  Using empty country list")
    
    print(f"Processing {len(country_codes)} countries...")
    
    for country_code in country_codes:
        print(f"  {country_code}...", end=" ")
        try:
            # Crop masks
            output_crop = f"{MASKS_CROP_COUNTRIES_DIR}/{SCENARIO}_mask_crop_{country_code}.tif"
            clip_raster_to_country(country_code, REPROJECTED_CROP_RASTER, output_crop)
            
            # Livestock masks
            output_ls = f"{MASKS_LIVESTOCK_COUNTRIES_DIR}/{SCENARIO}_mask_livestock_{country_code}.tif"
            clip_raster_to_country(country_code, REPROJECTED_LS_RASTER, output_ls)
            
            print("✓")
        except Exception as e:
            print(f"✗ Error: {e}")

# ============================================================================
# STEP 3: PROCESS CROPGRIDS (INTERSECTION)
# ============================================================================

def process_tiff_files_clipped_crops(folder_path, mask_tif_path, output_csv_path):
    """
    Sum clipped pixels for crop files
    """
    resultsclip = {}
    results = {}
    pattern = re.compile(r"CROPGRIDSv1\.08_(.*?)_(.*?)\.tif")
    
    for file_name in os.listdir(folder_path):
        if file_name.endswith(('.tif', '.tiff')):
            match = pattern.match(file_name)
            if match:
                crop_name = match.group(1)
                tif_path = os.path.join(folder_path, file_name)
                try:
                    pixel_sum = sum_tiff_pixels(tif_path)
                    if pixel_sum == 0:
                        pixel_sum_clip = 0
                    else:
                        pixel_sum_clip = multiply_and_sum_rasters(tif_path, mask_tif_path)
                    results[crop_name] = pixel_sum
                    resultsclip[crop_name] = pixel_sum_clip
                except Exception as e:
                    print(f"    Failed to process {file_name}: {e}")
    
    df = pd.DataFrame({
        'clipped_PA_surf': resultsclip,
        'total_surf': results
    })
    df.index.name = 'Crop'
    df.to_csv(output_csv_path)

def process_tiff_files_clipped_livestock(folder_path, mask_tif_path, output_csv_path):
    """
    Sum clipped pixels for livestock files
    """
    resultsclip = {}
    results = {}
    pattern = re.compile(r"5_(.*?)_2015_Da_BRAcrop\.tif")
    
    for file_name in os.listdir(folder_path):
        if file_name.endswith(('.tif', '.tiff')):
            match = pattern.match(file_name)
            if match:
                animal_name = match.group(1)
                tif_path = os.path.join(folder_path, file_name)
                try:
                    pixel_sum = sum_tiff_pixels(tif_path)
                    if pixel_sum == 0:
                        pixel_sum_clip = 0
                    else:
                        pixel_sum_clip = multiply_and_sum_rasters(tif_path, mask_tif_path)
                    results[animal_name] = pixel_sum
                    resultsclip[animal_name] = pixel_sum_clip
                except Exception as e:
                    print(f"    Failed to process {file_name}: {e}")
    
    df = pd.DataFrame({
        'clipped_PA_surf': resultsclip,
        'total_surf': results
    })
    df.index.name = 'Animal'
    df.to_csv(output_csv_path)

def step3_process_intersections():
    """
    Step 3: Generate intersection outputs
    """
    print("\n" + "="*70)
    print("STEP 3: PROCESSING CROPGRID INTERSECTIONS")
    print("="*70)
    
    ensure_directory_exists(RESULT_CROP_DIR)
    ensure_directory_exists(RESULT_LS_DIR)
    
    # Load country list from follow_status
    try:
        df = pd.read_csv(LIST_CROP_FILE, delimiter=";")
    except FileNotFoundError:
        print(f"⚠ File not found: {LIST_CROP_FILE}")
        print("  Using empty country list")
        return
    
    df_filtered = df[df["cropgrid"] == 1.0]
    country_list = df_filtered["ISO"].tolist()
    
    print(f"Processing {len(country_list)} countries for crops and livestock...")
    
    # Process crops
    print("\nCrops:")
    fllw_crop = {}
    for country_name in country_list:
        folder_path = f"data/currentPA/{country_name}/cropgrid/"
        if not os.path.exists(folder_path):
            continue
        
        tif_path = ""
        for file_name in os.listdir(folder_path):
            if file_name.lower().endswith('.tif'):
                tif_path = os.path.join(folder_path, file_name)
                break
        
        if tif_path == "":
            continue
        
        mask_path = f"{MASKS_CROP_COUNTRIES_DIR}/{SCENARIO}_mask_crop_{country_name}.tif"
        output_csv_path = f"{RESULT_CROP_DIR}/output_{country_name}.csv"
        
        if not os.path.exists(mask_path):
            continue
        
        print(f"  {country_name}...", end=" ")
        try:
            process_tiff_files_clipped_crops(folder_path, mask_path, output_csv_path)
            fllw_crop[country_name] = 1
            print("✓")
        except Exception as e:
            fllw_crop[country_name] = 0
            print(f"✗ {e}")
    
    # Save follow status for crops
    fllwdf_crop = pd.DataFrame.from_dict(fllw_crop, orient='index', columns=['Status'])
    fllwdf_crop.to_csv(FOLLOW_STATUS_CROP)
    
    # Process livestock
    print("\nLivestock:")
    fllw_ls = {}
    for country_name in country_list:
        folder_path = f"data/currentPA/{country_name}/livestock/"
        if not os.path.exists(folder_path):
            continue
        
        tif_path = ""
        for file_name in os.listdir(folder_path):
            if file_name.lower().endswith('.tif'):
                tif_path = os.path.join(folder_path, file_name)
                break
        
        if tif_path == "":
            continue
        
        mask_path = f"{MASKS_LIVESTOCK_COUNTRIES_DIR}/{SCENARIO}_mask_livestock_{country_name}.tif"
        output_csv_path = f"{RESULT_LS_DIR}/output_{country_name}.csv"
        
        if not os.path.exists(mask_path):
            continue
        
        print(f"  {country_name}...", end=" ")
        try:
            process_tiff_files_clipped_livestock(folder_path, mask_path, output_csv_path)
            fllw_ls[country_name] = 1
            print("✓")
        except Exception as e:
            fllw_ls[country_name] = 0
            print(f"✗ {e}")
    
    # Save follow status for livestock
    fllwdf_ls = pd.DataFrame.from_dict(fllw_ls, orient='index', columns=['Status'])
    fllwdf_ls.to_csv(FOLLOW_STATUS_LS)

# ============================================================================
# STEP 4: DATA MANIPULATION
# ============================================================================

def step4_manipulate_data():
    """
    Step 4: Aggregate and manipulate data to generate final outputs
    """
    print("\n" + "="*70)
    print("STEP 4: DATA MANIPULATION AND AGGREGATION")
    print("="*70)
    
    ensure_directory_exists(RESULT_CROP_MERGED_DIR)
    ensure_directory_exists(RESULT_LS_MERGED_DIR)
    
    # Load reference data
    print("Loading reference datasets...", end=" ")
    countrygroup_df = pd.read_csv('data/FAOSTAT/FAOSTAT_countrygroup_filtered.csv')
    cropyield_df = pd.read_csv('data/FAOSTAT/FAOSTAT_cropyield.csv', delimiter=";", dtype={"Item Code (CPC)": str})
    prodprices_df = pd.read_csv('data/FAOSTAT/FAOSTAT_prodprices_USD.csv', delimiter=";", dtype={"Item Code (CPC)": str})
    conv_df = pd.read_csv('data/FAOSTAT/FAOITEM_corr.csv', delimiter=";", dtype={"Item Code (CPC)": str}, encoding='latin-1')
    avg_yep_by_group_df = pd.read_csv('data/FAOSTAT/COUMPUTED_average_yep_by_group.csv', dtype={"Item Code (CPC)": str})
    print("✓")
    
    # Load country list
    try:
        df = pd.read_csv(LIST_CROP_FILE, delimiter=";")
    except FileNotFoundError:
        print(f"⚠ File not found: {LIST_CROP_FILE}")
        return
    
    df_filtered = df[df["cropgrid"] == 1.0]
    country_list = df_filtered["ISO"].tolist()
    
    # ========== Step 4a: Merge crop data ==========
    print("\n4a) Merging crop data...", end=" ")
    
    def get_country_group(country_code):
        group = countrygroup_df[countrygroup_df['ISO3 Code'] == country_code]['Country Group']
        return group.iloc[0] if not group.empty else None
    
    merged_df_crops = pd.DataFrame(columns=['Country Code (ISO3)', 'Crop', 'Item Code (CPC)', 
                                            'clipped_PA_surf', 'total_surf', 'yield', 'price'])
    
    for country_code in country_list:
        file_path = f"{RESULT_CROP_DIR}/output_{country_code}.csv"
        if not os.path.exists(file_path):
            continue
        
        country_data = pd.read_csv(file_path)
        country_data['Country Code (ISO3)'] = country_code
        country_data = country_data.merge(conv_df, on='Crop', how='left')
        
        country_data['yield'] = None
        country_data['price'] = None
        
        for index, row in country_data.iterrows():
            item_code = row['Item Code (CPC)']
            if pd.isna(item_code):
                continue
            
            yield_value = cropyield_df[(cropyield_df['Area Code (ISO3)'] == country_code) & 
                                       (cropyield_df['Item Code (CPC)'] == item_code)]['Value']
            price_value = prodprices_df[(prodprices_df['Area Code (ISO3)'] == country_code) & 
                                        (prodprices_df['Item Code (CPC)'] == item_code)]['Value']
            
            if not yield_value.empty:
                country_data.at[index, 'yield'] = yield_value.iloc[0]
            else:
                country_group = get_country_group(country_code)
                avg_yield = avg_yep_by_group_df[(avg_yep_by_group_df['Country Group'] == country_group) & 
                                                (avg_yep_by_group_df['Item Code (CPC)'] == item_code)]['average yield Value']
                if not avg_yield.empty:
                    country_data.at[index, 'yield'] = avg_yield.iloc[0]
            
            if not price_value.empty:
                country_data.at[index, 'price'] = price_value.iloc[0]
            else:
                country_group = get_country_group(country_code)
                avg_price = avg_yep_by_group_df[(avg_yep_by_group_df['Country Group'] == country_group) & 
                                                (avg_yep_by_group_df['Item Code (CPC)'] == item_code)]['average producer price Value']
                if not avg_price.empty:
                    country_data.at[index, 'price'] = avg_price.iloc[0]
        
        merged_df_crops = pd.concat([merged_df_crops, country_data[['Country Code (ISO3)', 'Crop', 'Item Code (CPC)', 
                                                                     'clipped_PA_surf', 'total_surf', 'yield', 'price']]])
    
    merged_df_crops.reset_index(drop=True, inplace=True)
    merged_df_crops.to_csv(f'{RESULT_CROP_MERGED_DIR}/merged_output.csv', index=False)
    print("✓")
    
    # ========== Step 4b: Merge livestock data ==========
    print("4b) Merging livestock data...", end=" ")
    
    volume_psp_pcount_df = pd.read_csv('data/FAOSTAT/Volumeprodpersp.csv')
    country_code_conv_df = pd.read_csv('data/FAOSTAT/FAOSTAT_countrygroup_filtered.csv')
    
    merged_df_ls = pd.DataFrame(columns=['Country Code (ISO3)', 'Country Code (M49)', 'Animal', 
                                         'heads_on_PA', 'total_heads', 'total volume'])
    
    for country_code in country_list:
        file_path = f"{RESULT_LS_DIR}/output_{country_code}.csv"
        if not os.path.exists(file_path):
            continue
        
        country_data = pd.read_csv(file_path)
        country_data.rename(columns={"clipped_PA_surf": "heads_on_PA", "total_surf": "total_heads"}, inplace=True)
        country_data['Country Code (ISO3)'] = country_code
        
        temp = country_code_conv_df.loc[country_code_conv_df["ISO3 Code"] == country_code, "M49 Code"]
        m49_code = temp.values[0] if not temp.empty else None
        country_data['Country Code (M49)'] = m49_code
        
        country_data['total volume'] = None
        
        for index, row in country_data.iterrows():
            item_code = row['Animal']
            vol_value = volume_psp_pcount_df[(volume_psp_pcount_df['Area Code (M49)'] == m49_code) & 
                                             (volume_psp_pcount_df['Species'] == item_code)]['Volume']
            if not vol_value.empty:
                country_data.at[index, 'total volume'] = vol_value.iloc[0]
        
        merged_df_ls = pd.concat([merged_df_ls, country_data[['Country Code (ISO3)', 'Country Code (M49)', 'Animal', 
                                                              'heads_on_PA', 'total_heads', 'total volume']]])
    
    merged_df_ls.reset_index(drop=True, inplace=True)
    merged_df_ls.to_csv(f'{RESULT_LS_MERGED_DIR}/merged_output_ls.csv', index=False)
    print("✓")
    
    # ========== Step 4c: Total aggregation ==========
    print("4c) Total aggregation...", end=" ")
    
    gdp_df = pd.read_csv('data/FAOSTAT/API_NY.GDP.MKTP.CD_DS2_en_csv_v2_1483921.csv', delimiter=";", dtype={"gdp2022": float})
    
    # Crop aggregation
    vegetal_impact = merged_df_crops.groupby('Country Code (ISO3)').apply(
        lambda x: (x['clipped_PA_surf'] * x['yield'] * x['price'] / 1e3).sum()).reset_index()
    vegetal_impact.columns = ['Country Code (ISO3)', 'macro_impact']
    
    vegetal_physical = merged_df_crops.groupby('Country Code (ISO3)').apply(
        lambda x: (x['clipped_PA_surf'] * x['yield'] / 1e3).sum()).reset_index()
    vegetal_physical.columns = ['Country Code (ISO3)', 'physical_volume']
    
    vegetal_df = pd.merge(vegetal_impact, vegetal_physical, on='Country Code (ISO3)')
    vegetal_df['type'] = 'vegetal'
    vegetal_df['physical_unit'] = 'tonnes'
    
    # Livestock aggregation
    animal_impact = merged_df_ls.groupby('Country Code (ISO3)').apply(
        lambda x: (x['heads_on_PA'] / x['total_heads'] * x['total volume']).sum()).reset_index()
    animal_impact.columns = ['Country Code (ISO3)', 'macro_impact']
    
    animal_physical = merged_df_ls.groupby('Country Code (ISO3)').apply(
        lambda x: (x['heads_on_PA'] / 1e3).sum()).reset_index()
    animal_physical.columns = ['Country Code (ISO3)', 'physical_volume']
    
    animal_df = pd.merge(animal_impact, animal_physical, on='Country Code (ISO3)')
    animal_df['type'] = 'animal'
    animal_df['physical_unit'] = '1000 heads'
    
    combined_impact = pd.concat([vegetal_df, animal_df], ignore_index=True)
    final_df = pd.merge(combined_impact, gdp_df, on='Country Code (ISO3)', how='left')
    final_df['relative impact'] = final_df['macro_impact'] / final_df['gdp2022']
    
    final_df.to_csv(f'{SCENARIO_DIR}/final_result_Shen.csv', index=False)
    print("✓")
    
    # ========== Step 4d: Add agricultural output ==========
    print("4d) Adding agricultural output...", end=" ")
    
    agrioutput_df = pd.read_csv('data/FAOSTAT/2022agrioutput.csv', delimiter=";")
    final_df = pd.merge(final_df, agrioutput_df, on='Country Code (ISO3)', how='left')
    final_df['relative agri impact_PPP'] = final_df['macro_impact'] / final_df['recomputed_sum']
    
    final_df.to_csv(f'{SCENARIO_DIR}/final_result_Shen_agrioutput.csv', index=False)
    print("✓")
    
    # ========== Step 4e: Disaggregated results ==========
    print("4e) Creating disaggregated results...", end=" ")
    
    merged_df_crops['macro_impact'] = merged_df_crops['clipped_PA_surf'] * merged_df_crops['yield'] * merged_df_crops['price'] / 1e3
    merged_df_crops['bu_agri_output'] = merged_df_crops['total_surf'] * merged_df_crops['yield'] * merged_df_crops['price'] / 1e3
    merged_df_crops['physical_volume'] = merged_df_crops['clipped_PA_surf'] * merged_df_crops['yield'] / 1e3
    merged_df_crops['type'] = 'vegetal'
    merged_df_crops['physical_unit'] = 'tonnes'
    vegetal_impact_dis = merged_df_crops[['Country Code (ISO3)', 'Crop', 'macro_impact', 'type', 'physical_volume', 'physical_unit', 'bu_agri_output']]
    vegetal_impact_dis.columns = ['Country Code (ISO3)', 'item', 'macro_impact', 'type', 'physical_volume', 'physical_unit', 'bu_agri_output']
    
    merged_df_ls['macro_impact'] = merged_df_ls['heads_on_PA'] / merged_df_ls['total_heads'] * merged_df_ls['total volume']
    merged_df_ls['bu_agri_output'] = merged_df_ls['total volume']
    merged_df_ls['physical_volume'] = merged_df_ls['heads_on_PA'] / 1e3
    merged_df_ls['type'] = 'animal'
    merged_df_ls['physical_unit'] = '1000 heads'
    animal_impact_dis = merged_df_ls[['Country Code (ISO3)', 'Animal', 'macro_impact', 'type', 'bu_agri_output']]
    animal_impact_dis.columns = ['Country Code (ISO3)', 'item', 'macro_impact', 'type', 'bu_agri_output']
    
    combined_impact_dis = pd.concat([vegetal_impact_dis, animal_impact_dis], ignore_index=True)
    final_df_dis = pd.merge(combined_impact_dis, gdp_df, on='Country Code (ISO3)', how='left')
    final_df_dis['relative impact'] = final_df_dis['macro_impact'] / final_df_dis['bu_agri_output']
    
    final_df_dis.to_csv(f'{SCENARIO_DIR}/final_result_disaggregated_{SCENARIO}.csv', index=False)
    print("✓")
    
    # ========== Step 4f: Disaggregated with agricultural output ==========
    print("4f) Creating disaggregated results with agri output...", end=" ")
    
    final_df_dis = pd.merge(final_df_dis, agrioutput_df, on='Country Code (ISO3)', how='left')
    final_df_dis.to_csv(f'{SCENARIO_DIR}/final_result_disaggregated_{SCENARIO}_buagrioutput.csv', index=False)
    print("✓")
    
    # Print summary statistics
    print("\n4g) Summary statistics:")
    total_exposure_df = pd.read_csv(f'{SCENARIO_DIR}/final_result_{SCENARIO}.csv')
    df_vegetal = total_exposure_df[total_exposure_df['type'] == 'vegetal']
    df_animal = total_exposure_df[total_exposure_df['type'] == 'animal']
    
    vegetal_sum = df_vegetal['macro_impact'].sum()
    animal_sum = df_animal['macro_impact'].sum()
    total_sum = animal_sum + vegetal_sum
    
    print(f"  Animal:  ${animal_sum/1e9:.2f}B")
    print(f"  Vegetal: ${vegetal_sum/1e9:.2f}B")
    print(f"  Total:   ${total_sum/1e9:.2f}B")

# ============================================================================
# STEP 5: CLEANUP INTERMEDIATE FILES
# ============================================================================

def cleanup_intermediate_files():
    """
    Step 5: Remove temporary/intermediate files to save space
    """
    print("\n" + "="*70)
    print("STEP 5: CLEANING UP INTERMEDIATE FILES")
    print("="*70)
    
    files_to_remove = [
        REPROJECTED_CROP_RASTER,
        REPROJECTED_LS_RASTER,
    ]
    
    # Add all country-level TIFs
    if os.path.exists(MASKS_CROP_COUNTRIES_DIR):
        for file_name in os.listdir(MASKS_CROP_COUNTRIES_DIR):
            if file_name.endswith('.tif'):
                files_to_remove.append(os.path.join(MASKS_CROP_COUNTRIES_DIR, file_name))
    
    if os.path.exists(MASKS_LIVESTOCK_COUNTRIES_DIR):
        for file_name in os.listdir(MASKS_LIVESTOCK_COUNTRIES_DIR):
            if file_name.endswith('.tif'):
                files_to_remove.append(os.path.join(MASKS_LIVESTOCK_COUNTRIES_DIR, file_name))
    
    print(f"Removing {len(files_to_remove)} intermediate files...")
    deleted_count = 0
    
    for file_path in files_to_remove:
        try:
            if os.path.exists(file_path):
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                os.remove(file_path)
                print(f"  ✓ Deleted: {file_path} ({file_size_mb:.1f} MB)")
                deleted_count += 1
        except Exception as e:
            print(f"  ✗ Failed to delete {file_path}: {e}")
    
    print(f"✓ Cleaned {deleted_count} files")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Execute all processing steps"""
    print("\n" + "="*70)
    print(f"SCENARIO: {SCENARIO}")
    print("="*70)
    
    try:
        step1_reproject_rasters()
        step2_create_country_masks()
        step3_process_intersections()
        step4_manipulate_data()
        cleanup_intermediate_files()
        
        print("\n" + "="*70)
        print("✓ ALL PROCESSING COMPLETED SUCCESSFULLY")
        print("="*70)
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
