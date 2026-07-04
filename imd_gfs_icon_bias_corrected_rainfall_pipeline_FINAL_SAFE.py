"""
=====================================================================
IMD–GFS–ICON District-wise Rainfall Forecast & Alert Pipeline (FINAL)
SAFE ICON VERSION (with fallback & no-crash logic)
=====================================================================
Author  : Ajay Ahirwar (NIH)
Purpose : Flood Early Warning / DSS / Dam Safety
Models  : GFS (IMD bias-corrected) + ICON (raw, optional)
=====================================================================
"""

# ------------------------------------------------------------------
# IMPORTS
# ------------------------------------------------------------------

import os
import glob
import requests
import imdlib as imd
import xarray as xr
import pandas as pd
import geopandas as gpd
import rioxarray
import numpy as np
from datetime import datetime, timedelta

# ------------------------------------------------------------------
# USER SETTINGS
# ------------------------------------------------------------------

DISTRICT_SHP = r"data/India_Districts.shp"
DIST_COL = "NAME_2"
STATE_COL = "NAME_1"

HYDRO_DATA_DIR = os.path.abspath(os.path.join("..", "data"))
SUBBASIN_SHP = os.path.join(HYDRO_DATA_DIR, "Subbasin.shp")
WATERSHED_SHP = os.path.join(HYDRO_DATA_DIR, "Watershed.shp")

IMD_DIR = "IMD_Rainfall"
GFS_DIR = "GFS_TMP"
ICON_DIR = "ICON_TMP"
# OUTPUT_DIR = "FINAL_OUTPUT"
OUTPUT_DIR = "."
DAILY_VERIFICATION_CSV = "India_Daily_GFS_IMD_Rainfall_Verification.csv"

os.makedirs(IMD_DIR, exist_ok=True)
os.makedirs(GFS_DIR, exist_ok=True)
os.makedirs(ICON_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

FORECAST_HOURS = list(range(3, 49, 3))
RESOLUTION = "0p25"

GFS_BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"
ICON_BASE_URL = "https://opendata.dwd.de/weather/nwp/icon/grib"

# ------------------------------------------------------------------
# UTILITY FUNCTIONS
# ------------------------------------------------------------------

def remove_cfgrib_indexes(grib):
    for idx in glob.glob(f"{grib}*.idx"):
        try:
            os.remove(idx)
        except OSError:
            pass


def gfs_file_url(date, cycle, fh):
    fname = f"gfs.t{cycle}z.pgrb2.{RESOLUTION}.f{fh:03d}"
    return f"{GFS_BASE_URL}/gfs.{date}/{cycle}/atmos/{fname}"


def gfs_file_available(date, cycle, fh):
    url = gfs_file_url(date, cycle, fh)
    try:
        r = requests.head(url, timeout=10, allow_redirects=True)
        if r.status_code == 200:
            return True
        if r.status_code in (403, 405):
            r = requests.get(url, stream=True, timeout=10)
            r.close()
            return r.status_code == 200
    except requests.RequestException:
        return False
    return False


def get_latest_gfs_datetime():
    today = datetime.utcnow()
    required_hours = [FORECAST_HOURS[0], FORECAST_HOURS[-1]]

    for day_offset in range(3):
        date_str = (today - timedelta(days=day_offset)).strftime("%Y%m%d")
        for cycle in ["18", "12", "06", "00"]:
            if all(
                gfs_file_available(date_str, cycle, fh)
                for fh in required_hours
            ):
                return date_str, cycle

    raise RuntimeError("No complete GFS cycle available")

def download_gfs(date, cycle, fh):
    fname = f"gfs.t{cycle}z.pgrb2.{RESOLUTION}.f{fh:03d}"
    url = gfs_file_url(date, cycle, fh)
    path = os.path.join(GFS_DIR, fname)

    r = requests.get(url, stream=True, timeout=60)
    r.raise_for_status()

    with open(path, "wb") as f:
        for c in r.iter_content(8192):
            f.write(c)

    return path


def read_gfs_tp(grib):
    remove_cfgrib_indexes(grib)

    ds = xr.open_dataset(
        grib,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {
                "typeOfLevel": "surface",
                "shortName": "tp",
                "stepType": "accum"
            }
        }
    )
    da = ds["tp"].rio.write_crs("EPSG:4326")

    if da.longitude.max() > 180:
        da = da.assign_coords(
            longitude=((da.longitude + 180) % 360) - 180
        ).sortby("longitude")

    return da


def download_icon_safe(base_date, fh, out_dir):
    """
    Safe ICON downloader with fallback:
    - tries 12 UTC then 00 UTC
    - tries today then yesterday
    - never crashes pipeline
    """
    ICON_RUN_HOURS = ["12", "00"]
    DATE_CANDIDATES = [
        base_date,
        (datetime.strptime(base_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
    ]

    for date in DATE_CANDIDATES:
        for run_hour in ICON_RUN_HOURS:
            fname = (
                f"icon_global_icosahedral_single-level_"
                f"{date}{run_hour}_{fh:03d}_APCP.grib2"
            )
            url = f"{ICON_BASE_URL}/{run_hour}/tot_prec/{fname}"
            path = os.path.join(out_dir, fname)

            try:
                r = requests.get(url, stream=True, timeout=30)
            except requests.RequestException as exc:
                print(f"ICON request failed -> fh{fh:03d}: {exc}")
                continue

            if r.status_code == 200:
                with open(path, "wb") as f:
                    for c in r.iter_content(8192):
                        f.write(c)
                print(f"ICON OK -> {date} {run_hour} fh{fh:03d}")
                return path

    print(f"ICON missing -> fh{fh:03d} (skipped)")
    return None


def read_icon_tp(grib):
    remove_cfgrib_indexes(grib)

    ds = xr.open_dataset(
        grib,
        engine="cfgrib",
        backend_kwargs={"filter_by_keys": {"shortName": "tp"}}
    )
    da = ds["tp"].rio.write_crs("EPSG:4326")

    if da.longitude.max() > 180:
        da = da.assign_coords(
            longitude=((da.longitude + 180) % 360) - 180
        ).sortby("longitude")

    return da


def imd_alert(r):
    if r >= 204.5:
        return "EXTREMELY HEAVY"
    elif r >= 115.6:
        return "VERY HEAVY"
    elif r >= 64.5:
        return "HEAVY"
    elif r >= 10.0:
        return "MODERATE"
    else:
        return "NO ALERT"


def row_key_from_values(state, district):
    return f"{state}||{district}"


def iso_utc(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def safe_numeric(series):
    return pd.to_numeric(series, errors="coerce")


UNIT_COLUMNS = [
    "unit_type",
    "unit_id",
    "unit_name",
    "basin",
    "sub_basin",
    "area_sqkm"
]


def clean_text(series, fallback=""):
    return series.fillna(fallback).astype(str).replace("nan", fallback)


def normalize_area(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def load_hydrologic_units():
    units = {}

    if os.path.exists(SUBBASIN_SHP):
        subbasins = gpd.read_file(SUBBASIN_SHP)
        subbasins = subbasins[
            subbasins.geometry.notna() & ~subbasins.geometry.is_empty
        ].copy()
        subbasins["unit_type"] = "subbasin"
        subbasins["unit_id"] = clean_text(subbasins["sbcode"])
        subbasins["unit_name"] = clean_text(subbasins["sub_basin"])
        subbasins["basin"] = clean_text(subbasins["ba_name"])
        subbasins["sub_basin"] = clean_text(subbasins["sub_basin"])
        subbasins["area_sqkm"] = (
            pd.to_numeric(subbasins["shape_Area"], errors="coerce") /
            1_000_000.0
        )
        subbasins = subbasins.to_crs("EPSG:4326")
        subbasins["_unit_index"] = np.arange(len(subbasins))
        units["subbasin"] = subbasins
        write_hydrologic_geojson(
            subbasins,
            "subbasins.geojson",
            simplify_tolerance=0.01
        )
    else:
        print(f"Subbasin shapefile not found -> {SUBBASIN_SHP}")

    if os.path.exists(WATERSHED_SHP):
        watersheds = gpd.read_file(WATERSHED_SHP)
        watersheds = watersheds[
            watersheds.geometry.notna() & ~watersheds.geometry.is_empty
        ].copy()

        if "subbasin" in units:
            sub_lookup = units["subbasin"][
                ["unit_id", "basin", "sub_basin"]
            ].rename(columns={"unit_id": "sbcode"})
            sub_lookup = sub_lookup.drop_duplicates("sbcode")
            watersheds = watersheds.merge(sub_lookup, on="sbcode", how="left")
        else:
            watersheds["basin"] = ""
            watersheds["sub_basin"] = clean_text(watersheds["sbcode"])

        watershed_code = clean_text(watersheds["wsconc"])
        object_id = clean_text(watersheds["objectid"])
        missing_code = watershed_code.str.lower().isin(["", "none", "nan"])

        watersheds["unit_type"] = "watershed"
        watersheds["unit_id"] = watershed_code.where(
            ~missing_code,
            "WS_" + object_id
        )
        watersheds["unit_name"] = "Watershed " + watershed_code.where(
            ~missing_code,
            object_id
        )
        watersheds["basin"] = clean_text(watersheds["basin"])
        watersheds["sub_basin"] = clean_text(watersheds["sub_basin"])
        watersheds["area_sqkm"] = pd.to_numeric(
            watersheds["area_sqkm"],
            errors="coerce"
        )
        watersheds = watersheds.to_crs("EPSG:4326")
        watersheds = watersheds.dissolve(
            by="unit_id",
            as_index=False,
            aggfunc={
                "unit_type": "first",
                "unit_name": "first",
                "basin": "first",
                "sub_basin": "first",
                "area_sqkm": "sum"
            }
        )
        watersheds["_unit_index"] = np.arange(len(watersheds))
        units["watershed"] = watersheds
        write_hydrologic_geojson(
            watersheds,
            "watersheds.geojson",
            simplify_tolerance=0.03
        )
    else:
        print(f"Watershed shapefile not found -> {WATERSHED_SHP}")

    return units


def write_hydrologic_geojson(gdf, filename, simplify_tolerance):
    out_path = os.path.join(OUTPUT_DIR, filename)
    web_gdf = gdf[UNIT_COLUMNS + ["geometry"]].copy()
    web_gdf["area_sqkm"] = pd.to_numeric(
        web_gdf["area_sqkm"],
        errors="coerce"
    ).fillna(0.0)
    web_gdf["geometry"] = web_gdf.geometry.simplify(
        simplify_tolerance,
        preserve_topology=True
    )
    web_gdf.to_file(out_path, driver="GeoJSON")


def lat_lon_names(da):
    lon_name = next(
        name for name in ["longitude", "lon", "x"]
        if name in da.coords
    )
    lat_name = next(
        name for name in ["latitude", "lat", "y"]
        if name in da.coords
    )
    return lat_name, lon_name


def zonal_mean_points(da, units_gdf, value_col):
    lat_name, lon_name = lat_lon_names(da)
    minx, miny, maxx, maxy = units_gdf.total_bounds
    pad = 0.5

    lons = np.asarray(da[lon_name].values)
    lats = np.asarray(da[lat_name].values)
    lon_idx = np.where((lons >= minx - pad) & (lons <= maxx + pad))[0]
    lat_idx = np.where((lats >= miny - pad) & (lats <= maxy + pad))[0]

    if len(lon_idx) == 0 or len(lat_idx) == 0:
        result = units_gdf[UNIT_COLUMNS + ["_unit_index"]].copy()
        result[value_col] = 0.0
        return result

    da_sub = da.isel({lon_name: lon_idx, lat_name: lat_idx})
    da_sub = da_sub.transpose(lat_name, lon_name)
    vals = np.asarray(da_sub.values, dtype=float)
    lon_grid, lat_grid = np.meshgrid(
        np.asarray(da_sub[lon_name].values),
        np.asarray(da_sub[lat_name].values)
    )

    points = gpd.GeoDataFrame(
        {"value": vals.ravel()},
        geometry=gpd.points_from_xy(lon_grid.ravel(), lat_grid.ravel()),
        crs="EPSG:4326"
    )
    points = points[np.isfinite(points["value"])].copy()

    joined = gpd.sjoin(
        points,
        units_gdf[["_unit_index", "geometry"]],
        how="inner",
        predicate="intersects"
    )
    grouped = joined.groupby("_unit_index")["value"].mean()

    result = units_gdf[UNIT_COLUMNS + ["_unit_index"]].copy()
    result[value_col] = result["_unit_index"].map(grouped).fillna(0.0)
    return result


def append_unit_records(records, means_df, from_hour, to_hour, value_col):
    for row in means_df.to_dict("records"):
        records.append({
            **{col: row.get(col, "") for col in UNIT_COLUMNS},
            "from_hour": from_hour,
            "to_hour": to_hour,
            value_col: row.get(value_col, 0.0)
        })


def hydro_bias_factor(row):
    if row["rain_gfs_mm"] < 0.1:
        return 1.0

    bf = row["imd_mean_mm"] / row["rain_gfs_mm"]
    return min(max(bf, 0.3), 3.0)


def build_hydrologic_outputs(gfs_records, icon_records, imd_means):
    empty_icon_cols = UNIT_COLUMNS + [
        "from_hour",
        "to_hour",
        "rain_icon_mm"
    ]
    gfs_df_h = pd.DataFrame(gfs_records)
    icon_df_h = pd.DataFrame(icon_records)

    if icon_df_h.empty:
        icon_df_h = pd.DataFrame(columns=empty_icon_cols)

    gfs_24h_h = (
        gfs_df_h[gfs_df_h["to_hour"] <= 24]
        .groupby(UNIT_COLUMNS)["rain_gfs_mm"]
        .sum()
        .reset_index()
    )
    bc_h = gfs_24h_h.merge(
        imd_means[["unit_type", "unit_id", "imd_mean_mm"]],
        on=["unit_type", "unit_id"],
        how="left"
    )
    bc_h["imd_mean_mm"] = pd.to_numeric(
        bc_h["imd_mean_mm"],
        errors="coerce"
    ).fillna(0.0)
    bc_h["bias_factor"] = bc_h.apply(hydro_bias_factor, axis=1)
    bc_h["rain_gfs_bc_mm"] = bc_h["rain_gfs_mm"] * bc_h["bias_factor"]

    gfs_distribution_h = gfs_df_h.merge(
        bc_h[["unit_type", "unit_id", "bias_factor"]],
        on=["unit_type", "unit_id"],
        how="left"
    )
    gfs_distribution_h["bias_factor"] = (
        gfs_distribution_h["bias_factor"]
        .fillna(1.0)
        .astype(float)
    )
    gfs_distribution_h["rain_gfs_bc_mm"] = (
        gfs_distribution_h["rain_gfs_mm"] *
        gfs_distribution_h["bias_factor"]
    )

    if not icon_df_h.empty:
        icon_24h_h = (
            icon_df_h[icon_df_h["to_hour"] <= 24]
            .groupby(["unit_type", "unit_id"])["rain_icon_mm"]
            .sum()
            .reset_index()
        )
    else:
        icon_24h_h = pd.DataFrame(
            columns=["unit_type", "unit_id", "rain_icon_mm"]
        )

    final_h = bc_h.merge(
        icon_24h_h,
        on=["unit_type", "unit_id"],
        how="left"
    )
    final_h["rain_icon_mm"] = pd.to_numeric(
        final_h["rain_icon_mm"],
        errors="coerce"
    ).fillna(0.0)
    final_h["alert_gfs_bc"] = final_h["rain_gfs_bc_mm"].apply(imd_alert)
    final_h["alert_icon"] = final_h["rain_icon_mm"].apply(imd_alert)
    final_h["date"] = DATE
    final_h["cycle_utc"] = CYCLE

    distribution_h = gfs_distribution_h.merge(
        icon_df_h,
        on=UNIT_COLUMNS + ["from_hour", "to_hour"],
        how="left"
    )
    distribution_h["rain_icon_mm"] = pd.to_numeric(
        distribution_h["rain_icon_mm"],
        errors="coerce"
    ).fillna(0.0)
    distribution_h["alert_gfs_bc"] = (
        distribution_h["rain_gfs_bc_mm"].apply(imd_alert)
    )
    distribution_h["alert_icon"] = (
        distribution_h["rain_icon_mm"].apply(imd_alert)
    )
    distribution_h["interval_label"] = (
        distribution_h["from_hour"].astype(int).astype(str) +
        "-" +
        distribution_h["to_hour"].astype(int).astype(str) +
        " h"
    )
    distribution_h["date"] = DATE
    distribution_h["cycle_utc"] = CYCLE

    final_cols = UNIT_COLUMNS + [
        "rain_gfs_mm",
        "imd_mean_mm",
        "bias_factor",
        "rain_gfs_bc_mm",
        "rain_icon_mm",
        "alert_gfs_bc",
        "alert_icon",
        "date",
        "cycle_utc"
    ]
    distribution_cols = UNIT_COLUMNS + [
        "from_hour",
        "to_hour",
        "interval_label",
        "rain_gfs_mm",
        "bias_factor",
        "rain_gfs_bc_mm",
        "rain_icon_mm",
        "alert_gfs_bc",
        "alert_icon",
        "date",
        "cycle_utc"
    ]

    return final_h[final_cols], distribution_h[distribution_cols]

# ------------------------------------------------------------------
# LOAD DISTRICTS
# ------------------------------------------------------------------

districts = gpd.read_file(DISTRICT_SHP).to_crs("EPSG:4326")
hydrologic_units = load_hydrologic_units()

# ------------------------------------------------------------------
# STEP 1: IMD OBSERVED RAINFALL (LAST COMPLETE YEAR)
# ------------------------------------------------------------------

last_complete_year = datetime.utcnow().year - 1
print(f"Using IMD rainfall data for year: {last_complete_year}")

imd_grd_path = os.path.join(IMD_DIR, "rain", f"{last_complete_year}.grd")

if os.path.exists(imd_grd_path) and os.path.getsize(imd_grd_path) > 0:
    print(f"IMD rainfall file found -> {imd_grd_path}")
    print("Skipping IMD download.")
else:
    print(f"IMD rainfall file missing -> {imd_grd_path}")
    print("Downloading IMD rainfall data...")
    imd.get_data(
        "rain",
        last_complete_year,
        last_complete_year,
        fn_format="yearwise",
        file_dir=IMD_DIR
    )

imd_data = imd.open_data(
    "rain",
    last_complete_year,
    last_complete_year,
    fn_format="yearwise",
    file_dir=IMD_DIR
)

ds_imd = imd_data.get_xarray()
ds_imd["rain"] = ds_imd["rain"].where(ds_imd["rain"] != -999.0)
ds_imd["rain"] = ds_imd["rain"].rio.write_crs("EPSG:4326")

imd_means = []

for _, row in districts.iterrows():
    try:
        clip = ds_imd["rain"].rio.clip(
            [row.geometry], districts.crs, drop=True
        )
        mean_val = float(clip.mean(dim=["time", "lat", "lon"]).values)
    except Exception:
        mean_val = 0.0

    imd_means.append({
        "state": row[STATE_COL],
        "district": row[DIST_COL],
        "imd_mean_mm": mean_val
    })

imd_df = pd.DataFrame(imd_means)

hydrologic_imd_means = {}

if hydrologic_units:
    imd_mean_grid = ds_imd["rain"].mean(dim="time", skipna=True)

    for unit_key, unit_gdf in hydrologic_units.items():
        print(f"Computing IMD mean for {unit_key} units...")
        hydrologic_imd_means[unit_key] = zonal_mean_points(
            imd_mean_grid,
            unit_gdf,
            "imd_mean_mm"
        )

imd_year_cache = {last_complete_year: ds_imd}


def get_imd_year_dataset(year):
    if year in imd_year_cache:
        return imd_year_cache[year]

    imd_grd = os.path.join(IMD_DIR, "rain", f"{year}.grd")

    if not (os.path.exists(imd_grd) and os.path.getsize(imd_grd) > 0):
        try:
            print(f"Downloading IMD observed rainfall for year: {year}")
            imd.get_data(
                "rain",
                year,
                year,
                fn_format="yearwise",
                file_dir=IMD_DIR
            )
        except Exception as exc:
            print(f"IMD observed rainfall unavailable for {year}: {exc}")
            imd_year_cache[year] = None
            return None

    try:
        imd_year_data = imd.open_data(
            "rain",
            year,
            year,
            fn_format="yearwise",
            file_dir=IMD_DIR
        )
        ds_year = imd_year_data.get_xarray()
        ds_year["rain"] = ds_year["rain"].where(ds_year["rain"] != -999.0)
        ds_year["rain"] = ds_year["rain"].rio.write_crs("EPSG:4326")
        imd_year_cache[year] = ds_year
        return ds_year
    except Exception as exc:
        print(f"Could not open IMD observed rainfall for {year}: {exc}")
        imd_year_cache[year] = None
        return None


def observed_rainfall_by_district(date_yyyymmdd):
    try:
        date_obj = datetime.strptime(str(date_yyyymmdd), "%Y%m%d")
    except ValueError:
        return {}, "UNAVAILABLE"

    ds_year = get_imd_year_dataset(date_obj.year)

    if ds_year is None:
        return {}, "PENDING"

    target_time = pd.Timestamp(date_obj.date())
    available_times = pd.to_datetime(ds_year.time.values).normalize()

    if target_time not in set(available_times):
        return {}, "PENDING"

    daily_rain = ds_year["rain"].sel(time=target_time)
    observed = {}

    for _, row in districts.iterrows():
        state = row[STATE_COL]
        district = row[DIST_COL]

        try:
            clip = daily_rain.rio.clip(
                [row.geometry], districts.crs, drop=True
            )
            value = float(clip.mean(dim=["lat", "lon"]).values)
        except Exception:
            value = float("nan")

        observed[row_key_from_values(state, district)] = value

    return observed, "MATCHED"


def add_observed_values(archive_df):
    if archive_df.empty:
        return archive_df

    archive_df["imd_observed_mm"] = safe_numeric(archive_df["imd_observed_mm"])
    archive_df["rain_forecast_daily_mm"] = safe_numeric(
        archive_df["rain_forecast_daily_mm"]
    )

    missing_mask = archive_df["imd_observed_mm"].isna()
    valid_dates = (
        archive_df.loc[missing_mask, "valid_date"]
        .dropna()
        .astype(str)
        .sort_values()
        .unique()
    )

    for valid_date in valid_dates:
        observed, status = observed_rainfall_by_district(valid_date)
        date_mask = archive_df["valid_date"].astype(str) == valid_date
        missing_date_mask = date_mask & archive_df["imd_observed_mm"].isna()

        if status != "MATCHED":
            archive_df.loc[missing_date_mask, "observed_status"] = status
            continue

        for index, row in archive_df.loc[missing_date_mask].iterrows():
            key = row_key_from_values(row["state"], row["district"])
            value = observed.get(key, float("nan"))

            if pd.notna(value):
                archive_df.at[index, "imd_observed_mm"] = value
                archive_df.at[index, "observed_status"] = "MATCHED"
            else:
                archive_df.at[index, "observed_status"] = "UNAVAILABLE"

    observed_numeric = safe_numeric(archive_df["imd_observed_mm"])
    forecast_numeric = safe_numeric(archive_df["rain_forecast_daily_mm"])
    valid_pair = observed_numeric.notna() & forecast_numeric.notna()

    archive_df["error_gfs_bc_mm"] = pd.NA
    archive_df["abs_error_gfs_bc_mm"] = pd.NA
    archive_df["pct_error_gfs_bc"] = pd.NA

    archive_df.loc[valid_pair, "error_gfs_bc_mm"] = (
        forecast_numeric[valid_pair] - observed_numeric[valid_pair]
    )
    archive_df.loc[valid_pair, "abs_error_gfs_bc_mm"] = (
        archive_df.loc[valid_pair, "error_gfs_bc_mm"].abs()
    )

    non_zero_observed = valid_pair & (observed_numeric.abs() >= 0.1)
    archive_df.loc[non_zero_observed, "pct_error_gfs_bc"] = (
        archive_df.loc[non_zero_observed, "error_gfs_bc_mm"] /
        observed_numeric[non_zero_observed] * 100.0
    )

    return archive_df


def update_daily_verification_archive(latest_df):
    archive_path = os.path.join(OUTPUT_DIR, DAILY_VERIFICATION_CSV)
    issue_dt = datetime.strptime(f"{DATE}{CYCLE}", "%Y%m%d%H")
    valid_start = issue_dt
    valid_end = issue_dt + timedelta(hours=24)
    valid_date = valid_end.strftime("%Y%m%d")
    updated_at = iso_utc(datetime.utcnow())

    daily_df = latest_df[
        [
            "state",
            "district",
            "rain_gfs_mm",
            "bias_factor",
            "rain_gfs_bc_mm",
            "rain_icon_mm"
        ]
    ].copy()

    daily_df["rain_gfs_mm"] = safe_numeric(daily_df["rain_gfs_mm"])
    daily_df["rain_gfs_bc_mm"] = safe_numeric(daily_df["rain_gfs_bc_mm"])
    daily_df["rain_icon_mm"] = safe_numeric(daily_df["rain_icon_mm"])
    daily_df["rain_forecast_daily_mm"] = daily_df["rain_gfs_bc_mm"].fillna(
        daily_df["rain_gfs_mm"]
    )

    daily_df = daily_df.rename(columns={
        "rain_gfs_mm": "rain_gfs_24h_mm",
        "rain_gfs_bc_mm": "rain_gfs_bc_24h_mm",
        "rain_icon_mm": "rain_icon_24h_mm"
    })

    daily_df["forecast_issue_date"] = DATE
    daily_df["cycle_utc"] = CYCLE
    daily_df["forecast_issue_time_utc"] = iso_utc(issue_dt)
    daily_df["valid_start_utc"] = iso_utc(valid_start)
    daily_df["valid_end_utc"] = iso_utc(valid_end)
    daily_df["valid_date"] = valid_date
    daily_df["imd_observed_mm"] = pd.NA
    daily_df["observed_status"] = "PENDING"
    daily_df["error_gfs_bc_mm"] = pd.NA
    daily_df["abs_error_gfs_bc_mm"] = pd.NA
    daily_df["pct_error_gfs_bc"] = pd.NA
    daily_df["updated_at_utc"] = updated_at

    columns = [
        "state",
        "district",
        "forecast_issue_date",
        "cycle_utc",
        "forecast_issue_time_utc",
        "valid_start_utc",
        "valid_end_utc",
        "valid_date",
        "rain_forecast_daily_mm",
        "rain_gfs_24h_mm",
        "bias_factor",
        "rain_gfs_bc_24h_mm",
        "rain_icon_24h_mm",
        "imd_observed_mm",
        "observed_status",
        "error_gfs_bc_mm",
        "abs_error_gfs_bc_mm",
        "pct_error_gfs_bc",
        "updated_at_utc"
    ]

    daily_df = daily_df[columns]

    if os.path.exists(archive_path):
        archive_df = pd.read_csv(
            archive_path,
            dtype={
                "forecast_issue_date": str,
                "cycle_utc": str,
                "valid_date": str
            }
        )
    else:
        archive_df = pd.DataFrame(columns=columns)

    archive_df = pd.concat([archive_df, daily_df], ignore_index=True)
    archive_df["cycle_utc"] = archive_df["cycle_utc"].astype(str).str.zfill(2)
    archive_df["forecast_issue_date"] = archive_df[
        "forecast_issue_date"
    ].astype(str)
    archive_df["valid_date"] = archive_df["valid_date"].astype(str)
    archive_df = archive_df.drop_duplicates(
        subset=[
            "state",
            "district",
            "forecast_issue_date",
            "cycle_utc",
            "valid_date"
        ],
        keep="last"
    )

    archive_df = add_observed_values(archive_df)
    archive_df = archive_df[columns].sort_values(
        ["valid_date", "state", "district", "forecast_issue_date", "cycle_utc"]
    )
    archive_df.to_csv(archive_path, index=False)

    return archive_df

# ------------------------------------------------------------------
# STEP 2: GFS FORECAST (NEXT 24 HOURS)
# ------------------------------------------------------------------

DATE, CYCLE = get_latest_gfs_datetime()
print(f"GFS Forecast -> {DATE} | Cycle {CYCLE} UTC")

gfs_records = []
hydrologic_gfs_records = {
    unit_key: []
    for unit_key in hydrologic_units
}

prev_tp = None
prev_file = None
prev_hr = None

for fh in FORECAST_HOURS:
    f = download_gfs(DATE, CYCLE, fh)
    tp = read_gfs_tp(f)

    if prev_tp is None:
        rain_inc = tp.where(tp >= 0, 0)
        from_hr = 0
    else:
        rain_inc = (tp - prev_tp).where((tp - prev_tp) >= 0, 0)
        from_hr = prev_hr

    for _, row in districts.iterrows():
        try:
            clip = rain_inc.rio.clip(
                [row.geometry], districts.crs, drop=True
            )
            val = float(
                clip.mean(dim=["latitude", "longitude"]).values
            )
        except Exception:
            val = 0.0

        gfs_records.append({
            "state": row[STATE_COL],
            "district": row[DIST_COL],
            "from_hour": from_hr,
            "to_hour": fh,
            "rain_gfs_mm": val
        })

    for unit_key, unit_gdf in hydrologic_units.items():
        means_df = zonal_mean_points(rain_inc, unit_gdf, "rain_gfs_mm")
        append_unit_records(
            hydrologic_gfs_records[unit_key],
            means_df,
            from_hr,
            fh,
            "rain_gfs_mm"
        )

    if prev_file and os.path.exists(prev_file):
        os.remove(prev_file)

    prev_tp = tp
    prev_file = f
    prev_hr = fh

if prev_file and os.path.exists(prev_file):
    os.remove(prev_file)

gfs_df = pd.DataFrame(gfs_records)

gfs_24h = (
    gfs_df[gfs_df["to_hour"] <= 24]
    .groupby(["state", "district"])["rain_gfs_mm"]
    .sum()
    .reset_index()
)

# ------------------------------------------------------------------
# STEP 3: BIAS CORRECTION (IMD -> GFS)
# ------------------------------------------------------------------

bc_df = gfs_24h.merge(imd_df, on=["state", "district"], how="left")

def bias_factor(row):
    if row["rain_gfs_mm"] < 0.1:
        return 1.0
    bf = row["imd_mean_mm"] / row["rain_gfs_mm"]
    return min(max(bf, 0.3), 3.0)

bc_df["bias_factor"] = bc_df.apply(bias_factor, axis=1)
bc_df["rain_gfs_bc_mm"] = bc_df["rain_gfs_mm"] * bc_df["bias_factor"]

gfs_distribution = gfs_df.merge(
    bc_df[["state", "district", "bias_factor"]],
    on=["state", "district"],
    how="left"
)
gfs_distribution["bias_factor"] = (
    gfs_distribution["bias_factor"]
    .fillna(1.0)
    .astype(float)
)
gfs_distribution["rain_gfs_bc_mm"] = (
    gfs_distribution["rain_gfs_mm"] *
    gfs_distribution["bias_factor"]
)

# ------------------------------------------------------------------
# STEP 4: ICON FORECAST (SAFE, OPTIONAL)
# ------------------------------------------------------------------

icon_records = []
hydrologic_icon_records = {
    unit_key: []
    for unit_key in hydrologic_units
}

prev_tp = None
prev_file = None
prev_hr = None

for fh in FORECAST_HOURS:
    f = download_icon_safe(DATE, fh, ICON_DIR)

    if f is None:
        continue

    tp = read_icon_tp(f)

    if prev_tp is None:
        rain_inc = tp.where(tp >= 0, 0)
        from_hr = 0
    else:
        rain_inc = (tp - prev_tp).where((tp - prev_tp) >= 0, 0)
        from_hr = prev_hr

    for _, row in districts.iterrows():
        try:
            clip = rain_inc.rio.clip(
                [row.geometry], districts.crs, drop=True
            )
            val = float(
                clip.mean(dim=["latitude", "longitude"]).values
            )
        except Exception:
            val = 0.0

        icon_records.append({
            "state": row[STATE_COL],
            "district": row[DIST_COL],
            "from_hour": from_hr,
            "to_hour": fh,
            "rain_icon_mm": val
        })

    for unit_key, unit_gdf in hydrologic_units.items():
        means_df = zonal_mean_points(rain_inc, unit_gdf, "rain_icon_mm")
        append_unit_records(
            hydrologic_icon_records[unit_key],
            means_df,
            from_hr,
            fh,
            "rain_icon_mm"
        )

    if prev_file and os.path.exists(prev_file):
        os.remove(prev_file)

    prev_tp = tp
    prev_file = f
    prev_hr = fh

if prev_file and os.path.exists(prev_file):
    os.remove(prev_file)

icon_df = pd.DataFrame(icon_records)

if not icon_df.empty:
    icon_24h = (
        icon_df[icon_df["to_hour"] <= 24]
        .groupby(["state", "district"])["rain_icon_mm"]
        .sum()
        .reset_index()
    )
else:
    icon_df = pd.DataFrame(
        columns=["state", "district", "from_hour", "to_hour", "rain_icon_mm"]
    )
    icon_24h = pd.DataFrame(
        columns=["state", "district", "rain_icon_mm"]
    )

# ------------------------------------------------------------------
# STEP 5: FINAL MERGE + ALERTS
# ------------------------------------------------------------------

final_df = bc_df.merge(icon_24h, on=["state", "district"], how="left")
final_df["rain_icon_mm"] = pd.to_numeric(
    final_df["rain_icon_mm"],
    errors="coerce"
).fillna(0.0)
final_df["alert_gfs_bc"] = final_df["rain_gfs_bc_mm"].apply(imd_alert)
final_df["alert_icon"] = final_df["rain_icon_mm"].apply(imd_alert)

final_df["date"] = DATE
final_df["cycle_utc"] = CYCLE

distribution_df = gfs_distribution.merge(
    icon_df,
    on=["state", "district", "from_hour", "to_hour"],
    how="left"
)
distribution_df["rain_icon_mm"] = pd.to_numeric(
    distribution_df["rain_icon_mm"],
    errors="coerce"
).fillna(0.0)
distribution_df["alert_gfs_bc"] = (
    distribution_df["rain_gfs_bc_mm"].apply(imd_alert)
)
distribution_df["alert_icon"] = (
    distribution_df["rain_icon_mm"].apply(imd_alert)
)
distribution_df["interval_label"] = (
    distribution_df["from_hour"].astype(int).astype(str) +
    "-" +
    distribution_df["to_hour"].astype(int).astype(str) +
    " h"
)
distribution_df["date"] = DATE
distribution_df["cycle_utc"] = CYCLE
distribution_df = distribution_df[
    [
        "state",
        "district",
        "from_hour",
        "to_hour",
        "interval_label",
        "rain_gfs_mm",
        "bias_factor",
        "rain_gfs_bc_mm",
        "rain_icon_mm",
        "alert_gfs_bc",
        "alert_icon",
        "date",
        "cycle_utc"
    ]
]

daily_verification_df = update_daily_verification_archive(final_df)
hydrologic_outputs = {}

for unit_key in hydrologic_units:
    if not hydrologic_gfs_records.get(unit_key):
        continue

    final_h, distribution_h = build_hydrologic_outputs(
        hydrologic_gfs_records[unit_key],
        hydrologic_icon_records.get(unit_key, []),
        hydrologic_imd_means[unit_key]
    )
    hydrologic_outputs[unit_key] = {
        "final": final_h,
        "distribution": distribution_h
    }

# ------------------------------------------------------------------
# STEP 6: SAVE OUTPUTS
# ------------------------------------------------------------------

final_df.to_csv(
    f"{OUTPUT_DIR}/India_24h_GFS_IMD_ICON_Rainfall.csv",
    index=False
)

alert_df = final_df[
    (final_df["alert_gfs_bc"] != "NO ALERT") |
    (final_df["alert_icon"] != "NO ALERT")
]

alert_df.to_csv(
    f"{OUTPUT_DIR}/India_24h_GFS_IMD_ICON_ALERTS.csv",
    index=False
)

distribution_df.to_csv(
    f"{OUTPUT_DIR}/India_3h_GFS_IMD_ICON_Rainfall.csv",
    index=False
)

for unit_key, output in hydrologic_outputs.items():
    label = "Subbasin" if unit_key == "subbasin" else "Watershed"
    output["final"].to_csv(
        f"{OUTPUT_DIR}/India_24h_GFS_IMD_ICON_{label}_Rainfall.csv",
        index=False
    )
    output["distribution"].to_csv(
        f"{OUTPUT_DIR}/India_3h_GFS_IMD_ICON_{label}_Rainfall.csv",
        index=False
    )

print("\n==============================================")
print("FINAL SAFE GFS + ICON PIPELINE COMPLETED")
print(f"Total districts : {final_df.shape[0]}")
print(f"Alert districts : {alert_df.shape[0]}")
print(f"3-hour records : {distribution_df.shape[0]}")
print(f"Daily verification records : {daily_verification_df.shape[0]}")
for unit_key, output in hydrologic_outputs.items():
    print(
        f"{unit_key.title()} units : {output['final'].shape[0]} | "
        f"3-hour records : {output['distribution'].shape[0]}"
    )
print("==============================================")
