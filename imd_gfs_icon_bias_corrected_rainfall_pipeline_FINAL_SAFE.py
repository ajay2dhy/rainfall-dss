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
import shutil
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

DISTRICT_SHP = r"data/DISTRICT_BOUNDARY.shp"
DISTRICT_SOURCE_DIST_COL = "District"
DISTRICT_SOURCE_STATE_COL = "STATE"
DIST_COL = "NAME_2"
STATE_COL = "NAME_1"
DISTRICT_GEOJSON = "india_districts.geojson"
DISTRICT_PROCESS_SIMPLIFY_TOLERANCE = 0.01
DISTRICT_GEOJSON_SIMPLIFY_TOLERANCE = 0.005

HYDRO_DATA_DIR = os.path.abspath(os.path.join("..", "data"))
SUBBASIN_SHP = os.path.join(HYDRO_DATA_DIR, "Subbasin.shp")
WATERSHED_SHP = os.path.join(HYDRO_DATA_DIR, "Watershed.shp")

IMD_DIR = "IMD_Rainfall"
IMD_REALTIME_DIR = os.path.join(IMD_DIR, "realtime_rain")
GFS_DIR = "GFS_TMP"
ICON_DIR = "ICON_TMP"
# OUTPUT_DIR = "FINAL_OUTPUT"
OUTPUT_DIR = "."
DAILY_VERIFICATION_CSV = "India_Daily_GFS_IMD_Rainfall_Verification.csv"
DISTRICT_DAILY_VERIFICATION_ALIAS_CSV = (
    "India_Daily_GFS_IMD_District_Verification.csv"
)
SUBBASIN_DAILY_VERIFICATION_CSV = (
    "India_Daily_GFS_IMD_Subbasin_Verification.csv"
)
WATERSHED_DAILY_VERIFICATION_CSV = (
    "India_Daily_GFS_IMD_Watershed_Verification.csv"
)
DISTRICT_3H_ARCHIVE_CSV = "India_3h_GFS_District_Archive.csv"
SUBBASIN_3H_ARCHIVE_CSV = "India_3h_GFS_Subbasin_Archive.csv"
WATERSHED_3H_ARCHIVE_CSV = "India_3h_GFS_Watershed_Archive.csv"
SUBBASIN_DISTRICT_CROSSWALK_CSV = "India_Subbasin_District_Crosswalk.csv"
SUBBASIN_WATERSHED_CROSSWALK_CSV = "India_Subbasin_Watershed_Crosswalk.csv"
DISTRICT_WATERSHED_CROSSWALK_CSV = "India_District_Watershed_Crosswalk.csv"
CROSSWALK_AREA_CRS = "EPSG:6933"
CROSSWALK_MIN_AREA_SQKM = 0.01
DAILY_ARCHIVE_MAX_VALID_DATES_BY_UNIT = {
    "district": 365,
    "subbasin": 365,
    "watershed": 18
}
CSV_FLOAT_FORMAT = "%.4f"
THREE_H_ARCHIVE_MAX_CYCLES = 4
CALIBRATION_THRESHOLD_MM = 12.0
CALIBRATION_FACTOR_MIN = 0.25
CALIBRATION_FACTOR_MAX = 4.0

os.makedirs(IMD_DIR, exist_ok=True)
os.makedirs(IMD_REALTIME_DIR, exist_ok=True)
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


def imd_rain_path(year):
    return os.path.join(IMD_DIR, "rain", f"{year}.grd")


def prepare_imd_download(year, refresh=False):
    path = imd_rain_path(year)
    backup_path = f"{path}.codex-backup"
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if os.path.exists(path):
        size = os.path.getsize(path)

        if size <= 0:
            print(f"IMD rainfall file is empty -> {path}")
            try:
                os.remove(path)
                print("Deleted empty IMD rainfall file; retrying download.")
            except OSError as exc:
                print(f"Could not delete empty IMD rainfall file: {exc}")
                return path, backup_path, False
        elif refresh:
            print(f"Refreshing IMD rainfall data for year: {year}")
            try:
                if os.path.exists(backup_path):
                    os.remove(backup_path)
                shutil.copy2(path, backup_path)
                os.remove(path)
            except OSError as exc:
                print(f"Could not prepare IMD refresh; using existing file. {exc}")
                return path, backup_path, False
        else:
            print(f"IMD rainfall file found -> {path}")
            print("Skipping IMD download.")
            return path, backup_path, False
    else:
        print(f"IMD rainfall file missing -> {path}")

    return path, backup_path, True


def restore_imd_backup(path, backup_path):
    if not os.path.exists(backup_path):
        return False

    try:
        if os.path.exists(path):
            os.remove(path)
        shutil.move(backup_path, path)
        print(f"Restored previous IMD rainfall file -> {path}")
        return True
    except OSError as exc:
        print(f"Could not restore previous IMD rainfall file: {exc}")
        return False


def ensure_imd_rain_file(year, refresh=False):
    path, backup_path, should_download = prepare_imd_download(year, refresh)

    if not should_download:
        return path if os.path.exists(path) and os.path.getsize(path) > 0 else None

    print(f"Downloading IMD rainfall data for year: {year}...")

    try:
        imd.get_data(
            "rain",
            year,
            year,
            fn_format="yearwise",
            file_dir=IMD_DIR
        )
    except Exception as exc:
        print(f"IMD rainfall download failed for {year}: {exc}")
        if restore_imd_backup(path, backup_path):
            return path
        return None

    if os.path.exists(path) and os.path.getsize(path) > 0:
        print(f"IMD rainfall file ready -> {path}")
        try:
            if os.path.exists(backup_path):
                os.remove(backup_path)
        except OSError:
            pass
        return path

    print(f"IMD rainfall download did not produce usable file for {year}.")

    if restore_imd_backup(path, backup_path):
        return path

    return None


def open_imd_year_dataset(year, refresh=False):
    if ensure_imd_rain_file(year, refresh=refresh) is None:
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
        return ds_year
    except Exception as exc:
        print(f"Could not open IMD observed rainfall for {year}: {exc}")
        return None


def limit_imd_dataset_through(ds_year, cutoff_date):
    times = pd.to_datetime(ds_year.time.values).normalize()
    keep_index = np.where(times <= pd.Timestamp(cutoff_date))[0]

    if len(keep_index) == 0:
        return None, None

    latest_available = times[keep_index].max().date()
    return ds_year.isel(time=keep_index), latest_available


def imd_realtime_rain_path(date_obj):
    return os.path.join(
        IMD_REALTIME_DIR,
        f"rain_ind0.25_{date_obj:%y_%m_%d}.grd"
    )


def prepare_realtime_rain_file(date_obj, refresh=False):
    path = imd_realtime_rain_path(date_obj)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if os.path.exists(path):
        size = os.path.getsize(path)

        if size <= 0:
            print(f"IMD real-time rainfall file is empty -> {path}")
            try:
                os.remove(path)
                print("Deleted empty real-time IMD file; retrying download.")
            except OSError as exc:
                print(f"Could not delete empty real-time IMD file: {exc}")
                return False
        elif refresh:
            try:
                os.remove(path)
            except OSError as exc:
                print(f"Could not refresh real-time IMD file: {exc}")
                return False
        else:
            return True

    return None


def sanitize_realtime_dataset(ds_day):
    ds_day["rain"] = ds_day["rain"].where(ds_day["rain"] != -999.0)
    ds_day["rain"] = ds_day["rain"].rio.write_crs("EPSG:4326")
    return ds_day


def open_imd_realtime_day(date_obj, refresh=False):
    date_str = date_obj.strftime("%Y-%m-%d")
    existing_state = prepare_realtime_rain_file(date_obj, refresh=refresh)

    if existing_state is True:
        try:
            data = imd.open_real_data(
                "rain",
                date_str,
                date_str,
                file_dir=IMD_REALTIME_DIR
            )
            return sanitize_realtime_dataset(data.get_xarray())
        except Exception as exc:
            print(f"Could not open cached IMD real-time rainfall for {date_str}: {exc}")
            return open_imd_realtime_day(date_obj, refresh=True)

    if existing_state is False:
        return None

    print(f"Downloading IMD real-time observed rainfall for date: {date_str}")

    try:
        data = imd.get_real_data(
            "rain",
            date_str,
            date_str,
            file_dir=IMD_REALTIME_DIR
        )
    except Exception as exc:
        print(f"IMD real-time rainfall unavailable for {date_str}: {exc}")
        return None

    if data is None:
        print(f"IMD real-time rainfall unavailable for {date_str}")
        return None

    try:
        return sanitize_realtime_dataset(data.get_xarray())
    except Exception as exc:
        print(f"Could not open IMD real-time rainfall for {date_str}: {exc}")
        return None


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


def empty_calibration_factors():
    return pd.DataFrame(
        columns=[
            "state",
            "district",
            "gfs_cal_factor",
            "gfs_cal_threshold_mm",
            "gfs_cal_samples"
        ]
    )


def district_calibration_factors(threshold_mm=CALIBRATION_THRESHOLD_MM):
    archive_path = os.path.join(OUTPUT_DIR, DAILY_VERIFICATION_CSV)

    if not os.path.exists(archive_path):
        return empty_calibration_factors()

    try:
        archive_df = pd.read_csv(
            archive_path,
            dtype={
                "state": str,
                "district": str,
                "forecast_issue_date": str,
                "cycle_utc": str,
                "valid_date": str
            }
        )
    except Exception as exc:
        print(f"Skipping calibrated GFS factors: {exc}")
        return empty_calibration_factors()

    required = {"state", "district", "imd_observed_mm"}
    if not required.issubset(archive_df.columns):
        return empty_calibration_factors()

    if "rain_gfs_24h_mm" in archive_df.columns:
        forecast_numeric = safe_numeric(archive_df["rain_gfs_24h_mm"])
    elif "rain_forecast_daily_mm" in archive_df.columns:
        forecast_numeric = safe_numeric(archive_df["rain_forecast_daily_mm"])
    else:
        return empty_calibration_factors()

    observed_numeric = safe_numeric(archive_df["imd_observed_mm"])
    observed_status = (
        archive_df["observed_status"]
        if "observed_status" in archive_df.columns
        else pd.Series("MATCHED", index=archive_df.index)
    )
    matched_mask = observed_status.astype(str).str.upper().eq("MATCHED")
    calibration_mask = (
        matched_mask &
        forecast_numeric.notna() &
        observed_numeric.notna() &
        (forecast_numeric > threshold_mm)
    )

    if not calibration_mask.any():
        return empty_calibration_factors()

    calibration_df = archive_df.loc[
        calibration_mask,
        ["state", "district"]
    ].copy()
    calibration_df["forecast"] = forecast_numeric.loc[calibration_mask]
    calibration_df["observed"] = observed_numeric.loc[calibration_mask]

    grouped = (
        calibration_df
        .groupby(["state", "district"], as_index=False)
        .agg(
            forecast_sum=("forecast", "sum"),
            observed_sum=("observed", "sum"),
            gfs_cal_samples=("forecast", "size")
        )
    )
    grouped = grouped[grouped["forecast_sum"] > 0].copy()

    if grouped.empty:
        return empty_calibration_factors()

    grouped["gfs_cal_factor"] = (
        grouped["observed_sum"] / grouped["forecast_sum"]
    ).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    grouped["gfs_cal_factor"] = grouped["gfs_cal_factor"].clip(
        CALIBRATION_FACTOR_MIN,
        CALIBRATION_FACTOR_MAX
    )
    grouped["gfs_cal_threshold_mm"] = threshold_mm

    return grouped[
        [
            "state",
            "district",
            "gfs_cal_factor",
            "gfs_cal_threshold_mm",
            "gfs_cal_samples"
        ]
    ]


def apply_district_calibration(latest_df):
    calibrated_df = latest_df.copy()
    factors_df = district_calibration_factors()

    if factors_df.empty:
        calibrated_df["gfs_cal_factor"] = 1.0
        calibrated_df["gfs_cal_threshold_mm"] = CALIBRATION_THRESHOLD_MM
        calibrated_df["gfs_cal_samples"] = 0
    else:
        calibrated_df = calibrated_df.merge(
            factors_df,
            on=["state", "district"],
            how="left"
        )
        calibrated_df["gfs_cal_factor"] = safe_numeric(
            calibrated_df["gfs_cal_factor"]
        ).fillna(1.0)
        calibrated_df["gfs_cal_threshold_mm"] = safe_numeric(
            calibrated_df["gfs_cal_threshold_mm"]
        ).fillna(CALIBRATION_THRESHOLD_MM)
        calibrated_df["gfs_cal_samples"] = safe_numeric(
            calibrated_df["gfs_cal_samples"]
        ).fillna(0).astype(int)

    raw_forecast = safe_numeric(calibrated_df["rain_gfs_mm"]).fillna(0.0)
    use_calibration = (
        (raw_forecast > CALIBRATION_THRESHOLD_MM) &
        (calibrated_df["gfs_cal_samples"] > 0)
    )

    calibrated_df["rain_gfs_cal_mm"] = raw_forecast
    calibrated_df.loc[use_calibration, "rain_gfs_cal_mm"] = (
        raw_forecast.loc[use_calibration] *
        calibrated_df.loc[use_calibration, "gfs_cal_factor"]
    )
    calibrated_df["alert_gfs_cal"] = (
        calibrated_df["rain_gfs_cal_mm"].apply(imd_alert)
    )

    print(
        "Paper-style GFS calibration: "
        f"{int(use_calibration.sum())} districts adjusted "
        f"(threshold > {CALIBRATION_THRESHOLD_MM:g} mm/day)."
    )

    return calibrated_df


UNIT_COLUMNS = [
    "unit_type",
    "unit_id",
    "unit_name",
    "basin",
    "sub_basin",
    "area_sqkm"
]

CROSSWALK_COLUMNS = [
    "parent_type",
    "parent_id",
    "parent_name",
    "parent_key",
    "parent_state",
    "parent_basin",
    "parent_sub_basin",
    "child_type",
    "child_id",
    "child_name",
    "child_key",
    "child_state",
    "child_basin",
    "child_sub_basin",
    "intersection_area_sqkm",
    "parent_overlap_pct",
    "child_overlap_pct"
]

DAILY_ARCHIVE_COLUMNS = UNIT_COLUMNS + [
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
    "gfs_cal_factor",
    "gfs_cal_threshold_mm",
    "gfs_cal_samples",
    "rain_gfs_cal_24h_mm",
    "rain_icon_24h_mm",
    "imd_observed_mm",
    "observed_status",
    "error_gfs_raw_mm",
    "abs_error_gfs_raw_mm",
    "pct_error_gfs_raw",
    "updated_at_utc"
]

THREE_H_ARCHIVE_COLUMNS = UNIT_COLUMNS + [
    "state",
    "district",
    "forecast_issue_date",
    "cycle_utc",
    "forecast_issue_time_utc",
    "from_hour",
    "to_hour",
    "interval_label",
    "valid_start_utc",
    "valid_end_utc",
    "valid_date",
    "rain_gfs_mm",
    "bias_factor",
    "rain_gfs_bc_mm",
    "rain_icon_mm",
    "imd_observed_mm",
    "observed_status",
    "error_gfs_raw_mm",
    "abs_error_gfs_raw_mm",
    "pct_error_gfs_raw",
    "updated_at_utc"
]

DAILY_ARCHIVE_FILES = {
    "district": DAILY_VERIFICATION_CSV,
    "subbasin": SUBBASIN_DAILY_VERIFICATION_CSV,
    "watershed": WATERSHED_DAILY_VERIFICATION_CSV
}

THREE_H_ARCHIVE_FILES = {
    "district": DISTRICT_3H_ARCHIVE_CSV,
    "subbasin": SUBBASIN_3H_ARCHIVE_CSV,
    "watershed": WATERSHED_3H_ARCHIVE_CSV
}


def issue_datetime():
    return datetime.strptime(f"{DATE}{CYCLE}", "%Y%m%d%H")


def ensure_columns(df, columns):
    result = df.copy()

    for col in columns:
        if col not in result.columns:
            result[col] = pd.NA

    return result[columns]


def normalize_daily_archive_identity(archive_df, unit_type):
    archive_df = ensure_columns(archive_df, DAILY_ARCHIVE_COLUMNS)
    archive_df["unit_type"] = archive_df["unit_type"].fillna(unit_type)
    archive_df["unit_type"] = archive_df["unit_type"].replace("", unit_type)

    if unit_type == "district":
        archive_df["state"] = archive_df["state"].fillna("")
        archive_df["district"] = archive_df["district"].fillna("")
        archive_df["unit_id"] = archive_df.apply(
            lambda row: row["unit_id"]
            if pd.notna(row["unit_id"]) and str(row["unit_id"]) not in ["", "nan"]
            else row_key_from_values(row["state"], row["district"]),
            axis=1
        )
        archive_df["unit_name"] = archive_df["unit_name"].fillna("")
        missing_name = archive_df["unit_name"].astype(str).isin(["", "nan"])
        archive_df.loc[missing_name, "unit_name"] = archive_df.loc[
            missing_name,
            "district"
        ]
    else:
        archive_df["state"] = archive_df["state"].fillna("")
        archive_df["district"] = archive_df["district"].fillna("")

    archive_df["cycle_utc"] = archive_df["cycle_utc"].astype(str).str.zfill(2)
    archive_df["forecast_issue_date"] = (
        archive_df["forecast_issue_date"].astype(str)
    )
    archive_df["valid_date"] = archive_df["valid_date"].astype(str)

    return archive_df


def archive_row_key(row):
    return f"{row.get('unit_type', '')}||{row.get('unit_id', '')}"


def prepare_daily_archive_rows(latest_df, unit_type):
    issue_dt = issue_datetime()
    valid_start = issue_dt
    valid_end = issue_dt + timedelta(hours=24)
    valid_date = valid_end.strftime("%Y%m%d")
    updated_at = iso_utc(datetime.utcnow())
    daily_df = latest_df.copy()

    if unit_type == "district":
        daily_df["unit_type"] = "district"
        daily_df["unit_id"] = daily_df.apply(
            lambda row: row_key_from_values(row["state"], row["district"]),
            axis=1
        )
        daily_df["unit_name"] = daily_df["district"]
        daily_df["basin"] = daily_df.get("basin", "")
        daily_df["sub_basin"] = ""
        daily_df["area_sqkm"] = pd.NA
    else:
        daily_df["state"] = ""
        daily_df["district"] = ""
        daily_df["gfs_cal_factor"] = pd.NA
        daily_df["gfs_cal_threshold_mm"] = pd.NA
        daily_df["gfs_cal_samples"] = 0
        daily_df["rain_gfs_cal_mm"] = pd.NA

    daily_df["rain_gfs_mm"] = safe_numeric(daily_df["rain_gfs_mm"])
    daily_df["rain_gfs_bc_mm"] = safe_numeric(daily_df["rain_gfs_bc_mm"])
    daily_df["gfs_cal_factor"] = safe_numeric(daily_df["gfs_cal_factor"])
    daily_df["gfs_cal_threshold_mm"] = safe_numeric(
        daily_df["gfs_cal_threshold_mm"]
    )
    daily_df["gfs_cal_samples"] = (
        safe_numeric(daily_df["gfs_cal_samples"]).fillna(0).astype(int)
    )
    daily_df["rain_gfs_cal_mm"] = safe_numeric(daily_df["rain_gfs_cal_mm"])
    daily_df["rain_icon_mm"] = safe_numeric(daily_df["rain_icon_mm"])
    daily_df["rain_forecast_daily_mm"] = daily_df["rain_gfs_mm"]

    daily_df = daily_df.rename(columns={
        "rain_gfs_mm": "rain_gfs_24h_mm",
        "rain_gfs_bc_mm": "rain_gfs_bc_24h_mm",
        "rain_gfs_cal_mm": "rain_gfs_cal_24h_mm",
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
    daily_df["error_gfs_raw_mm"] = pd.NA
    daily_df["abs_error_gfs_raw_mm"] = pd.NA
    daily_df["pct_error_gfs_raw"] = pd.NA
    daily_df["updated_at_utc"] = updated_at

    return ensure_columns(daily_df, DAILY_ARCHIVE_COLUMNS)


def prepare_3h_archive_rows(distribution_df, unit_type):
    issue_dt = issue_datetime()
    updated_at = iso_utc(datetime.utcnow())
    archive_df = distribution_df.copy()

    if unit_type == "district":
        archive_df["unit_type"] = "district"
        archive_df["unit_id"] = archive_df.apply(
            lambda row: row_key_from_values(row["state"], row["district"]),
            axis=1
        )
        archive_df["unit_name"] = archive_df["district"]
        archive_df["basin"] = archive_df.get("basin", "")
        archive_df["sub_basin"] = ""
        archive_df["area_sqkm"] = pd.NA
    else:
        archive_df["state"] = ""
        archive_df["district"] = ""

    archive_df["from_hour"] = safe_numeric(archive_df["from_hour"]).astype(int)
    archive_df["to_hour"] = safe_numeric(archive_df["to_hour"]).astype(int)
    archive_df["forecast_issue_date"] = DATE
    archive_df["cycle_utc"] = CYCLE
    archive_df["forecast_issue_time_utc"] = iso_utc(issue_dt)
    archive_df["valid_start_utc"] = archive_df["from_hour"].apply(
        lambda hour: iso_utc(issue_dt + timedelta(hours=int(hour)))
    )
    archive_df["valid_end_utc"] = archive_df["to_hour"].apply(
        lambda hour: iso_utc(issue_dt + timedelta(hours=int(hour)))
    )
    archive_df["valid_date"] = archive_df["to_hour"].apply(
        lambda hour: (issue_dt + timedelta(hours=int(hour))).strftime("%Y%m%d")
    )
    archive_df["imd_observed_mm"] = pd.NA
    archive_df["observed_status"] = "SUBDAILY_IMD_UNAVAILABLE"
    archive_df["error_gfs_raw_mm"] = pd.NA
    archive_df["abs_error_gfs_raw_mm"] = pd.NA
    archive_df["pct_error_gfs_raw"] = pd.NA
    archive_df["updated_at_utc"] = updated_at

    return ensure_columns(archive_df, THREE_H_ARCHIVE_COLUMNS)


def update_3h_archive(distribution_df, unit_type, archive_csv):
    archive_path = os.path.join(OUTPUT_DIR, archive_csv)
    latest_df = prepare_3h_archive_rows(distribution_df, unit_type)

    if os.path.exists(archive_path):
        archive_df = pd.read_csv(
            archive_path,
            dtype={
                "forecast_issue_date": str,
                "cycle_utc": str,
                "valid_date": str,
                "unit_id": str
            }
        )
        archive_df = ensure_columns(archive_df, THREE_H_ARCHIVE_COLUMNS)
    else:
        archive_df = pd.DataFrame(columns=THREE_H_ARCHIVE_COLUMNS)

    archive_df = pd.concat([archive_df, latest_df], ignore_index=True)
    archive_df["cycle_utc"] = archive_df["cycle_utc"].astype(str).str.zfill(2)
    archive_df["forecast_issue_date"] = (
        archive_df["forecast_issue_date"].astype(str)
    )
    archive_df["valid_date"] = archive_df["valid_date"].astype(str)
    archive_df = archive_df.drop_duplicates(
        subset=[
            "unit_type",
            "unit_id",
            "forecast_issue_date",
            "cycle_utc",
            "from_hour",
            "to_hour"
        ],
        keep="last"
    )
    cycle_keys = (
        archive_df[["forecast_issue_date", "cycle_utc"]]
        .drop_duplicates()
        .sort_values(["forecast_issue_date", "cycle_utc"])
        .tail(THREE_H_ARCHIVE_MAX_CYCLES)
    )
    archive_df = archive_df.merge(
        cycle_keys,
        on=["forecast_issue_date", "cycle_utc"],
        how="inner"
    )
    archive_df = archive_df[THREE_H_ARCHIVE_COLUMNS].sort_values(
        [
            "forecast_issue_date",
            "cycle_utc",
            "unit_type",
            "unit_name",
            "unit_id",
            "from_hour",
            "to_hour"
        ]
    )
    archive_df.to_csv(archive_path, index=False)

    return archive_df


def limit_daily_archive_dates(archive_df, unit_type):
    max_valid_dates = DAILY_ARCHIVE_MAX_VALID_DATES_BY_UNIT.get(unit_type, 365)

    if max_valid_dates <= 0 or archive_df.empty:
        return archive_df

    valid_dates = sorted(
        date_text
        for date_text in archive_df["valid_date"].dropna().astype(str).unique()
        if date_text and date_text.lower() != "nan"
    )

    if len(valid_dates) <= max_valid_dates:
        return archive_df

    keep_dates = set(valid_dates[-max_valid_dates:])
    return archive_df[archive_df["valid_date"].astype(str).isin(keep_dates)].copy()


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


def normalize_admin_name(value):
    text = "" if value is None else str(value).strip()
    text = text.replace(">", "A").replace("|", "I")
    text = " ".join(text.split())
    return text.title()


def load_district_boundaries():
    districts_gdf = gpd.read_file(DISTRICT_SHP)
    districts_gdf = districts_gdf[
        districts_gdf.geometry.notna() & ~districts_gdf.geometry.is_empty
    ].copy()

    missing_cols = [
        col for col in [DISTRICT_SOURCE_STATE_COL, DISTRICT_SOURCE_DIST_COL]
        if col not in districts_gdf.columns
    ]
    if missing_cols:
        raise RuntimeError(
            "District boundary shapefile is missing required field(s): " +
            ", ".join(missing_cols)
        )

    districts_gdf[STATE_COL] = (
        districts_gdf[DISTRICT_SOURCE_STATE_COL].map(normalize_admin_name)
    )
    districts_gdf[DIST_COL] = (
        districts_gdf[DISTRICT_SOURCE_DIST_COL].map(normalize_admin_name)
    )
    districts_gdf = districts_gdf[
        districts_gdf[STATE_COL].ne("") & districts_gdf[DIST_COL].ne("")
    ].copy()

    districts_gdf["geometry"] = districts_gdf.geometry.buffer(0)
    districts_gdf = districts_gdf[
        districts_gdf.geometry.notna() & ~districts_gdf.geometry.is_empty
    ].copy()

    districts_gdf = districts_gdf.to_crs("EPSG:4326")
    districts_gdf["geometry"] = districts_gdf.geometry.simplify(
        DISTRICT_PROCESS_SIMPLIFY_TOLERANCE,
        preserve_topology=True
    )
    districts_gdf["geometry"] = districts_gdf.geometry.buffer(0)
    districts_gdf["_unit_index"] = np.arange(len(districts_gdf))

    return districts_gdf[
        districts_gdf.geometry.notna() & ~districts_gdf.geometry.is_empty
    ].copy()


def write_district_geojson(districts_gdf):
    out_path = os.path.join(OUTPUT_DIR, DISTRICT_GEOJSON)
    web_gdf = districts_gdf[[STATE_COL, DIST_COL, "geometry"]].copy()
    web_gdf["geometry"] = web_gdf.geometry.simplify(
        DISTRICT_GEOJSON_SIMPLIFY_TOLERANCE,
        preserve_topology=True
    )
    web_gdf.to_file(out_path, driver="GeoJSON")


def empty_crosswalk(filename):
    out_path = os.path.join(OUTPUT_DIR, filename)
    pd.DataFrame(columns=CROSSWALK_COLUMNS).to_csv(out_path, index=False)
    print(f"Related layer crosswalk -> {filename}: 0 links")


def crosswalk_key(layer, unit_id, state="", district=""):
    if layer == "district":
        return f"district||{state}||{district}"
    return f"{layer}||{unit_id}"


def prepare_crosswalk_units(gdf, layer):
    if gdf is None or gdf.empty:
        return gpd.GeoDataFrame(columns=UNIT_COLUMNS + ["state", "district", "geometry"])

    if layer == "district":
        frame = gdf[[STATE_COL, DIST_COL, "geometry"]].copy()
        frame["state"] = clean_text(frame[STATE_COL])
        frame["district"] = clean_text(frame[DIST_COL])
        frame["unit_type"] = "district"
        frame["unit_id"] = frame["state"] + "||" + frame["district"]
        frame["unit_name"] = frame["district"]
        frame["basin"] = ""
        frame["sub_basin"] = ""
        frame["area_sqkm"] = 0.0
    else:
        frame = gdf[UNIT_COLUMNS + ["geometry"]].copy()
        frame["state"] = ""
        frame["district"] = ""
        frame["unit_type"] = layer
        frame["unit_id"] = clean_text(frame["unit_id"])
        frame["unit_name"] = clean_text(frame["unit_name"])
        frame["basin"] = clean_text(frame["basin"])
        frame["sub_basin"] = clean_text(frame["sub_basin"])

    frame = frame[frame.geometry.notna() & ~frame.geometry.is_empty].copy()
    frame["geometry"] = frame.geometry.buffer(0)
    frame = frame[frame.geometry.notna() & ~frame.geometry.is_empty].copy()
    frame["_cw_key"] = frame.apply(
        lambda row: crosswalk_key(
            layer,
            row["unit_id"],
            row.get("state", ""),
            row.get("district", "")
        ),
        axis=1
    )
    return frame


def safe_pct(part, total):
    return 0.0 if not total else (part / total) * 100.0


def write_unit_crosswalk(parent_gdf, child_gdf, parent_layer, child_layer, filename):
    parent = prepare_crosswalk_units(parent_gdf, parent_layer)
    child = prepare_crosswalk_units(child_gdf, child_layer)

    if parent.empty or child.empty:
        empty_crosswalk(filename)
        return pd.DataFrame(columns=CROSSWALK_COLUMNS)

    parent = parent.to_crs(CROSSWALK_AREA_CRS).reset_index(drop=True)
    child = child.to_crs(CROSSWALK_AREA_CRS).reset_index(drop=True)
    parent["_cw_area_sqkm"] = parent.geometry.area / 1_000_000.0
    child["_cw_area_sqkm"] = child.geometry.area / 1_000_000.0

    rows = []
    child_sindex = child.sindex

    for _, parent_row in parent.iterrows():
        candidate_indexes = child_sindex.query(
            parent_row.geometry,
            predicate="intersects"
        )

        for child_index in candidate_indexes:
            child_row = child.iloc[child_index]

            try:
                intersection = parent_row.geometry.intersection(child_row.geometry)
            except Exception:
                continue

            if intersection.is_empty:
                continue

            area_sqkm = intersection.area / 1_000_000.0
            if area_sqkm < CROSSWALK_MIN_AREA_SQKM:
                continue

            rows.append({
                "parent_type": parent_layer,
                "parent_id": parent_row["unit_id"],
                "parent_name": parent_row["unit_name"],
                "parent_key": parent_row["_cw_key"],
                "parent_state": parent_row.get("state", ""),
                "parent_basin": parent_row.get("basin", ""),
                "parent_sub_basin": parent_row.get("sub_basin", ""),
                "child_type": child_layer,
                "child_id": child_row["unit_id"],
                "child_name": child_row["unit_name"],
                "child_key": child_row["_cw_key"],
                "child_state": child_row.get("state", ""),
                "child_basin": child_row.get("basin", ""),
                "child_sub_basin": child_row.get("sub_basin", ""),
                "intersection_area_sqkm": round(area_sqkm, 4),
                "parent_overlap_pct": round(
                    safe_pct(area_sqkm, parent_row["_cw_area_sqkm"]),
                    2
                ),
                "child_overlap_pct": round(
                    safe_pct(area_sqkm, child_row["_cw_area_sqkm"]),
                    2
                )
            })

    out_df = pd.DataFrame(rows, columns=CROSSWALK_COLUMNS)
    if not out_df.empty:
        out_df = out_df.sort_values(
            ["parent_type", "parent_name", "child_type", "child_name"]
        )
    out_path = os.path.join(OUTPUT_DIR, filename)
    out_df.to_csv(out_path, index=False)
    print(f"Related layer crosswalk -> {filename}: {len(out_df)} links")
    return out_df


def write_related_crosswalks(districts_gdf, hydro_units):
    subbasins = hydro_units.get("subbasin") if hydro_units else None
    watersheds = hydro_units.get("watershed") if hydro_units else None

    write_unit_crosswalk(
        subbasins,
        districts_gdf,
        "subbasin",
        "district",
        SUBBASIN_DISTRICT_CROSSWALK_CSV
    )
    write_unit_crosswalk(
        subbasins,
        watersheds,
        "subbasin",
        "watershed",
        SUBBASIN_WATERSHED_CROSSWALK_CSV
    )
    write_unit_crosswalk(
        districts_gdf,
        watersheds,
        "district",
        "watershed",
        DISTRICT_WATERSHED_CROSSWALK_CSV
    )

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


def district_zonal_mean_points(da, value_col):
    unit_gdf = districts[[STATE_COL, DIST_COL, "_unit_index", "geometry"]].copy()
    unit_gdf["unit_type"] = "district"
    unit_gdf["unit_id"] = (
        unit_gdf[STATE_COL].astype(str) + "||" +
        unit_gdf[DIST_COL].astype(str)
    )
    unit_gdf["unit_name"] = unit_gdf[DIST_COL].astype(str)
    unit_gdf["basin"] = ""
    unit_gdf["sub_basin"] = ""
    unit_gdf["area_sqkm"] = 0.0

    means_df = zonal_mean_points(da, unit_gdf, value_col)
    means_df["state"] = means_df["unit_id"].str.split("||", regex=False).str[0]
    means_df["district"] = (
        means_df["unit_id"].str.split("||", regex=False).str[1]
    )

    return means_df[["state", "district", value_col]]


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

districts = load_district_boundaries()
write_district_geojson(districts)
hydrologic_units = load_hydrologic_units()
write_related_crosswalks(districts, hydrologic_units)

# ------------------------------------------------------------------
# STEP 1: IMD OBSERVED RAINFALL
# ------------------------------------------------------------------

today_local = datetime.now().date()
last_observed_date = today_local - timedelta(days=1)
realtime_imd_year = last_observed_date.year
baseline_imd_year = today_local.year - 1

print(f"Checking IMD real-time observed rainfall through: {last_observed_date:%Y-%m-%d}")
print(f"Using IMD annual bias baseline year: {baseline_imd_year}")

ds_imd = open_imd_year_dataset(baseline_imd_year, refresh=False)

if ds_imd is None:
    raise RuntimeError(
        "No usable IMD annual rainfall file is available for "
        f"{baseline_imd_year}."
    )

imd_mean_grid = ds_imd["rain"].mean(dim="time", skipna=True)
imd_df = district_zonal_mean_points(imd_mean_grid, "imd_mean_mm")

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

imd_year_cache = {baseline_imd_year: ds_imd}
imd_realtime_cache = {}


def get_imd_year_dataset(year):
    if year in imd_year_cache:
        return imd_year_cache[year]

    ds_year = open_imd_year_dataset(year, refresh=False)
    imd_year_cache[year] = ds_year
    return ds_year


def get_imd_realtime_day_dataset(date_obj):
    date_key = date_obj.strftime("%Y%m%d")

    if date_key not in imd_realtime_cache:
        imd_realtime_cache[date_key] = open_imd_realtime_day(date_obj)

    return imd_realtime_cache[date_key]


def observed_daily_rain_grid(date_yyyymmdd):
    try:
        date_obj = datetime.strptime(str(date_yyyymmdd), "%Y%m%d")
    except ValueError:
        return None, "UNAVAILABLE"

    if date_obj.date() > last_observed_date:
        return None, "PENDING"

    if date_obj.year == realtime_imd_year:
        ds_day = get_imd_realtime_day_dataset(date_obj.date())

        if ds_day is None:
            return None, "PENDING"

        return ds_day["rain"].isel(time=0), "MATCHED"

    ds_year = get_imd_year_dataset(date_obj.year)

    if ds_year is None:
        return None, "PENDING"

    target_time = pd.Timestamp(date_obj.date())
    available_times = pd.to_datetime(ds_year.time.values).normalize()

    if target_time not in set(available_times):
        return None, "PENDING"

    return ds_year["rain"].sel(time=target_time), "MATCHED"


def observed_rainfall_by_district(date_yyyymmdd):
    daily_rain, status = observed_daily_rain_grid(date_yyyymmdd)

    if status != "MATCHED":
        return {}, status

    means_df = district_zonal_mean_points(daily_rain, "imd_observed_mm")
    observed = {
        row_key_from_values(row["state"], row["district"]): row[
            "imd_observed_mm"
        ]
        for row in means_df.to_dict("records")
    }

    return observed, "MATCHED"


def observed_rainfall_by_layer(date_yyyymmdd, unit_type):
    if unit_type == "district":
        observed, status = observed_rainfall_by_district(date_yyyymmdd)

        return {
            f"district||{key}": value
            for key, value in observed.items()
        }, status

    daily_rain, status = observed_daily_rain_grid(date_yyyymmdd)

    if status != "MATCHED":
        return {}, status

    unit_gdf = hydrologic_units.get(unit_type)

    if unit_gdf is None:
        return {}, "UNAVAILABLE"

    means_df = zonal_mean_points(daily_rain, unit_gdf, "imd_observed_mm")

    return {
        f"{row['unit_type']}||{row['unit_id']}": row["imd_observed_mm"]
        for row in means_df.to_dict("records")
    }, "MATCHED"


def add_observed_values(archive_df, unit_type):
    if archive_df.empty:
        return archive_df

    archive_df = normalize_daily_archive_identity(archive_df, unit_type)
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
        observed, status = observed_rainfall_by_layer(valid_date, unit_type)
        date_mask = archive_df["valid_date"].astype(str) == valid_date
        missing_date_mask = date_mask & archive_df["imd_observed_mm"].isna()

        if status != "MATCHED":
            archive_df.loc[missing_date_mask, "observed_status"] = status
            continue

        for index, row in archive_df.loc[missing_date_mask].iterrows():
            key = archive_row_key(row)
            value = observed.get(key, float("nan"))

            if pd.notna(value):
                archive_df.at[index, "imd_observed_mm"] = value
                archive_df.at[index, "observed_status"] = "MATCHED"
            else:
                archive_df.at[index, "observed_status"] = "UNAVAILABLE"

    observed_numeric = safe_numeric(archive_df["imd_observed_mm"])
    forecast_numeric = safe_numeric(archive_df["rain_forecast_daily_mm"])
    valid_pair = observed_numeric.notna() & forecast_numeric.notna()

    archive_df["error_gfs_raw_mm"] = pd.NA
    archive_df["abs_error_gfs_raw_mm"] = pd.NA
    archive_df["pct_error_gfs_raw"] = pd.NA

    archive_df.loc[valid_pair, "error_gfs_raw_mm"] = (
        forecast_numeric[valid_pair] - observed_numeric[valid_pair]
    )
    archive_df.loc[valid_pair, "abs_error_gfs_raw_mm"] = (
        archive_df.loc[valid_pair, "error_gfs_raw_mm"].abs()
    )

    non_zero_observed = valid_pair & (observed_numeric.abs() >= 0.1)
    archive_df.loc[non_zero_observed, "pct_error_gfs_raw"] = (
        archive_df.loc[non_zero_observed, "error_gfs_raw_mm"] /
        observed_numeric[non_zero_observed] * 100.0
    )

    return archive_df


def update_daily_verification_archive(
    latest_df,
    unit_type,
    archive_csv,
    mirror_csv=None
):
    archive_path = os.path.join(OUTPUT_DIR, archive_csv)
    daily_df = prepare_daily_archive_rows(latest_df, unit_type)

    if os.path.exists(archive_path):
        archive_df = pd.read_csv(
            archive_path,
            dtype={
                "forecast_issue_date": str,
                "cycle_utc": str,
                "valid_date": str,
                "unit_id": str
            }
        )
    else:
        archive_df = pd.DataFrame(columns=DAILY_ARCHIVE_COLUMNS)

    archive_df = pd.concat([archive_df, daily_df], ignore_index=True)
    archive_df = normalize_daily_archive_identity(archive_df, unit_type)
    archive_df["rain_forecast_daily_mm"] = (
        safe_numeric(archive_df["rain_gfs_24h_mm"])
        .fillna(safe_numeric(archive_df["rain_forecast_daily_mm"]))
    )
    archive_df = archive_df.drop_duplicates(
        subset=[
            "unit_type",
            "unit_id",
            "forecast_issue_date",
            "cycle_utc",
            "valid_date"
        ],
        keep="last"
    )

    archive_df = add_observed_values(archive_df, unit_type)
    archive_df = limit_daily_archive_dates(archive_df, unit_type)
    archive_df = archive_df[DAILY_ARCHIVE_COLUMNS].sort_values(
        [
            "valid_date",
            "unit_type",
            "unit_name",
            "unit_id",
            "forecast_issue_date",
            "cycle_utc"
        ]
    )
    archive_df.to_csv(archive_path, index=False, float_format=CSV_FLOAT_FORMAT)

    if mirror_csv:
        archive_df.to_csv(
            os.path.join(OUTPUT_DIR, mirror_csv),
            index=False,
            float_format=CSV_FLOAT_FORMAT
        )

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

    district_means_df = district_zonal_mean_points(
        rain_inc,
        "rain_gfs_mm"
    )
    for row in district_means_df.to_dict("records"):
        gfs_records.append({
            "state": row["state"],
            "district": row["district"],
            "from_hour": from_hr,
            "to_hour": fh,
            "rain_gfs_mm": row["rain_gfs_mm"]
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

    district_means_df = district_zonal_mean_points(
        rain_inc,
        "rain_icon_mm"
    )
    for row in district_means_df.to_dict("records"):
        icon_records.append({
            "state": row["state"],
            "district": row["district"],
            "from_hour": from_hr,
            "to_hour": fh,
            "rain_icon_mm": row["rain_icon_mm"]
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
final_df["alert_gfs_raw"] = final_df["rain_gfs_mm"].apply(imd_alert)
final_df["alert_gfs_bc"] = final_df["rain_gfs_bc_mm"].apply(imd_alert)
final_df["alert_icon"] = final_df["rain_icon_mm"].apply(imd_alert)
final_df = apply_district_calibration(final_df)

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

daily_verification_outputs = {
    "district": update_daily_verification_archive(
        final_df,
        "district",
        DAILY_VERIFICATION_CSV,
        mirror_csv=DISTRICT_DAILY_VERIFICATION_ALIAS_CSV
    )
}
three_hour_archive_outputs = {
    "district": update_3h_archive(
        distribution_df,
        "district",
        DISTRICT_3H_ARCHIVE_CSV
    )
}

for unit_key, output in hydrologic_outputs.items():
    daily_verification_outputs[unit_key] = update_daily_verification_archive(
        output["final"],
        unit_key,
        DAILY_ARCHIVE_FILES[unit_key]
    )
    three_hour_archive_outputs[unit_key] = update_3h_archive(
        output["distribution"],
        unit_key,
        THREE_H_ARCHIVE_FILES[unit_key]
    )

# ------------------------------------------------------------------
# STEP 6: SAVE OUTPUTS
# ------------------------------------------------------------------

final_df.to_csv(
    f"{OUTPUT_DIR}/India_24h_GFS_IMD_ICON_Rainfall.csv",
    index=False
)

alert_df = final_df[
    (final_df["alert_gfs_raw"] != "NO ALERT") |
    (final_df["alert_gfs_cal"] != "NO ALERT") |
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
print(
    "Daily verification records : " +
    ", ".join(
        f"{key}={df.shape[0]}"
        for key, df in daily_verification_outputs.items()
    )
)
print(
    "3-hour archive records : " +
    ", ".join(
        f"{key}={df.shape[0]}"
        for key, df in three_hour_archive_outputs.items()
    )
)
for unit_key, output in hydrologic_outputs.items():
    print(
        f"{unit_key.title()} units : {output['final'].shape[0]} | "
        f"3-hour records : {output['distribution'].shape[0]}"
    )
print("==============================================")
