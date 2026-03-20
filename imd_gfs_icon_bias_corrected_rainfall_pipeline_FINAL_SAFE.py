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
import requests
import imdlib as imd
import xarray as xr
import pandas as pd
import geopandas as gpd
import rioxarray
from datetime import datetime, timedelta

# ------------------------------------------------------------------
# USER SETTINGS
# ------------------------------------------------------------------

DISTRICT_SHP = r"data/India_Districts.shp"
DIST_COL = "NAME_2"
STATE_COL = "NAME_1"

IMD_DIR = "IMD_Rainfall"
GFS_DIR = "GFS_TMP"
ICON_DIR = "ICON_TMP"
# OUTPUT_DIR = "FINAL_OUTPUT"
OUTPUT_DIR = "."

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

def get_latest_gfs_datetime():
    today = datetime.utcnow()
    yesterday = today - timedelta(days=1)

    for d in [today, yesterday]:
        date_str = d.strftime("%Y%m%d")
        for cycle in ["18", "12", "06", "00"]:
            url = f"{GFS_BASE_URL}/gfs.{date_str}/{cycle}/atmos/"
            if requests.head(url).status_code == 200:
                return date_str, cycle

    raise RuntimeError("No GFS data available")


def download_gfs(date, cycle, fh):
    fname = f"gfs.t{cycle}z.pgrb2.{RESOLUTION}.f{fh:03d}"
    url = f"{GFS_BASE_URL}/gfs.{date}/{cycle}/atmos/{fname}"
    path = os.path.join(GFS_DIR, fname)

    r = requests.get(url, stream=True)
    r.raise_for_status()

    with open(path, "wb") as f:
        for c in r.iter_content(8192):
            f.write(c)

    return path


def read_gfs_tp(grib):
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

            r = requests.get(url, stream=True)
            if r.status_code == 200:
                with open(path, "wb") as f:
                    for c in r.iter_content(8192):
                        f.write(c)
                print(f"ICON OK → {date} {run_hour} fh{fh:03d}")
                return path

    print(f"ICON missing → fh{fh:03d} (skipped)")
    return None


def read_icon_tp(grib):
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

# ------------------------------------------------------------------
# LOAD DISTRICTS
# ------------------------------------------------------------------

districts = gpd.read_file(DISTRICT_SHP).to_crs("EPSG:4326")

# ------------------------------------------------------------------
# STEP 1: IMD OBSERVED RAINFALL (LAST COMPLETE YEAR)
# ------------------------------------------------------------------

last_complete_year = datetime.utcnow().year - 1
print(f"Using IMD rainfall data for year: {last_complete_year}")

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

# ------------------------------------------------------------------
# STEP 2: GFS FORECAST (NEXT 24 HOURS)
# ------------------------------------------------------------------

DATE, CYCLE = get_latest_gfs_datetime()
print(f"GFS Forecast → {DATE} | Cycle {CYCLE} UTC")

gfs_records = []

prev_tp = None
prev_file = None
prev_hr = None

for fh in FORECAST_HOURS:
    f = download_gfs(DATE, CYCLE, fh)
    tp = read_gfs_tp(f)

    if prev_tp is not None:
        rain_inc = (tp - prev_tp).where((tp - prev_tp) >= 0, 0)

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
                "from_hour": prev_hr,
                "to_hour": fh,
                "rain_gfs_mm": val
            })

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
# STEP 3: BIAS CORRECTION (IMD → GFS)
# ------------------------------------------------------------------

bc_df = gfs_24h.merge(imd_df, on=["state", "district"], how="left")

def bias_factor(row):
    if row["rain_gfs_mm"] < 0.1:
        return 1.0
    bf = row["imd_mean_mm"] / row["rain_gfs_mm"]
    return min(max(bf, 0.3), 3.0)

bc_df["bias_factor"] = bc_df.apply(bias_factor, axis=1)
bc_df["rain_gfs_bc_mm"] = bc_df["rain_gfs_mm"] * bc_df["bias_factor"]

# ------------------------------------------------------------------
# STEP 4: ICON FORECAST (SAFE, OPTIONAL)
# ------------------------------------------------------------------

icon_records = []

prev_tp = None
prev_file = None
prev_hr = None

for fh in FORECAST_HOURS:
    f = download_icon_safe(DATE, fh, ICON_DIR)

    if f is None:
        continue

    tp = read_icon_tp(f)

    if prev_tp is not None:
        rain_inc = (tp - prev_tp).where((tp - prev_tp) >= 0, 0)

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
                "from_hour": prev_hr,
                "to_hour": fh,
                "rain_icon_mm": val
            })

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
    icon_24h = pd.DataFrame(
        columns=["state", "district", "rain_icon_mm"]
    )

# ------------------------------------------------------------------
# STEP 5: FINAL MERGE + ALERTS
# ------------------------------------------------------------------

final_df = bc_df.merge(icon_24h, on=["state", "district"], how="left")
# final_df["rain_icon_mm"].fillna(0.0, inplace=True)
# final_df["rain_icon_mm"] = (
#     final_df["rain_icon_mm"]
#     .fillna(0.0)
#     .infer_objects(copy=False)
# )
final_df["rain_icon_mm"] = (
    final_df["rain_icon_mm"]
    .fillna(0.0)
    .astype(float)
)

final_df["alert_gfs_bc"] = final_df["rain_gfs_bc_mm"].apply(imd_alert)
final_df["alert_icon"] = final_df["rain_icon_mm"].apply(imd_alert)

final_df["date"] = DATE
final_df["cycle_utc"] = CYCLE

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

print("\n==============================================")
print("FINAL SAFE GFS + ICON PIPELINE COMPLETED")
print(f"Total districts : {final_df.shape[0]}")
print(f"Alert districts : {alert_df.shape[0]}")
print("==============================================")
