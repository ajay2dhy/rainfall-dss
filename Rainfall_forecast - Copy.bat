@echo off

echo =====================================
echo   Rainfall DSS Auto Update Started
echo =====================================

CALL "%USERPROFILE%\anaconda3\Scripts\activate.bat"
CALL conda activate gfs

G:
cd "G:\My Drive\RAINFALL_FORECAST_INDIA\rainfall_web"

echo Running rainfall pipeline...
python imd_gfs_icon_bias_corrected_rainfall_pipeline_FINAL_SAFE.py

echo Adding files...
git add .

echo Committing...
git commit -m "Auto update" || echo No changes

echo Pulling latest...
git pull origin main --rebase

echo Pushing...
git push origin main

echo =====================================
echo   DSS Auto Update Completed
echo =====================================

pause