@echo off

echo =====================================
echo   Running Rainfall DSS Auto Update
echo =====================================

REM Activate Anaconda
CALL "%USERPROFILE%\anaconda3\Scripts\activate.bat"
CALL conda activate gfs

G:
cd "G:\My Drive\RAINFALL_FORECAST_INDIA\rainfall_web"

echo Running rainfall model...
python imd_gfs_icon_bias_corrected_rainfall_pipeline_FINAL_SAFE.py

echo Adding files...
git add .

echo Pulling latest changes...
git pull origin main --rebase

echo Committing...
git commit -m "Auto update"

echo Pushing...
git push origin main

echo =====================================
echo   Update Completed
echo =====================================

pause