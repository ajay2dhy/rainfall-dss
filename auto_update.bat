@echo off

echo =====================================
echo   Running Rainfall DSS Auto Update
echo =====================================

REM Go to your project folder
G:
cd "G:\My Drive\RAINFALL_FORECAST_INDIA\rainfall_web"

REM Run Python rainfall pipeline
echo Running rainfall model...
"C:\Users\PC\anaconda3\python.exe" imd_gfs_icon_bias_corrected_rainfall_pipeline_FINAL_SAFE.py

REM Add updated files
echo Adding files...
git add .

REM Commit with timestamp
echo Committing...
git commit -m "Auto update"

REM Push to GitHub
echo Pushing to GitHub...
git push origin main

echo =====================================
echo   Update Completed
echo =====================================

pause