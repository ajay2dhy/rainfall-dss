@echo off

echo =====================================
echo   Rainfall DSS Auto Update Started
echo =====================================

REM Activate Anaconda
CALL "%USERPROFILE%\anaconda3\Scripts\activate.bat"
CALL conda activate gfs

REM Go to project directory
G:
cd "G:\My Drive\RAINFALL_FORECAST_INDIA\rainfall_web"

echo.
echo STEP 1: Pull latest changes from GitHub
git pull origin main --rebase

echo.
echo STEP 2: Running rainfall pipeline
python imd_gfs_icon_bias_corrected_rainfall_pipeline_FINAL_SAFE.py

echo.
echo STEP 3: Adding updated files
git add .

echo.
echo STEP 4: Committing changes
git commit -m "Auto update"

echo.
echo STEP 5: Pushing to GitHub
git push origin main

echo.
echo =====================================
echo   DSS Auto Update Completed
echo =====================================

pause