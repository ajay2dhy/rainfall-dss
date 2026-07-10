@echo off

echo =====================================
echo   Rainfall DSS Auto Update Started
echo =====================================

CALL "%USERPROFILE%\anaconda3\Scripts\activate.bat"
if errorlevel 1 goto fail

CALL conda activate gfs
if errorlevel 1 goto fail

cd /d "G:\My Drive\RAINFALL_FORECAST_INDIA\rainfall_web"
if errorlevel 1 goto fail

echo Pulling latest...
git pull origin main --rebase
if errorlevel 1 goto fail

echo Running rainfall pipeline...
python imd_gfs_icon_bias_corrected_rainfall_pipeline_FINAL_SAFE.py
if errorlevel 1 goto fail

echo Checking GitHub file size limit...
powershell -NoProfile -ExecutionPolicy Bypass -Command "$bad=Get-ChildItem -File | Where-Object {$_.Length -ge 100MB}; if ($bad) { Write-Host 'Files over GitHub 100 MB limit:'; $bad | ForEach-Object { Write-Host ('  {0} ({1:N2} MB)' -f $_.Name, ($_.Length/1MB)) }; exit 1 }"
if errorlevel 1 goto fail

echo Adding files...
git add .
if errorlevel 1 goto fail

git diff --cached --quiet
if "%ERRORLEVEL%"=="0" (
    echo No changes to commit.
) else (
    echo Committing...
    git commit -m "Auto update"
    if errorlevel 1 goto fail
)

echo Pushing...
git push origin main
if errorlevel 1 goto fail

echo =====================================
echo   DSS Auto Update Completed
echo =====================================

exit /b 0

:fail
echo.
echo ERROR: Rainfall DSS Auto Update failed. Please check the message above.
echo =====================================
exit /b 1
