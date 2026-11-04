@echo off
setlocal
cd /d "G:\My Drive\RAINFALL_FORECAST_INDIA\rainfall_web"

call :main > run_details.txt 2>&1
set EXITCODE=%ERRORLEVEL%
type run_details.txt
exit /b %EXITCODE%

:main
echo =====================================
echo   Rainfall DSS Auto Update Started
echo   Time: %DATE% %TIME%
echo =====================================

CALL "%USERPROFILE%\anaconda3\Scripts\activate.bat"
if errorlevel 1 (
    echo ERROR: Failed to activate Anaconda base script.
    goto fail
)

CALL conda activate gfs
if errorlevel 1 (
    echo ERROR: Failed to activate conda environment 'gfs'.
    goto fail
)

cd /d "G:\My Drive\RAINFALL_FORECAST_INDIA\rainfall_web"
if errorlevel 1 goto fail

echo.
echo [Step 1] Cleaning uncommitted changes from prior aborted runs...
git reset --hard HEAD
git clean -fd

echo.
echo [Step 2] Pulling latest from GitHub...
git pull origin main --rebase
if errorlevel 1 (
    echo ERROR: git pull failed.
    goto fail
)

echo.
echo [Step 3] Running rainfall pipeline...
python imd_gfs_icon_bias_corrected_rainfall_pipeline_FINAL_SAFE.py
if errorlevel 1 (
    echo ERROR: Python rainfall pipeline failed.
    goto fail
)

echo.
echo [Step 4] Checking GitHub file size limit (100 MB)...
powershell -NoProfile -ExecutionPolicy Bypass -Command "$bad=Get-ChildItem -File | Where-Object {$_.Length -ge 100MB}; if ($bad) { Write-Host 'Files over GitHub 100 MB limit:'; $bad | ForEach-Object { Write-Host ('  {0} ({1:N2} MB)' -f $_.Name, ($_.Length/1MB)) }; exit 1 }"
if errorlevel 1 (
    echo ERROR: One or more files exceed GitHub 100 MB limit.
    goto fail
)

echo.
echo [Step 5] Adding files to git...
git add .
if errorlevel 1 (
    echo ERROR: git add failed.
    goto fail
)

git diff --cached --quiet
if "%ERRORLEVEL%"=="0" (
    echo No changes to commit.
) else (
    echo Committing changes...
    git commit -m "Auto update"
    if errorlevel 1 (
        echo ERROR: git commit failed.
        goto fail
    )
)

echo.
echo [Step 6] Pushing to GitHub...
git push origin main
if errorlevel 1 (
    echo ERROR: git push failed.
    goto fail
)

echo.
echo =====================================
echo   DSS Auto Update Completed Successfully
echo   Time: %DATE% %TIME%
echo =====================================
exit /b 0

:fail
echo.
echo =====================================
echo ERROR: Rainfall DSS Auto Update failed!
echo Time: %DATE% %TIME%
echo =====================================
exit /b 1
