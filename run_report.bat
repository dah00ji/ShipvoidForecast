@echo off
echo ================================================
echo Shipvoid Forecast Cross-Reference Report Generator
echo ================================================
echo.
echo This will download the NEWEST Shipvoid and Legacy files
echo from SharePoint and generate the HTML report.
echo.
echo Data Source: SharePoint - Atlas Ambient RDC Playbook Planning
echo Folder: Shipvoid Forecast/6031
echo.
echo Options:
echo   --skip-download : Use local files only (no SharePoint download)
echo.

if "%1"=="--skip-download" (
    echo Running with --skip-download flag...
    .venv\Scripts\python generate_report.py --skip-download
) else if "%1"=="--local" (
    echo Running with --local flag...
    .venv\Scripts\python generate_report.py --local
) else (
    .venv\Scripts\python generate_report.py
)

echo.
echo Press any key to exit...
pause >nul
