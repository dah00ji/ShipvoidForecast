@echo off
echo ================================================
echo Shipvoid Forecast Cross-Reference Report Generator
echo ================================================
echo.
echo This will find the NEWEST Shipvoid and Legacy files
echo in this folder and generate the HTML report.
echo.

.venv\Scripts\python generate_report.py

echo.
echo Press any key to exit...
pause >nul
