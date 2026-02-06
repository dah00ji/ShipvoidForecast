@echo off
echo ================================================
echo   Shipvoid Forecast Dashboard
echo ================================================
echo.
echo Starting server on http://127.0.0.1:8050
echo.
echo Data Source: \\us06006w8000d2a.s06006.us.wal-mart.com\Rdrive\Ship_Void_Forecast
echo.
echo Press Ctrl+C to stop the server.
echo.

if exist .venv\Scripts\python.exe (
    .venv\Scripts\python app.py
) else (
    echo ERROR: Virtual environment not found!
    echo Please run: uv venv ^&^& uv pip install -r requirements.txt --index-url https://pypi.ci.artifacts.walmart.com/artifactory/api/pypi/external-pypi/simple --allow-insecure-host pypi.ci.artifacts.walmart.com
    pause
)
