@echo off
echo ================================================
echo   Shipvoid Forecast - Publish to GitHub Pages
echo ================================================
echo.

:: Check for virtual environment
if not exist .venv\Scripts\python.exe (
    echo ERROR: Virtual environment not found!
    echo Please run: uv venv ^&^& uv pip install -r requirements.txt --index-url https://pypi.ci.artifacts.walmart.com/artifactory/api/pypi/external-pypi/simple --allow-insecure-host pypi.ci.artifacts.walmart.com
    pause
    exit /b 1
)

:: Generate the report
echo [1/3] Generating static HTML report...
echo.
.venv\Scripts\python generate_report.py

:: Check if report was generated
if not exist shipvoid_crossref_report.html (
    echo.
    echo ERROR: Report generation failed!
    echo Make sure you have Shipvoid*.xlsm and Legacy*.csv files in this folder.
    pause
    exit /b 1
)

:: Copy to docs folder for GitHub Pages
echo.
echo [2/3] Copying report to docs/ folder for GitHub Pages...
copy /Y shipvoid_crossref_report.html docs\index.html
copy /Y team-logo.png docs\team-logo.png 2>nul
echo.

:: Git commit and push
echo [3/3] Committing and pushing to GitHub...
echo.
git add docs/
git commit -m "Update GitHub Pages report - %date% %time%"
echo.
echo ================================================
echo   SUCCESS! Report published to docs/index.html
echo ================================================
echo.
echo Next steps:
echo   1. Push to GitHub: git push origin main
echo   2. Enable GitHub Pages in repo Settings:
echo      - Go to Settings ^> Pages
echo      - Source: Deploy from a branch
echo      - Branch: main, Folder: /docs
echo   3. Your report will be live at:
echo      https://gecgithub01.walmart.com/pages/YOUR-ORG/Shipvoid_Forecast
echo.
pause
