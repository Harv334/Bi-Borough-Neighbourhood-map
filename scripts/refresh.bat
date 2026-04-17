@echo off
REM =========================================================================
REM NW London Health Pipeline - one-button refresh (Windows)
REM
REM Run this from the repo root:  scripts\refresh.bat
REM
REM Requires Python 3.10+ installed from python.org (or via winget).
REM DO NOT use the Microsoft Store redirect shortcut - it will fail.
REM =========================================================================

setlocal enabledelayedexpansion
cd /d "%~dp0\.."

REM ---- 1. Find a real Python interpreter --------------------------------
REM Try `py -3` (the Windows Python launcher) first, then fall back.
set "PY_CMD="
py -3 --version >nul 2>&1
if not errorlevel 1 set "PY_CMD=py -3"

if not defined PY_CMD (
    python --version >nul 2>&1
    if not errorlevel 1 (
        REM Filter out the Microsoft Store stub which "succeeds" but does nothing
        for /f "delims=" %%v in ('python --version 2^>^&1') do set "PYV=%%v"
        echo \!PYV\! | findstr /i /c:"Python was not found" >nul
        if errorlevel 1 set "PY_CMD=python"
    )
)

if not defined PY_CMD (
    python3 --version >nul 2>&1
    if not errorlevel 1 set "PY_CMD=python3"
)

if not defined PY_CMD (
    echo.
    echo *** No working Python found on PATH. ***
    echo.
    echo Install Python 3.10 or later from one of these:
    echo.
    echo   1. https://www.python.org/downloads/ ^(easiest, tick "Add to PATH"^)
    echo   2. winget install Python.Python.3.12
    echo   3. Microsoft Store: search "Python 3.12" ^(not the redirect stub^)
    echo.
    echo If `python` opens the Microsoft Store and nothing else:
    echo   Settings -^> Apps -^> Advanced app settings -^> App execution aliases
    echo   -^> turn OFF "App Installer python.exe" and "App Installer python3.exe"
    echo.
    exit /b 1
)

echo Using interpreter: %PY_CMD%
%PY_CMD% --version

REM ---- 2. Create venv if needed ----------------------------------------
if not exist .venv (
    echo Creating .venv...
    %PY_CMD% -m venv .venv
    if errorlevel 1 goto :error
)

REM ---- 3. Activate and install ------------------------------------------
echo Activating .venv...
call .venv\Scripts\activate.bat
if errorlevel 1 goto :error

echo Upgrading pip...
python -m pip install --quiet --upgrade pip
if errorlevel 1 goto :error

echo Installing pipeline package...
python -m pip install --quiet -e .
if errorlevel 1 goto :error

REM ---- 4. Run ------------------------------------------------------------
echo.
echo ==== Running pipeline ====
pipeline run %*
if errorlevel 1 goto :error

echo.
echo ==== Done. Git status: ====
git status --short

echo.
echo If the output looks right:
echo   git add . ^&^& git commit -m "data: refresh %DATE%"
exit /b 0

:error
echo.
echo *** refresh.bat failed. See messages above. ***
exit /b 1
