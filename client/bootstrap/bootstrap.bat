@echo off
REM ============================================================================
REM AgentHub worker bootstrap - zero-Python launcher (V1.7, Windows)
REM
REM   bootstrap.bat --server http://<server>:8000 --token DL-XXXX-XXXX ^
REM                 [--device-name NAME] [--download-worker] [--install-dir DIR]
REM
REM What it does when Python is MISSING on this machine (auto-provision, in
REM priority order):
REM   1. AgentHub server LAN mode:  <server>/api/bootstrap/python-runtime
REM      (the admin stages ONE python installer into storage/bootstrap/python/)
REM   2. Public fallback: python.org official installer
REM   The installer runs SILENTLY per-user into %AGENTHUB_PY_HOME% and is NOT
REM   added to PATH - this machine's system Python stays untouched.
REM
REM Fully headless alternative (no Python at all, ever): build the one-file
REM   bootstrap.exe once with scripts\build_bootstrap_exe.ps1 and ship that.
REM ============================================================================

setlocal enabledelayedexpansion
set "PY_HOME=%ProgramData%\AgentHub\Python"
set "PYTHON_EXE=%PY_HOME%\python.exe"

REM --- 1. locate a usable python ------------------------------------------------
set "PY="
where py >nul 2>nul && set "PY=py -3"
if not defined PY (
    where python >nul 2>nul && set "PY=python"
)
if not defined PY (
    if exist "%PYTHON_EXE%" set "PY=%PYTHON_EXE%"
)

REM --- 2. auto-provision python if we found none --------------------------------
if not defined PY (
    if "%~1"=="" (
        echo [bootstrap] ERROR: no arguments ^(need --server^) - nothing to auto-provision for.
        exit /b 2
    )
    call :parse_server %*
    echo [bootstrap] no Python found on this machine - auto-provisioning...
    set "PY_URL=%AGENTHUB_SERVER%/api/bootstrap/python-runtime"
    mkdir "%TEMP%\agenthub-bootstrap" 2>nul
    set "INSTALLER=%TEMP%\agenthub-bootstrap\python-runtime.exe"
    echo [bootstrap] downloading python runtime from !PY_URL! ...
    curl.exe -fsSL -o "!INSTALLER!" "!PY_URL!" -H "X-Admin-Token: %ADMIN_TOKEN%"
    if errorlevel 1 (
        echo [bootstrap] server has no staged runtime ^(needs admin to stage it^);
        echo [bootstrap] falling back to the official python.org installer ...
        curl.exe -fsSL -o "!INSTALLER!" https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe
        if errorlevel 1 (
            echo [bootstrap] ERROR: cannot download python from server or python.org.
            echo [bootstrap] LAN without internet? Stage an installer at the server:
            echo             server storage/bootstrap/python/python-3.11.9-amd64.exe
            exit /b 3
        )
    )
    echo [bootstrap] installing python silently to %PY_HOME% ...
    "!INSTALLER!" /quiet InstallAllUsers=0 PrependPath=0 TargetDir="%PY_HOME%" Include_test=0
    if errorlevel 1 (
        echo [bootstrap] ERROR: silent python install failed - run "!INSTALLER!" manually.
        exit /b 4
    )
    if not exist "%PYTHON_EXE%" (
        echo [bootstrap] ERROR: python install finished but %PYTHON_EXE% not found.
        exit /b 4
    )
    set "PY=%PYTHON_EXE%"
    echo [bootstrap] python ready: %PYTHON_EXE%
)

REM --- 3. run the real bootstrap script ----------------------------------------
set "SCRIPT=%~dp0bootstrap.py"
if not exist "%SCRIPT%" set "SCRIPT=%~dp0..\bootstrap\bootstrap.py"
if not exist "%SCRIPT%" (
    echo [bootstrap] ERROR: bootstrap.py not found next to bootstrap.bat
    exit /b 5
)
%PY% "%SCRIPT%" %*
exit /b %errorlevel%

REM --- helper: extract --server value for the runtime download URL --------------
:parse_server
:parse_loop
if "%~1"=="" goto :parse_done
if /i "%~1"=="--server" (
    set "AGENTHUB_SERVER=%~2"
    shift
)
if /i "%~1"=="--admin-token" set "ADMIN_TOKEN=%~2"
shift
goto :parse_loop
:parse_done
if not defined AGENTHUB_SERVER (
    echo [bootstrap] ERROR: --server is required when python must be auto-provisioned.
    exit /b 2
)
goto :eof
