@echo off
rem ======================================================================
rem  Distributed Ticket Booking - one-click demo for Windows
rem
rem  Double-click this file (or run it from cmd / PowerShell).
rem   1. Uses the project's own environment (.demo_venv) if it exists.
rem   2. Otherwise finds Python 3.10+ on this computer and creates it.
rem   3. If there is no Python 3.10+ at all, downloads a private
rem      Python 3.11 into .demo_tools (no admin rights needed).
rem  Then it installs the requirements, downloads the LLM once and opens
rem  the 5 terminals: T1 LLM server, T2-T4 Raft nodes, T5 client.
rem
rem  Options:  run_demo.bat --no-llm      skip PyTorch / the model
rem            run_demo.bat --keep        keep saved Raft data
rem            run_demo.bat --background  no new windows, logs in .demo\logs
rem  Your own Python installations, virtual environments and conda envs
rem  are only read (to find a Python), never changed: everything goes into
rem  .demo_venv inside this folder.
rem ======================================================================
setlocal
rem Don't let an active venv/conda env or PYTHON*/PIP_* settings redirect anything.
set "PYTHONHOME="
set "PYTHONPATH="
set "PYTHONUSERBASE="
set "PYTHONSTARTUP="
set "VIRTUAL_ENV="
set "PYTHONNOUSERSITE=1"
for /f "delims==" %%V in ('set PIP_ 2^>nul') do set "%%V="
cd /d "%~dp0"
title Distributed Ticket Booking - launcher
set "VENVPY=%CD%\.demo_venv\Scripts\python.exe"
set "TOOLS=%CD%\.demo_tools"
set "CHECK=import sys, venv, ensurepip; sys.exit(0 if sys.version_info >= (3, 10) else 1)"
set "PYEXE="

rem ---- 1. existing project environment --------------------------------
if not exist "%VENVPY%" goto find_python
"%VENVPY%" -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)" >nul 2>&1
if errorlevel 1 goto broken_venv
set "PYEXE="%VENVPY%""
goto run

:broken_venv
if exist "%CD%\.demo_venv\pyvenv.cfg" goto remove_venv
echo %CD%\.demo_venv exists but is not a virtual environment; not touching it.
echo Rename or remove it and run this again.
pause
exit /b 1
:remove_venv
echo The existing .demo_venv does not work any more; recreating it.
rmdir /s /q "%CD%\.demo_venv"

rem ---- 2. find Python 3.10+ on this computer ---------------------------
:find_python
if defined DEMO_FORCE_UV goto download_python
call :try py -3.12
call :try py -3.11
call :try py -3.13
call :try py -3.10
call :try py -3
call :try python
call :try python3
for /d %%D in ("%LOCALAPPDATA%\Programs\Python\Python3*") do call :try "%%~D\python.exe"
for /d %%D in ("%ProgramFiles%\Python3*") do call :try "%%~D\python.exe"
call :try "%USERPROFILE%\miniforge3\python.exe"
call :try "%USERPROFILE%\miniconda3\python.exe"
call :try "%USERPROFILE%\anaconda3\python.exe"
call :try "%ProgramData%\miniforge3\python.exe"
call :try "%ProgramData%\miniconda3\python.exe"
call :try "%ProgramData%\anaconda3\python.exe"
if defined PYEXE goto run

rem ---- 3. no usable Python: download a private one with uv ------------
:download_python
echo.
echo No Python 3.10+ found on this computer.
echo Downloading a private Python 3.11 into .demo_tools (one time, about 40 MB; no admin rights needed) ...
if not exist "%TOOLS%" mkdir "%TOOLS%"
if exist "%TOOLS%\uv.exe" goto have_uv
powershell -NoProfile -ExecutionPolicy Bypass -Command "$env:UV_INSTALL_DIR='%TOOLS%'; $env:UV_NO_MODIFY_PATH='1'; irm https://astral.sh/uv/install.ps1 | iex"
:have_uv
if not exist "%TOOLS%\uv.exe" goto no_python
set "UV_PYTHON_INSTALL_DIR=%TOOLS%\python"
set "UV_CACHE_DIR=%TOOLS%\cache"
set "UV_NO_CONFIG=1"
"%TOOLS%\uv.exe" venv --seed --python 3.11 "%CD%\.demo_venv"
if errorlevel 1 goto no_python
if not exist "%VENVPY%" goto no_python
set "PYEXE="%VENVPY%""
goto run

:no_python
echo.
echo  Could not download Python automatically (no internet connection?).
echo  Install Python 3.11 from https://www.python.org/downloads/
echo  (tick "Add python.exe to PATH") and double-click run_demo.bat again.
pause
exit /b 1

rem ---- run the launcher -------------------------------------------------
:run
%PYEXE% -c "import sys; print('Using Python', sys.version.split()[0], '-', sys.executable)"
%PYEXE% scripts\demo_launcher.py %*
if errorlevel 1 goto failed
echo.
echo  Launcher finished. You can close this window (the demo keeps running in its own windows).
pause
exit /b 0

:failed
echo.
echo  The launcher stopped with an error (see the messages above).
pause
exit /b 1

rem ---- :try <python command...>  sets PYEXE to the first one that works --
:try
if defined PYEXE goto :eof
%* -c "%CHECK%" >nul 2>&1
if errorlevel 1 goto :eof
set "PYEXE=%*"
goto :eof
