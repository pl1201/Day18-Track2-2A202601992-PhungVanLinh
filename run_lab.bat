@echo off
REM Runs `make <target>` inside the WSL native-fs copy of the lab (~/day18lab).
REM Usage: run_lab.bat run-all
REM        run_lab.bat lab
REM        run_lab.bat smoke
if "%~1"=="" (
  echo Usage: run_lab.bat ^<make-target^>
  echo Example: run_lab.bat run-all
  exit /b 1
)
wsl.exe -d Ubuntu -- bash -lc "cd ~/day18lab && make %~1"
