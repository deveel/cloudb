@echo off
cd /d "%~dp0/libs/nant/"
echo "building from %CD%"
nant.exe -buildfile:../../cloudb.build %1 %2 %3 %4 %5 %6