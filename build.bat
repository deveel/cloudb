@echo off
cd /d "%~dp0/libs/nant/"
mono nant.exe -buildfile:../../default.build %1 %2 %3 %4 %5 %6