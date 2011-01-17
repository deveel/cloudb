@echo off
cd /d "%~dp0/libs/nant/"
mono nant.exe -buildfile:../../default.build %*