@echo off
"%~dp0src\build.bat" %* -D:libs="%~dp0libs" -D:conf="%~dp0conf"