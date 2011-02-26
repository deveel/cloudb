@echo off
IF "%~1" == "mono" GOTO MonoCompile ELSE GOTO DotNetCompile

:DotNetCompile
"%~dp0libs\nant\nant.exe" -buildfile:"%~dp0default.build" -D:package=%1 package
GOTO End

:MonoCompile
SHIFT
mono "%~dp0libs\nant\nant.exe" -buildfile:"%~dp0default.build" -D:package=%1 package
GOTO End

:End