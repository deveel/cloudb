@echo off
IF "%~1" == "mono" GOTO MonoCompile ELSE GOTO DotNetCompile

:DotNetCompile
"%~dp0nant\nant.exe" -buildfile:"%~dp0cloudb.build" %*
GOTO End

:MonoCompile
SHIFT
mono "%~dp0nant\nant.exe" -buildfile:"%~dp0cloudb.build" %*
echo %1
GOTO End

:End