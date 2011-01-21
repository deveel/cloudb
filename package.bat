@echo off
IF "%~1" == "mono" GOTO MonoCompile ELSE GOTO DotNetCompile

:DotNetCompile
"%~dp0libs\nant\nant.exe" -buildfile:"%~dp0default.build" %* package
GOTO End

:MonoCompile
SHIFT
mono "%~dp0libs\nant\nant.exe" -buildfile:"%~dp0default.build" %* package
echo %1
GOTO End

:End