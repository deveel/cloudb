@echo off
IF "%~1" == "mono" GOTO MonoCompile ELSE GOTO DotNetCompile

:DotNetCompile
"%~dp0..\..\tools\nant\nant.exe" -buildfile:"%~dp0kernel.build" %*
GOTO End

:MonoCompile
SHIFT
mono "%~dp0..\..\tools\nant\nant.exe" -buildfile:"%~dp0kernel.build" %*
echo %1
GOTO End

:End