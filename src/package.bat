@echo off
IF "%~1" == "mono" GOTO MonoCompile ELSE GOTO DotNetCompile

:DotNetCompile
"%~dp0nant\nant.exe" -buildfile:"%~dp0package.build" -D:libs="%~dp0libs" -D:package=%1 package
GOTO End

:MonoCompile
SHIFT
mono "%~dp0nant\nant.exe" -buildfile:"%~dp0package.build" -D:libs="%~dp0libs" -D:package=%1 package
GOTO End

:End