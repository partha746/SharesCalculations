@echo off

set script_path="{Script_Path}\main.py"

if not "%1" == "max" start /MAX cmd /c %0 max & exit/b
python %script_path%

:MENU
SET INPUT=
SET /P INPUT=Press 'r' to Refresh OR 'q' to Quit : 

IF /I '%INPUT%'=='r' CALL :runScript
IF /I '%INPUT%'=='q' CALL :Quit
CLS

PAUSE > NUL
GOTO :MENU

:runScript
CLS
python %script_path%
GOTO :MENU

:Quit
EXIT
PAUSE