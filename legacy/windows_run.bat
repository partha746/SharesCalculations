@echo off
if not "%1" == "max" start /MAX cmd /c %0 max & exit/b
python "E:\GoogleDrive\Work Files\#Scripts\SharesCalculations\main.py"

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
python "E:\GoogleDrive\Work Files\#Scripts\SharesCalculations\main.py"
GOTO :MENU

:Quit
EXIT
PAUSE