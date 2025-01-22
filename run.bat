@ECHO OFF
SETLOCAL enabledelayedexpansion


@REM :: insert your files here
SET "MAP_FILE=path\to\spss_map_savfile.xlsx"



python dist/mdmtool_fill_mddconvert.py --map "%MAP_FILE%"
if %ERRORLEVEL% NEQ 0 ( echo ERROR: Failure && pause && goto CLEANUP && exit /b %errorlevel% )



ECHO done!
exit /b %errorlevel%

