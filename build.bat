@ECHO OFF

ECHO Clear up dist\...
IF EXIST dist (
    REM -
) ELSE (
    MKDIR dist
)
DEL /F /Q dist\*

@REM ECHO Calling pinliner...
@REM REM REM :: comment: please delete .pyc files before every call of the mdmautostktoolsap_bundle - this is implemented in my fork of the pinliner
@REM @REM python src-make\lib\pinliner\pinliner\pinliner.py src -o dist/mdmautostktoolsap_bundle.py --verbose
@REM python src-make\lib\pinliner\pinliner\pinliner.py src -o dist/mdmautostktoolsap_bundle.py
@REM if %ERRORLEVEL% NEQ 0 ( echo ERROR: Failure && pause && exit /b %errorlevel% )
@REM ECHO Done
PUSHD dist
COPY ..\src\fill_mddconvert.py .\mdmtool_fill_mddconvert.py
POPD

@REM ECHO Patching mdmautostktoolsap_bundle.py...
@REM ECHO # ... >> dist/mdmautostktoolsap_bundle.py
@REM ECHO # print('within mdmautostktoolsap_bundle') >> dist/mdmautostktoolsap_bundle.py
@REM REM REM :: no need for this, the root package is loaded automatically
@REM @REM ECHO # import mdmautostktoolsap_bundle >> dist/mdmautostktoolsap_bundle.py
@REM ECHO from src import run_universal >> dist/mdmautostktoolsap_bundle.py
@REM ECHO run_universal.main() >> dist/mdmautostktoolsap_bundle.py
@REM ECHO # print('out of mdmautostktoolsap_bundle') >> dist/mdmautostktoolsap_bundle.py

PUSHD dist
COPY ..\run.bat .\run_fill_mddconvert.bat
powershell -Command "(gc 'run_fill_mddconvert.bat' -encoding 'Default') -replace '(dist[/\\])?mdmtool_fill_mddconvert.py', 'mdmtool_fill_mddconvert.py' | Out-File -encoding 'Default' 'run_fill_mddconvert.bat'"
POPD


ECHO End

