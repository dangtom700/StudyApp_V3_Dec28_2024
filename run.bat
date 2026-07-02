@echo off
setlocal
cd /d "%~dp0"

@REM Query the corpus with your prompt.
@REM   1. Setup the program once:   setup.bat   (builds the index)
@REM   2. Set the prompt:           edit PROMPT.txt and paste your query (essay-length is fine)
@REM   3. Query the result:         run.bat      (add --top N or --all to change how many show)

conda run -n StudyAssistant python src/query.py %* || exit /b 1
