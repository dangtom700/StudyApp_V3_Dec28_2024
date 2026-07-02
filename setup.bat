@echo off
setlocal
cd /d "%~dp0"

echo Building C++ retrieval engine...
cmake -B build -DCMAKE_PREFIX_PATH=C:/msys64/ucrt64 -G "MinGW Makefiles" . || exit /b 1
cmake --build build || exit /b 1

echo Setting up NLTK corpora...
conda run -n StudyAssistant python src/setup_nltk.py || exit /b 1

echo Running StudyApp V3 build pipeline...
conda run -n StudyAssistant python src/pipeline.py %* || exit /b 1

echo Done.
