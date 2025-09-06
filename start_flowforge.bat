@echo off
echo Starting FlowForge ETL Platform...
echo.

REM Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Python is not installed or not in PATH
    echo Please install Python and try again
    pause
    exit /b 1
)

REM Check if pip is available
pip --version >nul 2>&1
if %errorlevel% neq 0 (
    echo pip is not available
    echo Please ensure pip is installed
    pause
    exit /b 1
)

REM Install requirements
echo Installing required packages...
pip install -r requirements.txt

REM Check if Streamlit is installed
streamlit --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Streamlit installation failed
    echo Please check your Python environment
    pause
    exit /b 1
)

REM Start the application
echo.
echo Starting FlowForge...
echo.
echo The application will open in your default browser
echo Press Ctrl+C to stop the server
echo.

streamlit run app.py

pause