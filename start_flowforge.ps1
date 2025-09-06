# FlowForge Startup Script
Write-Host "🌊 Starting FlowForge ETL Platform..." -ForegroundColor Cyan
Write-Host ""

# Check if Python is installed
try {
    $pythonVersion = python --version 2>$null
    Write-Host "✅ Python found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ Python is not installed or not in PATH" -ForegroundColor Red
    Write-Host "Please install Python and try again" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

# Install requirements
Write-Host "📦 Installing required packages..." -ForegroundColor Yellow
try {
    pip install -r requirements.txt
    Write-Host "✅ Dependencies installed successfully" -ForegroundColor Green
} catch {
    Write-Host "❌ Failed to install dependencies" -ForegroundColor Red
    Write-Host "Please check your internet connection and Python environment" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

# Check if Streamlit is available
try {
    $streamlitVersion = streamlit --version 2>$null
    Write-Host "✅ Streamlit ready" -ForegroundColor Green
} catch {
    Write-Host "❌ Streamlit installation failed" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

# Start the application
Write-Host ""
Write-Host "🚀 Starting FlowForge..." -ForegroundColor Cyan
Write-Host ""
Write-Host "The application will open in your default browser" -ForegroundColor Yellow
Write-Host "Press Ctrl+C to stop the server" -ForegroundColor Yellow
Write-Host ""

streamlit run app.py