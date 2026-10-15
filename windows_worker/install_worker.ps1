# Installs the local transcript worker for the CURRENT Windows user. Run from the extracted repo folder:
#   powershell -ExecutionPolicy Bypass -File windows_worker\install_worker.ps1
# Needs: Python 3.10+ ("py" launcher) already installed. Admin rights are NOT required.
$ErrorActionPreference = "Stop"
$Here = Split-Path -Parent $MyInvocation.MyCommand.Path
$Repo = Split-Path -Parent $Here
$Home2 = Join-Path $env:APPDATA "yt-worker"
New-Item -ItemType Directory -Force -Path $Home2 | Out-Null

Write-Host "1/4 Creating a private Python environment..."
$Venv = Join-Path $Home2 "venv"
if (-not (Test-Path (Join-Path $Venv "Scripts\python.exe"))) { & py -3 -m venv $Venv }
& (Join-Path $Venv "Scripts\python.exe") -m pip install --quiet --upgrade pip
& (Join-Path $Venv "Scripts\python.exe") -m pip install --quiet -r (Join-Path $Here "requirements-worker.txt")

Write-Host "2/4 Settings file..."
$EnvFile = Join-Path $Home2 "worker.env"
if (-not (Test-Path $EnvFile)) {
  $url = Read-Host "Your Render URL (https://....onrender.com)"
  $sec = Read-Host "Worker token (input hidden)" -AsSecureString
  $tok = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec))
  Set-Content -Path $EnvFile -Value @("RENDER_URL=$url", "WORKER_TOKEN=$tok") -Encoding UTF8
  # only this Windows user (and SYSTEM) may read the file
  icacls $EnvFile /inheritance:r /grant:r "$($env:USERNAME):(R,W)" "SYSTEM:(R,W)" | Out-Null
}

Write-Host "3/4 Registering the logon task (runs hidden, no admin)..."
$Py = Join-Path $Venv "Scripts\pythonw.exe"
$Main = Join-Path $Here "worker_main.py"
$Action = New-ScheduledTaskAction -Execute $Py -Argument "`"$Main`" --run" -WorkingDirectory $Repo
$Trigger = New-ScheduledTaskTrigger -AtLogOn -User "$env:USERDOMAIN\$env:USERNAME"
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable `
  -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 5) -MultipleInstances IgnoreNew
Register-ScheduledTask -TaskName "YT Transcript Worker" -Action $Action -Trigger $Trigger -Settings $Settings `
  -Description "Local YouTube transcript worker (outbound HTTPS only)" -Force | Out-Null

Write-Host "4/4 Done. It starts at your next sign-in. To start now: Start-ScheduledTask -TaskName 'YT Transcript Worker'"
Write-Host "Logs and state: $Home2"
