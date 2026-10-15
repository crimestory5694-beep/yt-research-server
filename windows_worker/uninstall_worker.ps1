# Removes the scheduled task. Add -RemoveData to also delete settings, logs and the private Python environment.
param([switch]$RemoveData)
Unregister-ScheduledTask -TaskName "YT Transcript Worker" -Confirm:$false -ErrorAction SilentlyContinue
Get-CimInstance Win32_Process -Filter "Name='pythonw.exe'" | Where-Object { $_.CommandLine -like '*worker_main.py*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -ErrorAction SilentlyContinue }
if ($RemoveData) { Remove-Item -Recurse -Force (Join-Path $env:APPDATA "yt-worker") -ErrorAction SilentlyContinue }
Write-Host "Worker removed."
