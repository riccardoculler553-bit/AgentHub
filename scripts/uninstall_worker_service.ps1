# Removes the AgentHubWorker Windows service (V1.7 doc §21/§58).
#
# Run this script from an elevated (Administrator) PowerShell.
# Worker files under the install dir are left in place on purpose - a later
# update or reinstall can reuse them.

param(
    [string]$ServiceName = "AgentHubWorker"
)

$ErrorActionPreference = "Stop"

Write-Host "Stopping service $ServiceName (ignored if not running) ..."
sc.exe stop $ServiceName | Out-Null

# sc.exe stop is asynchronous; give the SCM a moment before deleting.
Start-Sleep -Seconds 2

Write-Host "Deleting service $ServiceName ..."
sc.exe delete $ServiceName
if ($LASTEXITCODE -ne 0) {
    Write-Error "sc.exe delete failed (exit $LASTEXITCODE). Check: sc.exe query $ServiceName"
    exit $LASTEXITCODE
}

Write-Host "Service $ServiceName removed."
