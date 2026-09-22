# Installs the AgentHub worker as a Windows service using sc.exe (V17 doc §21/§58).
#
# The service runs:  <PythonPath> <WorkerMain> --server <ServerUrl>
# where WorkerMain defaults to <InstallDir>\worker\main.py - that layout is
# what client/bootstrap/bootstrap.py --download-worker produces.
#
# SYNTAX NOTE: sc.exe argument syntax requires NO space before '=' and ONE
# space after it (binPath= "C:\..."). Quotes inside binPath must use the Win32
# CommandLineToArgvW escaping (\") - hence the `\`" sequences below.
#
# NSSM ALTERNATIVE (recommended when you want automatic restart + stdout
# logging without pywin32):
#   nssm install AgentHubWorker "<PythonPath>" "<WorkerMain>"
#   nssm set AgentHubWorker AppParameters --server <ServerUrl>
#   nssm set AgentHubWorker AppDirectory "<InstallDir>\worker"
#   nssm set AgentHubWorker AppStdout "<InstallDir>\logs\service.out.log"
#   nssm set AgentHubWorker AppStderr "<InstallDir>\logs\service.err.log"
#   nssm set AgentHubWorker AppRotateFiles 1
#   nssm start AgentHubWorker
#
# Run this script from an elevated (Administrator) PowerShell.

param(
    [Parameter(Mandatory = $true)][string]$PythonPath,
    [Parameter(Mandatory = $true)][string]$ServerUrl,
    [string]$InstallDir = "C:\ProgramData\AgentHub",
    [string]$ServiceName = "AgentHubWorker",
    [string]$WorkerMain = ""
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $PythonPath)) {
    Write-Error "Python interpreter not found: $PythonPath"
    exit 1
}

if (-not $WorkerMain) {
    $WorkerMain = Join-Path $InstallDir "worker\main.py"
}
if (-not (Test-Path $WorkerMain)) {
    Write-Error "Worker entrypoint not found: $WorkerMain (run bootstrap.py --download-worker first)"
    exit 1
}

# Win32 quoting: each path wrapped in \" so the SCM command line survives spaces.
$binPath = "\`"$PythonPath\`" \`"$WorkerMain\`" --server \`"$ServerUrl\`""

Write-Host "Creating service $ServiceName ..."
Write-Host "  binPath: $binPath"
sc.exe create $ServiceName binPath= "$binPath" start= auto DisplayName= "AgentHub Worker"
if ($LASTEXITCODE -ne 0) {
    Write-Error "sc.exe create failed (exit $LASTEXITCODE). Run this script from an elevated PowerShell."
    exit $LASTEXITCODE
}

sc.exe description $ServiceName "AgentHub device worker (persistent task executor)"
if ($LASTEXITCODE -ne 0) {
    Write-Warning "sc.exe description failed (exit $LASTEXITCODE); continuing."
}

Write-Host "Starting service $ServiceName ..."
sc.exe start $ServiceName
if ($LASTEXITCODE -ne 0) {
    Write-Error "sc.exe start failed (exit $LASTEXITCODE). Inspect with: sc.exe query $ServiceName"
    exit $LASTEXITCODE
}

Write-Host "Service $ServiceName installed and started."
