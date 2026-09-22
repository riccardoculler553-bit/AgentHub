# ============================================================================
# AgentHub bootstrap.exe builder (V1.7, Windows)
#
# Builds a ONE-FILE, fully self-contained bootstrap.exe with PyInstaller.
# The target machine then needs NO Python at all - just run:
#
#     bootstrap.exe --server http://<server>:8000 --token DL-XXXX-XXXX `
#                   [--device-name NAME] [--download-worker]
#
# Usage (on the admin/dev machine, once):
#   .\scripts\build_bootstrap_exe.ps1              # -> dist\bootstrap.exe
#   .\scripts\build_bootstrap_exe.ps1 -DistOnly    # skip pip install step
#
# The exe still honors the same switches as bootstrap.py (argparse is bundled).
# ============================================================================

param(
    [switch]$DistOnly
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot          # repo root (d:\Websocket)
$bootstrapDir = Join-Path $root "client\bootstrap"

Write-Host "[build] repo root: $root"

if (-not $DistOnly) {
    Write-Host "[build] ensuring PyInstaller ..."
    python -m pip install --disable-pip-version-check --quiet pyinstaller
}

Write-Host "[build] cleaning dist/build ..."
Remove-Item (Join-Path $bootstrapDir "dist") -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item (Join-Path $bootstrapDir "build") -Recurse -Force -ErrorAction SilentlyContinue

Write-Host "[build] running PyInstaller (one-file, console) ..."
Push-Location $bootstrapDir
try {
    python -m PyInstaller --onefile --console --clean --name bootstrap `
        --hidden-import urllib.request --hidden-import urllib.error `
        bootstrap.py
    if ($LASTEXITCODE -ne 0) { throw "PyInstaller failed with exit code $LASTEXITCODE" }
}
finally {
    Pop-Location
}

$exe = Join-Path $bootstrapDir "dist\bootstrap.exe"
if (Test-Path $exe) {
    Write-Host "[build] OK: $exe ($([math]::Round((Get-Item $exe).Length / 1MB, 1)) MB)"
    Write-Host "[build] ship this single file to fresh machines:"
    Write-Host '         bootstrap.exe --server http://<server>:8000 --token DL-XXXX-XXXX --download-worker'
} else {
    throw "bootstrap.exe was not produced - check the PyInstaller output above"
}
