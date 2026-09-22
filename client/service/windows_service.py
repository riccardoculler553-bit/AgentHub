"""Windows Service wrapper for the AgentHub worker (V1.7 doc §21, §58).

Two install paths:
- pywin32 (preferred): a real Windows service named AgentHubWorker that runs
  `python client/main.py --server <url>` with the current interpreter.
- scripts/install_worker_service.ps1: sc.exe-based fallback (NSSM/WinSW notes
  in the script header) when pywin32 is not installed.

The module imports cleanly WITHOUT pywin32 (all win32 imports happen inside
functions) so CI machines and the worker.service alias can load it safely.

Usage (with pywin32 installed):
    python windows_service.py install
    python windows_service.py start
    python windows_service.py remove
    (no arguments: dispatched by the Windows service controller)
"""

import os
import subprocess
import sys
from pathlib import Path

SERVICE_NAME = "AgentHubWorker"
SERVICE_DISPLAY_NAME = "AgentHub Worker"
SERVICE_DESCRIPTION = "AgentHub device worker (persistent task executor)"

# client/main.py lives one level above this package (client/service/ -> client/).
CLIENT_MAIN = Path(__file__).resolve().parents[1] / "main.py"

_FALLBACK_INSTRUCTIONS = f"""\
pywin32 is not installed in this interpreter, so the service cannot be
installed from Python. Choose one of:

1) pip install pywin32
   python {Path(__file__).resolve()}

2) Script-based install (sc.exe; NSSM/WinSW alternatives are documented in
   the script header):
   powershell -ExecutionPolicy Bypass -File scripts/install_worker_service.ps1 \
       -PythonPath "{sys.executable}" -ServerUrl "http://<host>:8000"
"""


def _worker_command(server_url: str | None) -> list[str]:
    """Command line the service runs: the current interpreter on client/main.py."""
    command = [sys.executable, str(CLIENT_MAIN)]
    if server_url:
        command += ["--server", server_url]
    return command


def _build_service_class():
    """Define the service class with pywin32 imports resolved lazily, so the
    module itself imports cleanly without pywin32."""
    import servicemanager
    import win32event
    import win32service
    import win32serviceutil

    class AgentHubWorkerService(win32serviceutil.ServiceFramework):
        _svc_name_ = SERVICE_NAME
        _svc_display_name_ = SERVICE_DISPLAY_NAME
        _svc_description_ = SERVICE_DESCRIPTION

        def __init__(self, args):
            win32serviceutil.ServiceFramework.__init__(self, args)
            self.stop_event = win32event.CreateEvent(None, 0, 0, None)
            self.child: subprocess.Popen | None = None

        def SvcStop(self):
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            win32event.SetEvent(self.stop_event)

        def SvcDoRun(self):
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, ""),
            )
            # Worker entrypoint: python client/main.py --server <url> (§21).
            # The URL travels via AGENTHUB_SERVER_URL (set by the installer or
            # the machine environment).
            command = _worker_command(os.getenv("AGENTHUB_SERVER_URL", ""))
            self.child = subprocess.Popen(command, cwd=str(CLIENT_MAIN.parent))
            # Stay alive until the SCM asks us to stop or the worker exits.
            while True:
                if win32event.WaitForSingleObject(self.stop_event, 5000) == win32event.WAIT_OBJECT_0:
                    break
                if self.child.poll() is not None:
                    break
            if self.child.poll() is None:
                self.child.terminate()
                self.child.wait(timeout=15)

    return AgentHubWorkerService


def service_main() -> int:
    """Install/start the AgentHubWorker service via pywin32; without pywin32,
    print the script-based fallback instructions and exit 2 (§58)."""
    try:
        import servicemanager
        import win32serviceutil
    except ImportError:
        print(_FALLBACK_INSTRUCTIONS, file=sys.stderr)
        return 2

    service_cls = _build_service_class()

    if len(sys.argv) == 1:
        # Started by the Windows service controller: hand control to the SCM.
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(service_cls)
        servicemanager.StartServiceCtrlDispatcher()
        return 0

    # CLI mode: python windows_service.py install|start|stop|remove|...
    win32serviceutil.HandleCommandLine(service_cls)
    return 0


if __name__ == "__main__":
    sys.exit(service_main())
