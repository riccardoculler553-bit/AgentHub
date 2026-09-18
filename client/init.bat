@echo off
chcp 65001 >nul
REM ============================================================
REM DeviceLink 子电脑客户端初始化脚本（配合当前 client 版本部署）
REM   1. 检测 Python 3.11+（优先 py -3.11，其次 PATH 中的 python）
REM   2. 创建客户端专用虚拟环境 client\.venv（不污染系统 Python）
REM   3. 安装客户端运行依赖：websockets>=13 / httpx
REM 说明：能力包（如 data.excel.preprocess）的 venv 与第三方依赖
REM       （polars/fastexcel/xlsxwriter）由 worker 首次执行时自动
REM       创建到 ~/.devicelink/work/envs，本脚本无需提前准备。
REM ============================================================
cd /d "%~dp0"

REM ---- pip 镜像：清华源失败自动回退官方源 ----
set "PIP_INDEX=https://pypi.tuna.tsinghua.edu.cn/simple"

echo [1/4] 检测 Python ...
set "PY="
py -3.11 -c "print(1)" >nul 2>&1
if not errorlevel 1 set "PY=py -3.11"
if not defined PY (
    python -c "print(1)" >nul 2>&1
    if not errorlevel 1 set "PY=python"
)
if not defined PY (
    echo [ERROR] 未找到 Python，请先安装 Python 3.11 或更高版本，安装时勾选 Add to PATH。
    pause
    exit /b 1
)
for /f "delims=" %%v in ('%PY% --version') do set "PYVER=%%v"
echo [ok] %PYVER%

%PY% -c "import sys; sys.exit(0 if sys.version_info >= (3,11) else 1)"
if errorlevel 1 (
    echo [ERROR] 需要 Python 3.11 及以上，当前为：%PYVER%
    pause
    exit /b 1
)

echo [2/4] 创建客户端虚拟环境 client\.venv ...
if exist ".venv\Scripts\python.exe" (
    echo [ok] 虚拟环境已存在，跳过
) else (
    %PY% -m venv .venv
    if errorlevel 1 (
        echo [ERROR] venv 创建失败
        pause
        exit /b 1
    )
)

echo [3/4] 安装客户端依赖（websockets httpx）...
".venv\Scripts\python.exe" -m pip install --disable-pip-version-check -i "%PIP_INDEX%" "websockets>=13" httpx
if errorlevel 1 (
    echo [warn] 镜像源安装失败，改用官方源重试 ...
    ".venv\Scripts\python.exe" -m pip install --disable-pip-version-check "websockets>=13" httpx
    if errorlevel 1 (
        echo [ERROR] 依赖安装失败，请检查子电脑网络或代理设置后重新运行本脚本。
        pause
        exit /b 1
    )
)

echo [4/4] 依赖自检 ...
".venv\Scripts\python.exe" -c "import websockets, httpx; print('websockets', websockets.__version__, '| httpx', httpx.__version__)"

echo.
echo ============ 初始化完成 ============
echo 首次启动（注册码在服务端管理页生成，一次性、5分钟内有效）：
echo   .venv\Scripts\python.exe main.py --server http://服务器IP:8000 --code DL-XXXX-XXXX --name "子电脑01"
echo 注册成功后身份保存在 C:\Users\你的用户名\.devicelink\device.json，
echo 之后启动不再需要 --code / --name：
echo   .venv\Scripts\python.exe main.py --server http://服务器IP:8000
echo.
echo 能力运行数据（输入副本/映射表/结果文件/运行参数/日志）均保存在：
echo   C:\Users\你的用户名\.devicelink\work\
echo ====================================
pause
