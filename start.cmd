@echo off
chcp 65001 >nul
setlocal

rem 这个脚本只做三件事：找 Python、设好 PYTHONPATH、把流程交给 miniflow demo。
rem 真正的"生成示例 + 跑一次 + 打印下一步"都在 CLI 里，那样才能被自动化测试覆盖。

set "HERE=%~dp0"
if "%HERE:~-1%"=="\" set "HERE=%HERE:~0,-1%"
set "PYTHONPATH=%HERE%"

where python >nul 2>nul
if errorlevel 1 goto :nopython

python -c "import sys; sys.exit(0 if sys.version_info>=(3,9) else 1)" >nul 2>nul
if errorlevel 1 goto :oldpython

python -m miniflow demo
set "RC=%ERRORLEVEL%"
echo.
if not "%RC%"=="0" (
    echo 演示失败，退出码 %RC%。
    echo 先跑体检：python -m miniflow doctor
)
echo.
pause
exit /b %RC%

:nopython
echo [MF010] 找不到 python 命令。
echo.
echo 请安装 Python 3.9 或更高版本，安装时勾选 "Add Python to PATH"。
echo 下载：https://www.python.org/downloads/
echo.
pause
exit /b 5

:oldpython
echo [MF010] Python 版本太低，miniflow 需要 3.9 或更高。
python --version
echo.
pause
exit /b 5
