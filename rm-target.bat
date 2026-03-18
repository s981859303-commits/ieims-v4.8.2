@echo off 
chcp 65001 > nul
:: 定义要扫描的根目录
set "ROOT_DIR=."

:: 检查根目录是否存在（仅保留核心校验）
if not exist "%ROOT_DIR%" (
    echo 错误：目录 %ROOT_DIR% 不存在！
    pause
    exit
)

echo 正在删除 %ROOT_DIR% 下所有 target 目录...
:: 递归查找并删除所有 target 目录
for /f "delims=" %%i in ('dir /s /b /ad "%ROOT_DIR%\target" 2^>nul') do (
    rd /s /q "%%i"
    echo 已删除：%%i
)

echo.
echo 操作完成！
pause