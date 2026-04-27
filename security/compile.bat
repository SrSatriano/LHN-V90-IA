@echo off
setlocal EnableExtensions
cd /d "%~dp0"

echo [LHN] Optional C++ shield build / Build opcional do modulo C++.

if exist "C:\msys64\mingw64\bin\g++.exe" (
  set "PATH=C:\msys64\mingw64\bin;%PATH%"
)

where g++.exe >nul 2>&1
if errorlevel 1 (
  echo [ERROR] g++.exe was not found. Install MinGW/MSYS2 or add g++ to PATH.
  echo [ERRO] g++.exe nao encontrado. Instale MinGW/MSYS2 ou adicione ao PATH.
  exit /b 1
)

if not exist "lhn_shield.cpp" (
  echo [ERROR] lhn_shield.cpp was not found in the security folder.
  echo [ERRO] lhn_shield.cpp nao foi encontrado na pasta security.
  exit /b 1
)

g++.exe -shared -o lhn_shield.dll lhn_shield.cpp -Wl,--out-implib,liblhn_shield.a
if errorlevel 1 (
  echo [ERROR] Build failed.
  echo [ERRO] Falha na compilacao.
  exit /b 1
)

echo [OK] Build completed: lhn_shield.dll
echo [OK] Compilacao concluida: lhn_shield.dll
endlocal
