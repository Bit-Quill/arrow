# FligthSQL ODBC Driver

## Overview 

ODBC driver for Apache Arrow Flight SQL.

## License

This project is licensed under the Apache-2.0 License.

## Pre-requisites 
1. Visual Studio 2022 needs to be installed.
  - Latest version is recommended
2. Install VCPKG and set `VCPKG_ROOT`.
  - Latest version is recommended.
  - To update go to the VCPKG_ROOT folder and execute `git pull` and  `bootstrap-vcpkg.bat` 
3. CMAKE version 3.25.1

## Driver Building (Windows)

Open Visual Studio Developer PowerShell or Powershell and run `./build_win32.bat` or `./build_win64.bat` to build the 32-bit and 64-bit driver correspondingly. The .bat file will build and install the dependencies and the driver.

### Build Troubleshoot Guide

| Error | Root Cause | Fix|
|-------|------------|----|
| CUSTOMBUILD : CMake error : The source directory "C:/<repo_folder>/build/flight_sql/ApacheArrow-prefix/src/ApacheArrow" does not appear to contain CMakeLists.txt. [C:\Dev\forked-flightsql-odbc\build\flight_sql\ApacheArrow.vcxproj] | current CMAKE version 3.28 has issues in build the project | downgrade to the 3.25 version |
| error in the `builtin-baseline` at `vcpkg.json` or issues to build and install the dependencies | vcpkg outdated | updated to the latest vcpkg version |
| vcpkg crashes error | vcpkg was not properly updated | make sure to execute `bootstrap-vcpkg.bat` in the `VCPKG_ROOT` |
