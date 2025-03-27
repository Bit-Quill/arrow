# Assumes vcpkg is installed
# Install required dependencies 
vcpkg install

# Make build directory
mkdir build

cmake -S . `
    -B build `
    -A x64 `
    -D CMAKE_BUILD_TYPE=Release `
    -D CMAKE_INSTALL_PREFIX=vcpkg_installed\x64-windows\bin `
    -D VCPKG_TARGET_TRIPLET=x64-windows `
    -D CMAKE_TOOLCHAIN_FILE=c:/vcpkg/scripts/buildsystems/vcpkg.cmake

