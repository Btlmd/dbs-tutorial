#!/usr/bin/env bash

# gcc-11
sudo add-apt-repository ppa:ubuntu-toolchain-r/test
sudo apt-get install -y g++-11
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 50
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-11 50

# cmake 3.25
sudo apt purge --auto-remove cmake
wget https://github.com/Kitware/CMake/releases/download/v3.25.0-rc4/cmake-3.25.0-rc4-linux-x86_64.tar.gz
tar xf cmake-3.25.0-rc4-linux-x86_64.tar.gz
ln -s "$(pwd)"/cmake-3.25.0-rc4-linux-x86_64/bin/* /usr/local/bin

# JRE
sudo apt install default-jre