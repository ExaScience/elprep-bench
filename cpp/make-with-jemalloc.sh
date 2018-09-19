g++ -O3 -std=c++17 -lstdc++ -ltbb -pthread elprep.cpp -o elprep -L`jemalloc-config --libdir` -Wl,-rpath,`jemalloc-config --libdir` -ljemalloc `jemalloc-config --libs`
