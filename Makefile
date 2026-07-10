# Variables for compiler and flags
CXX = g++
CXXFLAGS = -O2 -DNDEBUG -Iinclude -Ivcpkg_installed/x64-mingw-dynamic/include
LIBS = -lws2_32

# Find all .cpp files in the src directory
SRCS = src/main.cpp src/ThreadSafeQueue.cpp

# Target to build the executable
main: $(SRCS)
	$(CXX) $(SRCS) $(CXXFLAGS) -o main.exe $(LIBS)

.PHONY: learning-map
learning-map:
	$(CXX) src/learning_flat_map.cpp $(CXXFLAGS) -o learning_flat_map.exe

.PHONY: test-claims
test-claims:
	python tests/e2e_claims.py
