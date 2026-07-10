# Variables for compiler and flags
CXX = g++
CXXFLAGS = -O2 -DNDEBUG -Iinclude
LIBS = -lws2_32

# Find all .cpp files in the src directory
SRCS = src/main.cpp src/ThreadSafeQueue.cpp

# Target to build the executable
main: $(SRCS)
	$(CXX) $(SRCS) $(CXXFLAGS) -o main.exe $(LIBS)

.PHONY: test-claims
test-claims:
	python tests/e2e_claims.py
