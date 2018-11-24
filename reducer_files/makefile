CC = g++
CFLAGS = -g -Wall -std=c++1z
MAPPER_SRC = utilities.cpp reducer_server.cpp dummy_mapper.cpp
REDUCER_SRC = utilities.cpp reducer_worker.cpp

all: dummy_mapper reducer

dummy_mapper: $(MAPPER_SRC)
	$(CC) $(CFLAGS) -o $@ $(MAPPER_SRC)

reducer: $(REDUCER_SRC)
	$(CC) $(CFLAGS) -o $@ $(REDUCER_SRC) -lpthread

clean:
	rm dummy_mapper reducer
