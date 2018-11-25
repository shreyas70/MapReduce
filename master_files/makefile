CC = g++
CFLAGS = -g -Wall -std=c++1z
CLIENT_SRC = utilities.cpp master_server.cpp dummy_client.cpp
MASTER_SRC = utilities.cpp Mapper.cpp master_worker.cpp

all: dummy_client master

dummy_client: $(CLIENT_SRC)
	$(CC) $(CFLAGS) -o $@ $(CLIENT_SRC)

master: $(MASTER_SRC)
	$(CC) $(CFLAGS) -o $@ $(MASTER_SRC) -lpthread

clean:
	rm dummy_client master