CC = g++
OBJ_DIR = bin
SRC_DIR = src
S_OBJ_FILE_NAMES = Utility.o WordCount.o InvertedIndex.o DummyReducerClient.o MapperServer.o mapper_server.o
C_OBJ_FILE_NAMES = Mapper.o dummy_master.o
X_OBJ_FILE_NAMES = Utility.o DummyReducerServer.o dummy_reducer_server.o
S_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(S_OBJ_FILE_NAMES))
C_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(C_OBJ_FILE_NAMES))
X_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(X_OBJ_FILE_NAMES))

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(CC) -c -g $< -o $@

all : mapper_server dummy_master dummy_reducer_server

mapper_server : $(S_OBJ_FILES)
	$(CC) -o $@ $^ -pthread

dummy_master : $(C_OBJ_FILES)
	$(CC) -o $@ $^

dummy_reducer_server : $(X_OBJ_FILES)
	$(CC) -o $@ $^ -pthread

clean:
	-rm $(OBJ_DIR)/*.o
