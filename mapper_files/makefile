CC = g++
OBJ_DIR = bin
SRC_DIR = src
CFLAGS = -Wall -std=c++1z
S_OBJ_FILE_NAMES = Utility.o WordCount.o InvertedIndex.o MapperServer.o mapper_server.o
C_OBJ_FILE_NAMES = Mapper.o dummy_master.o
S_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(S_OBJ_FILE_NAMES))
C_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(C_OBJ_FILE_NAMES))

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(CC) $(CFLAGS) -c -g $< -o $@

all : mapper_server dummy_master

mapper_server : $(S_OBJ_FILES)
	$(CC) $(CFLAGS) -o $@ $^ -pthread

dummy_master : $(C_OBJ_FILES)
	$(CC) $(CFLAGS) -o $@ $^

clean:
	-rm $(OBJ_DIR)/*.o
