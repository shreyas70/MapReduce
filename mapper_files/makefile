CC = g++
OBJ_DIR = bin
SRC_DIR = src
S_OBJ_FILE_NAMES = Utility.o Mapper.o dummy_master_server.o
C_OBJ_FILE_NAMES = Utility.o WordCount.o DummyMaster.o MapperNode.o mapper_node.o
S_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(S_OBJ_FILE_NAMES))
C_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(C_OBJ_FILE_NAMES))

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(CC) -c -g $< -o $@

all : dummy_master_server mapper_node

dummy_master_server : $(S_OBJ_FILES)
	$(CC) -o $@ $^ -pthread

mapper_node : $(C_OBJ_FILES)
	$(CC) -o $@ $^ -pthread

clean:
	-rm $(OBJ_DIR)/*.o
