CC = g++
OBJ_DIR = bin
SRC_DIR = src
S_OBJ_FILE_NAMES = Utility.o Mapper.o Reducer.o dummy_master_server.o
M_OBJ_FILE_NAMES = Utility.o WordCount.o InvertedIndex.o DummyMaster.o MapperNode.o mapper_node.o
R_OBJ_FILE_NAMES = Utility.o WordCount.o InvertedIndex.o DummyMaster.o ReducerNode.o reducer_node.o
S_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(S_OBJ_FILE_NAMES))
M_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(M_OBJ_FILE_NAMES))
R_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(R_OBJ_FILE_NAMES))

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(CC) -c -g $< -o $@

all : dummy_master_server mapper_node reducer_node

dummy_master_server : $(S_OBJ_FILES)
	$(CC) -o $@ $^ -pthread

mapper_node : $(M_OBJ_FILES)
	$(CC) -o $@ $^ -pthread

reducer_node : $(R_OBJ_FILES)
	$(CC) -o $@ $^ -pthread

clean:
	-rm $(OBJ_DIR)/*.o
