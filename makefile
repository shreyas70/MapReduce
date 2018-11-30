CC = g++ -std=c++11
OBJ_DIR = bin
SRC_DIR = src
S_OBJ_FILE_NAMES = utilities.o Mapper.o Reducer.o dummy_master_server.o
M_OBJ_FILE_NAMES = utilities.o WordCount.o InvertedIndex.o master_client.o MapperNode.o mapper_node.o
R_OBJ_FILE_NAMES = utilities.o WordCount.o InvertedIndex.o DummyMaster.o ReducerNode.o reducer_node.o
S_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(S_OBJ_FILE_NAMES))
M_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(M_OBJ_FILE_NAMES))
R_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(R_OBJ_FILE_NAMES))

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(CC) -c -g $< -o $@

all : mapper_node 

dummy_master_server : $(S_OBJ_FILES)
	$(CC) -o $@ $^ -pthread

mapper_node : $(M_OBJ_FILES)
	$(CC) -o $@ $^ -pthread

reducer_node : $(R_OBJ_FILES)
	$(CC) -o $@ $^ -pthread

clean:
	-rm $(OBJ_DIR)/*.o
