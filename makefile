CC = g++ -std=c++11
OBJ_DIR = bin
SRC_DIR = src
F_OBJ_FILE_NAMES = utilities.o fs_server.o 
C_OBJ_FILE_NAMES = utilities.o master_client.o dummy_client.o fs_client.o
S_OBJ_FILE_NAMES = utilities.o Mapper.o Reducer.o master_server.o fs_client.o
M_OBJ_FILE_NAMES = utilities.o WordCount.o InvertedIndex.o master_client.o MapperNode.o mapper_node.o fs_client.o
R_OBJ_FILE_NAMES = utilities.o WordCount.o InvertedIndex.o master_client.o ReducerNode.o reducer_node.o fs_client.o
C_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(C_OBJ_FILE_NAMES))
F_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(F_OBJ_FILE_NAMES))
S_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(S_OBJ_FILE_NAMES))
M_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(M_OBJ_FILE_NAMES))
R_OBJ_FILES = $(patsubst %,$(OBJ_DIR)/%,$(R_OBJ_FILE_NAMES))

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(CC) -c -g $< -o $@

all : mapper_node master_server dummy_client reducer_node fs_server


dummy_client : $(C_OBJ_FILES)
	$(CC) -o $@ $^ 

master_server : $(S_OBJ_FILES)
	$(CC) -o $@ $^ -pthread

mapper_node : $(M_OBJ_FILES)
	$(CC) -o $@ $^ -pthread

reducer_node : $(R_OBJ_FILES)
	$(CC) -o $@ $^ -pthread

fs_server : $(F_OBJ_FILES)
	$(CC) -o $@ $^ -pthread

clean:
	-rm $(OBJ_DIR)/*.o mapper_node master_server dummy_client reducer_node fs_server
