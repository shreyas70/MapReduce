#ifndef REDUCERNODE_H
#define REDUCERNODE_H

#include<string>
#include<unordered_map>
#include "master_client.h"
#include "WordCount.h"
#include "InvertedIndex.h"
#include "fs_client.h"

class ReducerNode
{

    private:
    std::unordered_map<std::string, WordCountReducer*> wc_reducer_map;
    std::unordered_map<std::string, InvertedIndexReducer*> ii_reducer_map;

    public:
    void start_reducer_node(std::string master_ip_address, int master_port_number, FS_Client * fs);
    void word_count(MasterClient dm, std::string request_string, FS_Client * fs);
    void inverted_index(MasterClient dm, std::string request_string, FS_Client * fs);
};

#endif