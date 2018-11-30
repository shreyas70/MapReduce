#ifndef REDUCERNODE_H
#define REDUCERNODE_H

#include<string>
#include<unordered_map>
#include "DummyMaster.h"
#include "WordCount.h"
#include "InvertedIndex.h"

class ReducerNode
{

    private:
    std::unordered_map<std::string, WordCountReducer*> wc_reducer_map;
    std::unordered_map<std::string, InvertedIndexReducer*> ii_reducer_map;

    public:
    void start_reducer_node(std::string master_ip_address, int master_port_number);
    void word_count(DummyMaster dm, std::string request_string);
    void inverted_index(DummyMaster dm, std::string request_string);
};

#endif