#ifndef MAPPERNODE_H
#define MAPPERNODE_H

#include<mutex>
#include<string>
#include "DummyMaster.h"

class MapperNode
{

    public:
    void start_mapper_node(std::string master_ip_address, int master_port_number);
    void word_count(DummyMaster dm, std::string request_string);
    void inverted_index(DummyMaster dm, std::string request_string);
};

#endif