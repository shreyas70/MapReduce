#ifndef MAPPERNODE_H
#define MAPPERNODE_H

#include<mutex>
#include<string>

#include "master_client.h"

class MapperNode
{
public:
    void start_mapper_node(std::string master_ip_address, int master_port_number);
    void word_count(MasterClient dm, std::string request_string);
    void inverted_index(MasterClient dm, std::string request_string);
};

#endif