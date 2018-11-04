#ifndef MAPPER_H
#define MAPPER_H

#include<string>

class Mapper
{
    
    public:
    int initiate_word_count_request(std::string mapper_ip, int mapper_port, std::string job_id, std::string file_path, off_t file_offset, size_t piece_size);

};

#endif