#ifndef MAPPER_H
#define MAPPER_H

#include<string>

typedef struct
{
    int mapper_socket;
    int heart_beat_socket;
}MapperConnection;

class Mapper
{
    private:
    int initiate_heart_beats(std::string mapper_ip, int mapper_port);
    
    public:
    MapperConnection connect_to_mapper(std::string mapper_ip, int mapper_port);
    int initiate_word_count_request(int sock, std::string job_id, std::string file_path, off_t file_offset, size_t piece_size);
    int get_available_slots(int sock);
};

#endif