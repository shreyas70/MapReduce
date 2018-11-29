#ifndef MAPPER_H
#define MAPPER_H

#include<string>
#include<vector>

class Mapper
{
    private:
    int mapper_socket;
    int heart_beat_socket;
    int initiate_heart_beats(std::string mapper_ip, int mapper_port);
    
    public:
    // Mapper(std::string mapper_ip, int mapper_port);
    // ~Mapper();
    void connect_to_mapper(std::string mapper_ip, int mapper_port);

    std::string receive_heart_beat();
    void reply_to_heart_beat(); 
    void initiate_word_count_request(std::string job_id, std::string file_path, off_t file_offset, size_t piece_size);
    void initiate_inverted_index_request(std::string job_id, std::vector<std::string> file_paths, std::vector<off_t> offsets, std::vector<size_t> piece_sizes);
    int get_available_slots();
};

#endif