#ifndef MAPPER_H
#define MAPPER_H

#include<string>
#include<vector>

class Mapper
{
    private:
    int mapper_socket;
    
    public: 
    void init(int mapper_socket);
    int get_socket();
    void initiate_word_count_request(std::string job_id, int chunk_id, std::string file_path, off_t file_offset, size_t piece_size, int no_of_reducers);
    void initiate_inverted_index_request(std::string job_id, int chunk_id, std::vector<std::string> file_paths, std::vector<off_t> offsets, std::vector<size_t> piece_sizes, int no_of_reducers);
    std::string get_reply();
};

#endif