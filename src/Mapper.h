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
    void initiate_word_count_request(int job_id, int chunk_id, std::string file_path, int start_line, int no_of_lines, int no_of_reducers);
    void initiate_inverted_index_request(int job_id, int chunk_id, std::vector<std::string> file_paths, std::vector<int> start_lines, std::vector<int> no_of_lines, int no_of_reducers);
    std::string get_reply();
};

#endif