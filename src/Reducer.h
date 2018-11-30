#ifndef REDUCER_H
#define REDUCER_H

#include<string>
#include<vector>

class Reducer
{
    private:
    int reducer_socket;

    public:
    void init(int reducer_socket);
    int get_socket();
    void word_count_request(int job_id, int category, std::string file_path, int no_of_mappers);
    void inverted_index_request(int job_id, int category, std::string file_path, int no_of_mappers);
    std::string get_reply();
};

#endif