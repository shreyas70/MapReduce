#ifndef WORDCOUNT_H
#define WORDCOUNT_H

#include<string>
#include<mutex>
#include<unordered_map>
#include "fs_client.h"

typedef struct
{
    std::string data;
    int file_id;
}HeapData;
class Compare
{
    public:
    
    bool operator()(HeapData d1, const HeapData d2);
};

class WordCountMapper
{
    private:

    std::string job_id;
    int chunk_id;
    std::string file_path;
    int start_line;
    int no_of_lines;
    int no_of_reducers;

    // int client_port_number;

    public:

    WordCountMapper(std::string job_id, int chunk_id, std::string file_path, int start_line, int no_of_lines, int no_of_reducers);
    WordCountMapper(std::string request_string);
    std::string get_job_id();
    std::string get_chunk_id();
    std::string get_file_path();
    int get_no_of_reducers();
    std::string start_job(FS_Client * fs);

    // int get_client_port_number();
    // void set_client_port_number(int client_port_number);
};

class WordCountReducer
{
    private:

    std::string job_id;
    int no_of_files;
    std::unordered_map<int, int> category_to_files_map;
    std::mutex category_file_map_lock;
    std::mutex words_count_map_lock;
    void increment_files_in_category(int category);
    int get_file_count_in_category(int category);
    std::unordered_map<std::string, int> words_count;
    void update_word_count(std::string word, int count);

    // int client_port_number;

    public:
    void init(std::string request_string);
    std::string reduce(int category, std::string file_path, FS_Client * fs);
    // int get_client_port_number();
    // void set_client_port_number(int client_port_number);
};

#endif
