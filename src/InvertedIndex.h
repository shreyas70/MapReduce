#ifndef INVERTEDINDEX_H
#define INVERTEDINDEX_H

#include<string>
#include<vector>
#include<mutex>
#include<unordered_map>
#include<unordered_set>

#include "fs_client.h"

typedef struct
{
    std::string file_path;
    int start_line;
    int no_of_lines;
}FileInfo;

class InvertedIndexMapper
{
    private:

    std::string job_id;
    std::vector<FileInfo> input_files;
    int no_of_reducers;
    int chunk_id;
    
    // int client_port_number;

    public:

    InvertedIndexMapper(std::string job_id, int chunk_id, std::vector<std::string> file_paths, std::vector<int> start_lines, std::vector<int> no_of_lines, int no_of_reducers);
    InvertedIndexMapper(std::string request_string);
    std::string get_job_id();
    std::string get_chunk_id();
    int get_no_of_reducers();
    std::string start_job(FS_Client * fs);

    // int get_client_port_number();
    // void set_client_port_number(int client_port_number);
};

class InvertedIndexReducer
{
    private:

    std::string job_id;
    int no_of_files;
    std::unordered_map<int, int> category_to_files_map;
    std::mutex category_file_map_lock;
    std::mutex words_to_files_map_lock;
    void increment_files_in_category(int category);
    int get_file_count_in_category(int category);
    std::unordered_map<std::string, std::unordered_set<std::string>> words_to_files_map;
    void update_files_list(std::string word, std::vector<std::string> files_list);

    // int client_port_number;

    public:
    void init(std::string request_string);
    std::string reduce(int category, std::string file_path, FS_Client * fs);

    // int get_client_port_number();
    // void set_client_port_number(int client_port_number);
};


#endif
