#ifndef INVERTEDINDEX_H
#define INVERTEDINDEX_H

#include<string>
#include<vector>
#include<mutex>
#include<unordered_map>
#include<unordered_set>

typedef struct
{
    std::string file_path;
    off_t offset;
    size_t piece_size;
}FileInfo;

class InvertedIndexMapper
{
    private:

    std::string job_id;
    std::vector<FileInfo> input_files;

    public:

    InvertedIndexMapper(std::string job_id, std::vector<std::string> file_paths, std::vector<off_t> offsets, std::vector<size_t> piece_sizes);
    InvertedIndexMapper(std::string request_string);
    std::string get_job_id();
    std::string start_job();
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

    public:
    void init(std::string request_string);
    std::string reduce(int category, std::string file_path);
};


#endif