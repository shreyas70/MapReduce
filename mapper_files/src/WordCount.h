#ifndef WORDCOUNT_H
#define WORDCOUNT_H

#include<string>
#include<mutex>
#include<unordered_map>

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
    off_t offset;
    size_t piece_size;

    public:

    WordCountMapper(std::string job_id, int chunk_id, std::string file_path, off_t offset, size_t piece_size);
    WordCountMapper(std::string request_string);
    std::string get_job_id();
    std::string get_file_path();
    off_t get_offset();
    size_t get_piece_size();
    std::string start_job();
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

    public:
    void init(std::string request_string);
    std::string reduce(int category, std::string file_path);
};

#endif