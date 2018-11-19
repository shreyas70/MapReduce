#ifndef WORDCOUNT_H
#define WORDCOUNT_H

#include<string>

class WordCount
{
    private:

    std::string job_id;
    std::string file_path;
    off_t offset;
    size_t piece_size;

    public:

    WordCount(std::string job_id, std::string file_path, off_t offset, size_t piece_size);
    WordCount(std::string request_string);
    std::string get_job_id();
    std::string get_file_path();
    off_t get_offset();
    size_t get_piece_size();


};

#endif