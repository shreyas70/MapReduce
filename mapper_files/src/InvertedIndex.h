#ifndef INVERTEDINDEX_H
#define INVERTEDINDEX_H

#include<string>
#include<vector>

typedef struct
{
    std::string file_path;
    off_t offset;
    size_t piece_size;
}FileInfo;

class InvertedIndex
{
    private:

    std::string job_id;
    std::vector<FileInfo> input_files;

    public:

    InvertedIndex(std::string job_id, std::vector<std::string> file_paths, std::vector<off_t> offsets, std::vector<size_t> piece_sizes);
    InvertedIndex(std::string request_string);
    void start_job();
};


#endif