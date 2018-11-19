#ifndef MAPPERSERVERUTIL_H
#define MAPPERSERVERUTIL_H

#include<string>

class MapperServerUtil
{
    public:
    int start_word_count_job(std::string file_path, off_t start_offset, size_t file_size);
};

#endif