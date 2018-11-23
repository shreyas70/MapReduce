#ifndef _UTILITIES_H_
#define _UTILITIES_H_

#include <string>

#define pb push_back

#define FAILURE        -1
#define SUCCESS        0

enum class Problem
{
    WORD_COUNT,
    INVERTED_INDEX
};

void         util_from_cursor_line_clear();
std::string  util_abs_path_get(std::string str);
void         util_ip_port_split(std::string addr, std::string &ip, int &port);
int          util_connection_make(std::string ip, uint16_t port);
void         util_data_read(int sock, char* read_buffer, int size_to_read);

#endif
