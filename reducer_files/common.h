/* Name:    Bhavi Dhingra
 * RollNo.: 2018201058
 */

#ifndef _COMMON_H_
#define _COMMON_H_

#define pb push_back

#define FAILURE        -1
#define SUCCESS        0

#include <string>

void         from_cursor_line_clear();
std::string  abs_path_get(std::string str);
void         ip_and_port_split(std::string addr, std::string &ip, int &port);

#endif
