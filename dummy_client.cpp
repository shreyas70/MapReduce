#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <cmath>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <vector>
#include <sys/ioctl.h>
#include <stdio.h> 
#include <sys/socket.h> 
#include <netinet/in.h> 
#include <arpa/inet.h>
#include <string>
#include "fs_client.h"
using namespace std;


int main(int argc, char* argv[])
{
    int start_line = 5;
    int line_count = 100;
    FS_Client fs_client(argv[1], argv[2]);
    size_t cnt = 0;
    cnt = fs_client.get_lines_count("/home/kaushik/Coursework/OS/input.txt");
    cout << cnt << endl;
    fs_client.get_chunk("input.txt", "output.txt", start_line, line_count);
    int upload_state = fs_client.upload_file("file.cpp");
    fs_client.append_file("input.txt", "output.txt");
    fs_client.append_file("input2.txt", "output.txt");
    fs_client.append_file("input3.txt", "output.txt");
    fs_client.append_file("input4.txt", "output.txt");
    fs_client.append_file("input.txt", "output.txt");
    fs_client.append_file("input2.txt", "output.txt");
    fs_client.append_file("input3.txt", "output.txt");
    fs_client.append_file("input4.txt", "output.txt");

    fs_client.remove_file("input5.txt");

    while(1);
    return 0;
}