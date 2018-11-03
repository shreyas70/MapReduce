#include<iostream>
#include<unistd.h>
#include "Mapper.h"
#include "MapperServer.h"
int main()
{
    MapperServer ms;
    ms.start_mapper_server(7000);
    Mapper m;
    int sock = m.initiate_word_count_request("127.0.0.1", 7000, "JOB1", "/etc/fdsfd.txt", 200, 4000);
    close(sock);
    return 0;
}