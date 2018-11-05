#include<iostream>
#include<unistd.h>
#include "Mapper.h"
#include "MapperServer.h"
int main()
{
    /* starting server in a process */
    if(fork()==0)
    {
        MapperServer ms;
        ms.start_mapper_server(7004);
        exit(0);
    }
    for(int i=0; i<5; i++)
    {
        if(fork()==0)
        {
            Mapper m;
            int sock = m.initiate_word_count_request("127.0.0.1", 7004, "JOB1", "/etc/fdsfd.txt", 200, 4000);
            close(sock);
            exit(0);
        }
    }
    return 0;
}