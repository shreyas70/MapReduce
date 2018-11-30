#include<iostream>
#include<vector>
#include<string>
#include "MapperServer.h"
using namespace std;

int main(int argc, char * argv[])
{
    if(argc<2)
    {
        cout<<"\nEnter mapper ip address\n";
        exit(1);
    }
    vector<string> reducer_ips;
    reducer_ips.push_back("127.0.0.1:9000");
    reducer_ips.push_back("127.0.0.1:9001");
    reducer_ips.push_back("127.0.0.1:9002");
    reducer_ips.push_back("127.0.0.1:9003");
    MapperServer ms;
    string mapper_ip = argv[1];
    ms.set_mapper_ip(mapper_ip);
    ms.set_reducer_info(reducer_ips);
    ms.start_mapper_server();
    return 0;
}