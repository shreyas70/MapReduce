#include<iostream>
#include<vector>
#include<string>
#include "MapperServer.h"
using namespace std;

int main(int argc, char * argv[])
{
    if(argc<2)
    {
        cout<<"\nAre you insane?\n";
        exit(1);
    }
    vector<string> reducer_ips;
    reducer_ips.push_back("10.1.1.1:1000");
    reducer_ips.push_back("10.1.1.2:1000");
    reducer_ips.push_back("10.1.1.3:1000");
    reducer_ips.push_back("10.1.1.4:1000");
    MapperServer ms;
    string mapper_ip = argv[1];
    ms.set_mapper_ip(mapper_ip);
    ms.set_reducer_info(reducer_ips);
    ms.start_mapper_server();
    return 0;
}