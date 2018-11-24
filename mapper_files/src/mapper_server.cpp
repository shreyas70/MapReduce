#include<iostream>
#include<vector>
#include<string>
#include "MapperServer.h"
using namespace std;

int main()
{
    vector<string> reducer_ips;
    reducer_ips.push_back("10.1.1.1:1000");
    reducer_ips.push_back("10.1.1.2:1000");
    reducer_ips.push_back("10.1.1.3:1000");
    reducer_ips.push_back("10.1.1.4:1000");
    MapperServer ms;
    ms.set_mapper_ip("127.0.0.1:7000");
    ms.set_reducer_info(reducer_ips);
    ms.start_mapper_server();
    return 0;
}