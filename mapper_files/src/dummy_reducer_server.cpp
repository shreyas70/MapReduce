#include <iostream>
#include <string>
#include <vector>
#include "DummyReducerServer.h"
#include "Utility.h"

using namespace std;

int main(int argc, char * argv[])
{
    if(argc < 2)
    {
        cout<<"\n Please provide ip address of reducer\n";
    }
    string ip_string = argv[1];
    vector<string> ip_split = split_string(ip_string, ':');
    string ip_address = ip_split[0];
    int port_number = stoi(ip_split[1]);
    DummyReducerServer ds = DummyReducerServer(ip_address, port_number);
    ds.start_server();
    return 0;
}