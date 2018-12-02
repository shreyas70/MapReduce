#include <iostream>
#include <vector>
#include <string>

#include "utilities.h"
#include "MapperNode.h"
#include "fs_client.h"

using namespace std;

int main(int argc, char * argv[])
{
    if(argc < 3)
    {
        cout<<"\n\nEnter ip and port of master\n\n";
        exit(1);
    }
    vector<string> ip_split = split_string(argv[1], ':');
    string master_ip = ip_split[0];
    int master_port = stoi(ip_split[1]);
    
    string client_ip = argv[2];

    FS_Client * fs = new FS_Client(client_ip, FILE_SERVER_IP);

    // int client_port = stoi(argv[2]);

    // FileClient::init("127.0.0.1:8500");

    MapperNode mn ;
    mn.start_mapper_node(master_ip, master_port, fs);
    return 0;
}
