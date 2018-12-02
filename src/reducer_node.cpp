#include <iostream>
#include <vector>
#include <string>
#include "utilities.h"
#include "ReducerNode.h"
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

    // int client_port_number = stoi(argv[2]);
    ReducerNode rn;
    // FileClient::init("127.0.0.1:7500");

    string client_ip_address = argv[2];

    FS_Client * fs = new FS_Client(client_ip_address, FILE_SERVER_IP);

    rn.start_reducer_node(master_ip, master_port, fs);
    return 0;
}
