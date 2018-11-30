#include <iostream>
#include <vector>
#include <string>
#include "utilities.h"
#include "ReducerNode.h"

using namespace std;

int main(int argc, char * argv[])
{
    if(argc < 2)
    {
        cout<<"\n\nEnter ip and port of master\n\n";
        exit(1);
    }
    vector<string> ip_split = split_string(argv[1], ':');
    string master_ip = ip_split[0];
    int master_port = stoi(ip_split[1]);
    ReducerNode rn;
    rn.start_reducer_node(master_ip, master_port);
    return 0;
}
