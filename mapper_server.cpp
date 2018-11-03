#include<iostream>
#include<unistd.h>
#include "MapperServer.h"

using namespace std;

int main()
{
    MapperServer ms;
    cout<<"\nStarting server at port 7000"<<endl;
    ms.start_mapper_server(7000);
    return 0;
}