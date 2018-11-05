#include<iostream>
#include "MapperServer.h"

int main()
{
    MapperServer ms;
    ms.start_mapper_server(7000);
    return 0;
}