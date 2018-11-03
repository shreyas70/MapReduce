#ifndef MAPPERSERVER_H
#define MAPPERSERVER_H

class MapperServer
{
    private:

    void process_map_request(int sock_desc);
    
    public:
    
    void start_mapper_server(int port_number);


};

#endif