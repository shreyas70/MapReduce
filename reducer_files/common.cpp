#include "common.h"
#include "includes.h"

using namespace std;

extern string  working_dir;

void from_cursor_line_clear()
{
    cout << "\e[0K";
    cout.flush();
}

string abs_path_get(string str)
{
    if(str[0] == '/')
        return str;

    char *str_buf = new char[str.length() + 1];
    strncpy(str_buf, str.c_str(), str.length());
    str_buf[str.length()] = '\0';

    string ret_path = working_dir, prev_tok = working_dir;
    if(str_buf[0] == '/')
        ret_path = "/";

    char *p_str = strtok(str_buf, "/");
    while(p_str)
    {
        string tok(p_str);
        if(tok == ".")
        {
            prev_tok = tok;
            p_str = strtok (NULL, "/");
        }
        else if(tok == "..")
        {
            ret_path.erase(ret_path.length() - 1);
            size_t fwd_slash_pos = ret_path.find_last_of("/");
            ret_path = ret_path.substr(0, fwd_slash_pos + 1);
            prev_tok = tok;
            p_str = strtok (NULL, "/");
        }
        else if (tok == "~")
        {
            ret_path = getenv("HOME");
            ret_path = ret_path + "/";
            prev_tok = tok;
            p_str = strtok (NULL, "/");
        }
        else if(tok == "")
        {
            if(!prev_tok.empty())
                ret_path = "/";
            p_str = strtok (NULL, "/");
        }
        else
        {
            p_str = strtok (NULL, "/");
            if(!p_str)
                ret_path += tok;
            else
                ret_path += tok + "/";
        }
    }

    return ret_path;
}

void ip_and_port_split(string addr, string &ip, int &port)
{
    int colon_pos = addr.find(':');
    if(colon_pos != string::npos)
    {
        ip = addr.substr(0, colon_pos);
        port = stoi(addr.substr(colon_pos + 1));
    }
}
