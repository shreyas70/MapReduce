#include "Utility.h"

using namespace std;

vector<string> split_string(string input_string, char delimiter)
{
    vector<string> output_vector;
    string curr_string = "";
    for(int i=0; i<input_string.length(); i++)
    {
        char curr_char = input_string[i];
        if(curr_char == delimiter)
        {
            output_vector.push_back(curr_string);
            curr_string = "";
        }
        else
        {
            curr_string+=curr_char;
        }
    }
    output_vector.push_back(curr_string);
    return output_vector;
}