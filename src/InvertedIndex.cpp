#include<unistd.h>
#include<fcntl.h>
#include <iostream>
#include <vector>
#include <mutex>
#include <string.h>
#include <string>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include "InvertedIndex.h"
#include "utilities.h"

using namespace std;

InvertedIndexMapper::InvertedIndexMapper(string job_id, int chunk_id, vector<string> file_paths, vector<int> start_lines, vector<int> no_of_lines, int no_of_reducers)
{
    this->job_id = job_id;
    this->no_of_reducers = no_of_reducers;
    this->chunk_id = chunk_id;
    for(int i=0; i<file_paths.size(); i++)
    {
        FileInfo file_info;
        file_info.file_path = file_paths[i];
        file_info.start_line = start_lines[i];
        file_info.no_of_lines = no_of_lines[i];
        this->input_files.push_back(file_info);
    }
}

InvertedIndexMapper::InvertedIndexMapper(string request_string)
{
    vector<string> req_vec = split_string(request_string, '$');
    this->job_id = req_vec[0];
    this->chunk_id = stoi(req_vec[1]);
    this->no_of_reducers = stoi(req_vec[2]);
    FileInfo curr_file_info;
    bool first = true;
    for(int i=3; i<req_vec.size(); i++)
    {
        string req_string = req_vec[i];
        int index = (i-3);
        if(index%3 == 0)
        {
            if(!first)
            {
                this->input_files.push_back(curr_file_info);
            }
            else
            {
                first = false;
            }
            FileInfo temp;
            curr_file_info = temp;
            curr_file_info.file_path = req_string;
        }
        else if(index%3 == 1)
        {
            curr_file_info.start_line = stoi(req_string);
        }
        else if(index%3 == 2)
        {
            curr_file_info.no_of_lines = stoi(req_string);
        }
    }
    this->input_files.push_back(curr_file_info);
}

string InvertedIndexMapper::get_job_id()
{
    return this->job_id;
}

string InvertedIndexMapper::get_chunk_id()
{
    return to_string(this->chunk_id);

}

int InvertedIndexMapper::get_no_of_reducers()
{
    return this->no_of_reducers;
}

string InvertedIndexMapper::start_job()
{
    try
    {
        cout<<"\n\nJOB "<<this->job_id<<" started!!"<<endl;
        unordered_map<string, set<string>> index;
        for(int i=0; i<this->input_files.size(); i++)
        {
            FileInfo file_info = this->input_files[i];
            string file_path = file_info.file_path;
            FILE * file_ptr = fopen(file_path.c_str(), "r");
            char buff[100];
            bzero(buff, 100);
            while( fscanf(file_ptr, "%s", buff)!=EOF )
            {
                string word = buff;
                if(index.find(word)==index.end())
                {
                    set<string> file_names;
                    index[word] = file_names;
                }
                set<string> file_names = index[word];
                file_names.insert(file_path);
                index[word] = file_names;
                bzero(buff,100);
            }
            fclose(file_ptr);
        }
        string out_file_name = "output_files/output_inverted.txt";
        int wd = open(out_file_name.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
        for(unordered_map<string, set<string>>::iterator it = index.begin(); it!=index.end(); ++it)
        {
            string word = it->first;
            write(wd, word.c_str(), word.length());
            write(wd, " ", 1);
            set<string> file_names = it->second;
            for(set<string>::iterator fit = file_names.begin(); fit!=file_names.end(); ++fit)
            {
                string file_name = *fit;
                write(wd, file_name.c_str(), file_name.length());
                write(wd, " ", 1);
            }
            write(wd, "\n", 1);
        }
        close(wd);
        cout<<"\nJOB "<<this->job_id<<" CHUNK "<<this->chunk_id<<" COMPLETED\n";
    }
    catch(...)
    {
        return "FAILURE";
    }
    
    return "output_files/output_inverted.txt";
}

void InvertedIndexReducer::init(string request_string)
{
    vector<string> req_vec = split_string(request_string, '$');
    this->job_id = req_vec[0];
    this->no_of_files = stoi(req_vec[1]);
}

void InvertedIndexReducer::increment_files_in_category(int category)
{
    lock_guard<mutex> lock(category_file_map_lock);
    if(this->category_to_files_map.find(category)==this->category_to_files_map.end())
    {
        this->category_to_files_map[category] = 0;
    }
    int nf = this->category_to_files_map[category];
    nf++;
    this->category_to_files_map[category] = nf;
}

void InvertedIndexReducer::update_files_list(string word, vector<string> files_list)
{
    lock_guard<mutex> lock(words_to_files_map_lock);
    if(this->words_to_files_map.find(word)==this->words_to_files_map.end())
    {
        unordered_set<string> file_set;
        this->words_to_files_map[word] = file_set;
    }
    unordered_set<string> file_set = this->words_to_files_map[word];
    for(int i=0; i<files_list.size(); i++)
    {
        file_set.insert(files_list[i]);
    }
    this->words_to_files_map[word] = file_set;
}

int InvertedIndexReducer::get_file_count_in_category(int category)
{
    lock_guard<mutex> lock(category_file_map_lock);
    if(this->category_to_files_map.find(category)==this->category_to_files_map.end())
    {
        return -1;
    }
    return this->category_to_files_map[category];
}

string InvertedIndexReducer::reduce(int category, string file_path)
{
    try
    {
        cout<<"\n\nReducing "<<file_path<<endl;
        FILE * file_ptr = fopen(file_path.c_str(), "r");
        char buff[100];
        bzero(buff, 100);
        while( fscanf(file_ptr, "%s", buff)!=EOF )
        {
            string word = buff;
            bzero(buff, 100);
            fscanf(file_ptr, "%s", buff);
            int files_count = stoi(buff);
            vector<string> file_list;
            for(int i=0; i<files_count; i++)
            {
                bzero(buff, 100);
                fscanf(file_ptr, "%s", buff);
                string file_name = buff;
                file_list.push_back(file_name);
            }
            this->update_files_list(word, file_list);
        }

        fclose(file_ptr);
        cout<<"\n Done with "<<file_path<<endl;

        this->increment_files_in_category(category);
        if(get_file_count_in_category(category)==this->no_of_files)
        {
            string out_file_name = "output_files/R_job_"+this->job_id + "_category_" + to_string(category) + "_ii.txt";
            int wd = open(out_file_name.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
            for(unordered_map<string,unordered_set<string>>::iterator it = words_to_files_map.begin(); it!=words_to_files_map.end(); ++it)
            {
                string word = it->first;
                unordered_set<string> file_list =  it->second;
                write(wd, word.c_str(), word.length());
                for(unordered_set<string>::iterator it = file_list.begin(); it!=file_list.end(); ++it)
                {
                    string file_name = *it;
                    write(wd, " ", 1);
                    write(wd, file_name.c_str(), file_name.length());
                }
                write(wd, "\n", 1);
            }
            close(wd);
            cout<<this->job_id<<" COMPLETED\n";
            return out_file_name;
        }
    }
    catch(...)
    {
        return "FAILURE";
    }
    
    return "INCOMPLETE";
}
