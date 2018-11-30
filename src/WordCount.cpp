#include<iostream>
#include<string>
#include<vector>
#include<unistd.h>
#include<fcntl.h>
#include<algorithm>
#include<queue>
#include<string.h>
#include<mutex>
#include "WordCount.h"
#include "utilities.h"

using namespace std;

bool Compare::operator()(HeapData d1, const HeapData d2)
{
    return !lexicographical_compare(d1.data.begin(), d1.data.end(), d2.data.begin(), d2.data.end());
}

WordCountMapper::WordCountMapper(std::string job_id, int chunk_id, std::string file_path, int start_line, int no_of_lines, int no_of_reducers)
{
    this->job_id = job_id;
    this->chunk_id = chunk_id;
    this->file_path = file_path;
    this->start_line = start_line;
    this->no_of_lines = no_of_lines;
    this->no_of_reducers = no_of_reducers;
}

WordCountMapper::WordCountMapper(string request_string)
{

    cout << endl << request_string << endl;
    vector<string> req_vec = split_string(request_string, '$');
    this->job_id = req_vec[0];
    this->chunk_id = stoi(req_vec[1]);
    this->file_path = req_vec[2];
    cout << endl << this->file_path << endl;
    this->start_line = stoi(req_vec[3]);
    this->no_of_lines = stoi(req_vec[4]);
    cout << endl << " numberof lines assigned" << endl;
    this->no_of_reducers = stoi(req_vec[5]);
    cout << "number of reducers" << endl;

}

string WordCountMapper::get_job_id()
{
    return this->job_id;
}

string WordCountMapper::get_chunk_id()
{
    return to_string(this->chunk_id);

}
string WordCountMapper::get_file_path()
{
    return this->file_path;
}

int WordCountMapper::get_no_of_reducers()
{
    return this->no_of_reducers;
}

string WordCountMapper::start_job()
{

    cout<<"\n\nJOB "<<this->job_id<<" started!!"<<endl;
    string file_name = this->file_path;
    FILE * file_ptr = fopen(file_name.c_str(), "r");

    //cout<<"\n\nREACHED HERE "<<file_ptr<<endl<<endl; 
    int iteration = 0;
    string out_file_name = "temp_files/out_file"+to_string(iteration)+".txt";
    int wd = open(out_file_name.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
    int count = 0;
    char word[100];
    bzero(word,100);
    vector<string> file_words;
    while( fscanf(file_ptr, "%s", word) != EOF )
    {
        string temp_string = word;
        string word_string = "";
        for(int i=0; i<temp_string.length(); i++)
        {
            char curr_char = temp_string[i];
            if((curr_char < 65) || (curr_char > 90 && curr_char < 97) || (curr_char > 122))
            {
                continue;
            }
            word_string+=curr_char;
        }
        if(word_string.empty())
        {
            continue;
        }
        count++;
        if(count > 100000)
        {
            count = 1;
            out_file_name = "temp_files/out_file"+to_string(iteration)+".txt";
            wd = open(out_file_name.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
            sort(file_words.begin(), file_words.end());
            for(int i=0; i<file_words.size(); i++)
            {
                write(wd, file_words[i].c_str(), file_words[i].length());
                if(i!=file_words.size()-1)
                {
                    write(wd, " ", 1);
                }
            }
            close(wd);
            file_words.clear();
            iteration++;
        }
        file_words.push_back(word_string);
        bzero(word, 100);
    }
    if(!file_words.empty())
    {
        out_file_name = "temp_files/out_file"+to_string(iteration)+".txt";
        wd = open(out_file_name.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
        sort(file_words.begin(), file_words.end());
        for(int i=0; i<file_words.size(); i++)
        {
            write(wd, file_words[i].c_str(), file_words[i].length());
            if(i!=file_words.size()-1)
            {
                write(wd, " ", 1);
            }
        }
        close(wd);
        iteration++;
    }
    fclose(file_ptr);
    //iteration++;
    FILE * out_files[iteration];
    for(int i=0; i<iteration; i++)
    {
        string out_file_name = "temp_files/out_file"+to_string(i)+".txt";
        out_files[i] = fopen(out_file_name.c_str(), "r");
    }

    priority_queue<HeapData, vector<HeapData>, Compare> min_heap;

    for(int i=0; i<iteration; i++)
    {
        char buff[15];
        bzero(buff, 15);
        if( fscanf(out_files[i], "%s", buff)!=EOF )
        {
            string curr_string = buff;
            HeapData d;
            d.data = curr_string;
            d.file_id = i;
            min_heap.push(d);
        }
    }

    string output_file_name = "temp_files/word_file_sorted.txt";
    int sorted_wd = open(output_file_name.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
    bool start = true;
    while(!min_heap.empty())
    {
        HeapData d = min_heap.top();
        min_heap.pop();
        write(sorted_wd, d.data.c_str(), d.data.length());
        write(sorted_wd, " ", 1);
        int curr_id = d.file_id;
        char buff[15];
        bzero(buff, 15);
        if(fscanf(out_files[curr_id], "%s", buff)!=EOF)
        {
            string curr_string = buff;
            HeapData d;
            d.data = curr_string;
            d.file_id = curr_id;
            min_heap.push(d);
        }
    }
    close(sorted_wd);
    for(int i=0; i<iteration; i++)
    {
        fclose(out_files[i]);
    }
    FILE * sorted_file = fopen("temp_files/word_file_sorted.txt", "r");
    char buff[255];
    bzero(buff, 255);
    int current_count = 1;
    string curr_word = "";
    int out_wd = open("temp_files/output.txt",(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
    if(fscanf(sorted_file, "%s", buff)!=EOF)
    {
        curr_word = buff;
    }
    bzero(buff, 255);
    while(fscanf(sorted_file, "%s", buff)!=EOF)
    {
        string new_word = buff;
        if(new_word.compare(curr_word))
        {
            write(out_wd, curr_word.c_str(), curr_word.length());
            write(out_wd, " ", 1);
            string size_string = to_string(current_count);
            write(out_wd, size_string.c_str(), size_string.length());
            write(out_wd, "\n", 1);
            curr_word = new_word;
            current_count = 1;
        }
        else
        {
            current_count++;
        }
    }
    write(out_wd, curr_word.c_str(), curr_word.length());
    write(out_wd, " ", 1);
    string size_string = to_string(current_count);
    write(out_wd, size_string.c_str(), size_string.length());
    close(out_wd);
    fclose(sorted_file);

    cout<<"\nJOB COMPLETED\n";
    return "temp_files/output.txt";
}

// WordCountReducer::WordCountReducer(string job_id, int no_of_files)
// {
//     this->job_id = job_id;
//     this->no_of_files = no_of_files;
// }

void WordCountReducer::init(string request_string)
{
    vector<string> req_vec = split_string(request_string, '$');
    this->job_id = req_vec[0];
    this->no_of_files = stoi(req_vec[1]);
}

int WordCountReducer::get_file_count_in_category(int category)
{
    lock_guard<mutex> lock(category_file_map_lock);
    if(this->category_to_files_map.find(category)==this->category_to_files_map.end())
    {
        return -1;
    }
    return this->category_to_files_map[category];
}

void WordCountReducer::increment_files_in_category(int category)
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

void WordCountReducer::update_word_count(string word, int count)
{
    lock_guard<mutex> lock(words_count_map_lock);
    if(this->words_count.find(word)==this->words_count.end())
    {
        this->words_count[word] = count;
    }
    else
    {
        int prev_count = this->words_count[word];
        count = count+prev_count;
        this->words_count[word] = count;
    }
}

string WordCountReducer::reduce(int category,  string file_path)
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
        int count = stoi(buff);
        update_word_count(word, count);
    }
    fclose(file_ptr);
    cout<<"\nDone with "<<file_path<<endl;
    this->increment_files_in_category(category);
    if(get_file_count_in_category(category)==this->no_of_files)
    {
        //cout<<"\n\nREACHED HERE 1\n\n";
        string out_file_name = "output_files/wc_reducer_"+this->job_id+".txt";
        int wd = open(out_file_name.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
        //cout<<"\n\nREACHED HERE 2\n\n";
        for(unordered_map<string,int>::iterator it = words_count.begin(); it!=words_count.end(); ++it)
        {
            string word = it->first;
            int count = it->second;
            string count_string = to_string(count);
            write(wd, word.c_str(), word.length());
            write(wd, " ", 1);
            write(wd, count_string.c_str(), count_string.length());
            write(wd, "\n", 1);
        }
        close(wd);
        cout<<this->job_id<<" COMPLETED\n";
        return out_file_name;
    }
    return "INCOMPLETE";
}
