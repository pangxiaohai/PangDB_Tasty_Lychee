#include<iostream>
#include"./BIndex.h"
#include<math.h>
#include<stdlib.h>
#include<string>
#include<string.h>
#include<time.h>
#include<fstream>

using namespace std;


/*This file is used to manager log.*/


#define LOG_FILE "./PangDB_Log"


RUN_RESULT exec_write_log(PID, ACTION, DATA_RECORD *);
LOG_INFO *exec_read_log(time_t);


/*This is used to execute write log.*/
RUN_RESULT 
exec_write_log(PID user, ACTION act, DATA_RECORD *data)
{
	time_t now;
	time(&now);
	
	ofstream write_log(LOG_FILE, ios::app);
	
	if(!write_log)
	{
		cout<<"Cannot open log file.\n"<<endl;
		return(RUN_FAILED);
	}
	
	int  value_len, res_len;
	char log_buf[30];

	value_len = strlen(data->value);
	res_len = value_len + 25;
	sprintf(log_buf,"%10d%3d%6d%1d%10d", now, res_len, user, act, data->key);
	write_log<<log_buf<<data->value<<" LOG_END"<<endl;
	write_log.close();
	return(RUN_SUCCESS);
}

/*This is used to execute read log.*/
LOG_INFO *
exec_read_log(time_t search_time)
{
	LOG_INFO *res;
	res = create_log_info_mem();
	LOG_LIST *cur_log, *new_log;
	cur_log = NULL;

	res->log_list = cur_log;
	res->total_num = 0;
	res->begin_time = search_time;
	res->log_err = FALSE;
	
	ifstream read_log(LOG_FILE);
	
	if(!read_log)
	{
		cout<<"Cannot open log file.\n"<<endl;
                return(NULL);
	}

	int is_first = 1;
	
	char user_buf[6];
	char act_buf[1];
	char key_buf[10];
	char value_buf[MAXLENGTH];
	char end_buf[8];
	char log_time[10], res_len[3];
	while(!read_log.eof())
	{
		read_log.read(log_time, 10);
		read_log.read(res_len, 3);
		
		long trans_time;
		int length;
		sscanf(log_time, "%d", &trans_time);
		sscanf(res_len, "%d", &length);
		length = length - 25;

		read_log.read(user_buf, 6);
		read_log.read(act_buf, 1);
		read_log.read(key_buf, 10);
		read_log.read(value_buf, length);
		read_log.read(end_buf, 7);

		if(strcmp(end_buf, " LOG_END"))
		{
			cout<<"Log Error!\n"<<endl;
			res->log_err = TRUE;
			return(res);
		}

		/*This log is what we look for.*/
		if(trans_time >= search_time)
		{
			new_log = create_log_list_mem();
			sscanf(user_buf, "%d", &(new_log->user));
			sscanf(act_buf, "%d", &(new_log->act));
			new_log->time = trans_time;
	
			DATA_RECORD *new_record;
			new_record = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
			sscanf(key_buf, "%d", &(new_record->key));
			strcpy(new_record->value, value_buf);
			new_log->data_record = new_record;
			new_log->next_log = NULL;
		
			res->begin_time = trans_time;
				
			if(is_first)
			{
				res->log_list = new_log;
				cur_log = res->log_list;
				res->total_num ++;
				is_first = 0;
			}
			else
			{
				cur_log->next_log = new_log;
				cur_log = cur_log->next_log;
				res->total_num ++;
			}
		
		}
	}
	
	read_log.close();	

	return(res);
}
