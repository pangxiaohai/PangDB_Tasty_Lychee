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


RUN_RESULT exec_write_log(PID, ACTION, DATA_RECORD *);
LOG_INFO *exec_read_log(int);
INDEX_NODE *redo_according_log(INDEX_NODE *, LOG_INFO *);



/*This is used to execute write log.*/
RUN_RESULT 
exec_write_log(PID user, ACTION act, DATA_RECORD *data)
{
	int now;
	now = time((time_t *)NULL);
	
	ofstream write_log(LOG_FILE, ios::app);
	
	if(!write_log)
	{
		cout<<"Cannot open log file.\n"<<endl;
		return(RUN_FAILED);
	}
	
	int  value_len, res_len;
	char log_buf[30];

	value_len = data->len;
	res_len = value_len + 25;
	sprintf(log_buf,"%10d%3d%6d%1d%10d", now, res_len, user, act, data->key);
	write_log.write(log_buf, 30);
	write_log.write(data->value, value_len);
	write_log.write(LOG_END, 8);
	write_log.close();
	return(RUN_SUCCESS);
}

/*This is used to execute read log.*/
LOG_INFO *
exec_read_log(int search_time)
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
	char end_buf[9];
	end_buf[8] = '\0';
	char log_time[10], res_len[3];
	while(read_log.read(log_time,10))
	{
		read_log.read(res_len, 3);
		
		int trans_time;
		int length;
		
		/*Need to fix the result in memory.*/
                char *fix_time = create_n_byte_mem(10);
		strncpy(fix_time, log_time, 10);
		char *fix_len = create_n_byte_mem(3);
		strncpy(fix_len, res_len, 3);

		sscanf(fix_time, "%10d", &trans_time);
		
		sscanf(fix_len, "%3d", &length);
		length = length - 25;

		free(fix_time);
		free(fix_len);
		fix_time = NULL;
		fix_len = NULL;

		read_log.read(user_buf, 6);
		read_log.read(act_buf, 1);
		read_log.read(key_buf, 10);
		
		/*Fix the data in memory.*/
		char *fix_user = create_n_byte_mem(6);
		strncpy(fix_user, user_buf, 6);
		char *fix_act = create_n_byte_mem(1);
		strncpy(fix_act, act_buf, 1);
		char *fix_key = create_n_byte_mem(10);
		strncpy(fix_key, key_buf, 10);

		char *value_buf = create_n_byte_mem(length+1);
		value_buf[length] = '\0';
		read_log.read(value_buf, length);
		read_log.read(end_buf, 8);

		if(strcmp(end_buf, LOG_END))
		{
			cout<<"Log Error!\n"<<endl;
			res->log_err = TRUE;
			free(fix_user);
			free(fix_act);
			free(fix_key);
			free(value_buf);
			fix_user = NULL;
			fix_act = NULL;
			fix_key = NULL;
			value_buf = NULL;
			return(res);
		}

		/*This log is what we look for.*/
		if(trans_time >= search_time)
		{
			new_log = create_log_list_mem();
			sscanf(fix_user, "%6d", &(new_log->user));
			sscanf(fix_act, "%1d", &(new_log->act));

			free(fix_user);
                        free(fix_act);
                        fix_user = NULL;
                        fix_act = NULL;

			new_log->time = trans_time;
	
			DATA_RECORD *new_record;
			new_record = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
			sscanf(fix_key, "%10d", &(new_record->key));
			free(fix_key);
			fix_key = NULL;
		
			new_record->value = create_n_byte_mem(length+1);
			strncpy(new_record->value, value_buf,length+1);
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
		free(value_buf);
		value_buf = NULL;
	}
	
	read_log.close();	

	return(res);
}

/*This is used to redo all modifications according to log.*/
INDEX_NODE *
redo_according_log(INDEX_NODE *root, LOG_INFO *log_info)
{
	/*Need to lock whole tree during recovery.*/
	//apply_write_whole_tree_lock(root);
	if(log_info->log_err)
	{
		cout<<"Error in log! Cannot redo modifications!\n"<<endl;
		return(root);
	}

	LOG_LIST *cur_log;
	/*Older the log, former in log info.*/
	cur_log = log_info->log_list;
	int success = 0, fail = 0, igore = 0;

	while(cur_log)
	{
		if(cur_log->act == INSERT)
		{
			if(insert_node(root, cur_log->data_record))
			{
				success ++;
			}
			else
			{
				fail ++;
			}
		}
		else if(cur_log->act == UPDATE)
		{
			if(update_value(root, cur_log->data_record))
			{
				success ++;
                        }
                        else
                        {
                                fail ++;
			}
		}
		else if(cur_log->act == DELETE)
		{
			if(delete_node(root, cur_log->data_record->key))
			{
                                success ++;
                        }
                        else
                        {
                                fail ++;
                        }
		}
		else
		{
			/*Igore others.*/
			igore ++;
		}
		
		cur_log = cur_log->next_log;
	}

	cout<<"Totally "<<(success + fail + igore)<<" logs checked."<<
		success<<" redos successed, "<<fail<<" redos failed, "<<
		igore<<" redos igored.\n"<<endl;

	//free_write_whole_tree_lock(root);
	return(root);
}
