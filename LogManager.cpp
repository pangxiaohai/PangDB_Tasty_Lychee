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
LOG_INFO *read_recent_logs(int);


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
	write_log.clear();
	write_log.close();
	return(RUN_SUCCESS);
}

/*This is used to execute read log.*/
LOG_INFO *
exec_read_log(int search_time)
{
	if(search_time < 0)
	{
		cout<<"Wrong input search time!\n"<<endl;
		return(NULL);
	}

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

	int is_first = 1, act_num;
	
	char user_buf[7];
	user_buf[6] = '\0';
	char act_buf[2];
	act_buf[1] = '\0';
	char key_buf[11];
	key_buf[10] = '\0';
	char end_buf[9];
	end_buf[8] = '\0';
	char log_time[11], res_len[4];
	log_time[10] = '\0';
	res_len[3] = '\0';
	while(read_log.read(log_time,10))
	{
		read_log.read(res_len, 3);
		
		int trans_time;
		int length;
		
		sscanf(log_time, "%10d", &trans_time);
		
		sscanf(res_len, "%3d", &length);
		length = length - 25;

		read_log.read(user_buf, 6);
		read_log.read(act_buf, 1);
		read_log.read(key_buf, 10);
		
		char *value_buf = create_n_byte_mem(length+1);
		value_buf[length] = '\0';
		read_log.read(value_buf, length);
		read_log.read(end_buf, 8);

		if(strcmp(end_buf, LOG_END))
		{
			cout<<"Log Error!\n"<<endl;
			res->log_err = TRUE;
			free(value_buf);
			value_buf = NULL;

			read_log.clear();
			read_log.close();
			return(res);
		}

		/*This log is what we look for.*/
		if(trans_time >= search_time)
		{
			new_log = create_log_list_mem();
			sscanf(user_buf, "%6d", &(new_log->user));
			sscanf(act_buf, "%1d", &act_num);
			new_log->act = get_action(act_num);

			new_log->time = trans_time;
	
			DATA_RECORD *new_record;
			new_record = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
			sscanf(key_buf, "%10d", &(new_record->key));
		
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

	read_log.clear();	
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

/*This is used to read recent serveral logs.*/
LOG_INFO *
read_recent_logs(int num)
{
	if(num <= 0)
	{
		cout<<"Wrong input number!"<<endl;
		return(NULL);
	}

	ifstream read_log(LOG_FILE);

        if(!read_log)
        {
                cout<<"Cannot open log file.\n"<<endl;
                return(NULL);
        }

	LOG_LIST *cur_log;
	cur_log = create_log_list_mem();

	int saved_num = 0, is_first = 1, is_circle_closed = 0;

	LOG_LIST *first_log;
	first_log = cur_log;

	char log_time[11];
	log_time[10] = '\0';
	char user_buf[7];
	user_buf[6] = '\0';
        char act_buf[2];
	act_buf[1] = '\0';
        char key_buf[11];
	key_buf[10] = '\0';
        char end_buf[9];
        end_buf[8] = '\0';
        char res_len[4];
	res_len[3] = '\0';

	int begin_time, act, len, key;
	PID user;

	while(read_log.read(log_time,10))
	{
		sscanf(log_time, "%10d", &begin_time);
		
		read_log.read(res_len, 3);
		sscanf(res_len, "%3d", &len);		

		read_log.read(user_buf, 6);
		sscanf(user_buf, "%6d", &user);
		
		read_log.read(act_buf, 1);
		sscanf(act_buf, "%1d", &act);

		read_log.read(key_buf, 10);
		sscanf(key_buf, "%10d", &key);
		
		len = len - 25;
		char *value = create_n_byte_mem(len + 1);
		value[len] = '\0';
		read_log.read(value, len);

		read_log.read(end_buf, 8);
		if(!strcmp(end_buf, LOG_END))
		{
			/*This is an avalibale log.*/
		
			/*Only read the lastest need to be handled specially.*/
			if(num == 1)
			{
				cur_log->user = user;
				cur_log->time = begin_time;
				cur_log->act = get_action(act);
				if(is_first)
				{
					cur_log->data_record = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
					is_first = 0;
					saved_num = 1;
				}
				else
				{
					free(cur_log->data_record->value);
					cur_log->data_record->value = NULL;
				}
				cur_log->data_record->key = key;
				cur_log->data_record->value = create_n_byte_mem(len+1);
				strncpy(cur_log->data_record->value, value, len+1);
                                cur_log->next_log = NULL;
			}
			else
			{	
				/*Firstly, add new log.*/
				if(is_first)
				{
					cur_log->user = user;
					cur_log->time = begin_time;
					cur_log->act = get_action(act);
					cur_log->data_record = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
					cur_log->data_record->key = key;
					cur_log->data_record->value = create_n_byte_mem(len+1);
					strncpy(cur_log->data_record->value, value, len+1);
					cur_log->next_log = NULL;
					is_first = 0;
					saved_num ++;
				}
				else
				{
					if(!is_circle_closed)
					{
						/*Circle is not closed, need to creat a new_log.*/
						LOG_LIST *new_log;
						new_log = create_log_list_mem();
	
						new_log->user = user;
                		                new_log->time = begin_time;
                        	        	new_log->act = get_action(act);
	                              		new_log->data_record = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
		                               	new_log->data_record->key = key;
        		                        new_log->data_record->value = create_n_byte_mem(len+1);
                		                strncpy(new_log->data_record->value, value, len+1);
                        		        new_log->next_log = NULL;
				
						/*Circle is not closed, add it directly.*/
						cur_log->next_log = new_log;
						cur_log = cur_log->next_log;
						saved_num ++;
					}
					else
					{
						/*Circle is closed. When adding new log, 
						need to replace old one.*/
						LOG_LIST *replace_log;
						replace_log = cur_log->next_log;
						replace_log->user = user;
						replace_log->act = get_action(act);
						free(replace_log->data_record->value);
						replace_log->data_record->key = key;
						replace_log->data_record->value = create_n_byte_mem(len+1);
						strncpy(replace_log->data_record->value, value, len+1);

						cur_log = cur_log->next_log;
					}
				}

				/*The circle must be closed now*/
				if((saved_num == num) && !is_circle_closed)
				{
					is_circle_closed = 1;
					cur_log->next_log = first_log;
				}
			}
		}
		
		free(value);
		value = NULL;
	}

	if(is_circle_closed)
	{
		/*Oldest one of which are not been replaced.*/
		first_log = cur_log->next_log;
		
		/*Need to dislink the last and first.*/
		cur_log->next_log = NULL;
	}
	
	LOG_INFO *ret;
	ret = create_log_info_mem();
	ret->log_list = first_log;

	ret->total_num = saved_num;
	ret->begin_time = first_log->time;

	if(saved_num < num)
	{
		ret->log_err = TRUE;
	}
	else
	{
		ret->log_err = FALSE;
	}
	
	read_log.clear();
	read_log.close();

	return(ret);
}
