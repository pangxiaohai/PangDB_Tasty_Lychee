#include<iostream>
#include"./BIndex.h"
#include<math.h>
#include<stdlib.h>
#include<string>
#include<string.h>
#include<time.h>
#include<sys/time.h>
#include<fstream>
#include <unistd.h>


using namespace std;


IDX_BOOL is_auto_backup;
IDX_BOOL is_force_backup;


/*This file is used to backup data and support recovery.*/


RUN_RESULT exec_write_all_data(INDEX_NODE *, char *);
INDEX_NODE *data_recovery(void);
BACK_INFO *search_backup_info(void);
DATA_INFO *exec_read_data(char *);
BACK_INFO *search_backup_file(void);
int write_file_according_log(char *, LOG_INFO *);
RUN_RESULT auto_backup(void);


/*This is used to write all data to disk file.*/
RUN_RESULT
exec_write_all_data(INDEX_NODE *root, char *file_name)
{
	if(is_auto_backup)
	{
		cout<<"Auto backup runnig.\n"<<endl;
		return(RUN_FAILED);
	}

	is_force_backup = TRUE;

	/*Two backup files to use. If backup failed, another can be used.*/

	int file_right = 0;
	if((!(strcmp(file_name, BACK_FILE1))) || 
			(!(strcmp(file_name, BACK_FILE2))))
	{
		file_right = 1;
	}

	if(!file_right)
	{
		cout<<"Wrong backup filename!\n"<<endl;
		return(RUN_FAILED);
	}

	ofstream write_file(file_name, ios::trunc);
	ofstream back_log(BACK_LOG, ios::app);

	if((!write_file)||(!back_log))
	{
		cout<<"Write file failed!\n"<<endl;
		return(RUN_FAILED);
	}
	
	LEAF_NODE *cur_leaf;
	NODE_PATH_INFO *node_path;
	
	node_path = search_special_leaf(root, FIRST_NODE);
	if((!node_path->status) || (!node_path->is_include_leaf)) 
	{
		cout<<"Cannot backup now!\n"<<endl;
		return(RUN_FAILED);
	}

	/*Need to apply read lock here?*/
	/*
	if(!apply_read_all_leaves_lock(root))
	{
		cout<<"Cannot backup now!\n"<<endl;
                return(RUN_FAILED);
	}
	*/

	uint64_t begin_time;
	int value_len;

	struct timeval now;
	gettimeofday(&now, NULL);
	begin_time = now.tv_sec * 1000 + now.tv_usec/1000;
	//free_read_lock(node_path);
	cur_leaf = node_path->end_leaf;
	while(cur_leaf)
	{
		char *data_buf = create_n_byte_mem(13);
		value_len = cur_leaf->data_record->len;
		sprintf(data_buf,"%10d%3d", cur_leaf->data_record->key, value_len);
		write_file.write(data_buf,13);
		write_file.write(cur_leaf->data_record->value, value_len);
		write_file.write(DATA_END, 8);
		
		free(data_buf);
		data_buf = NULL;
		//free_read_leaf_lock(cur_leaf);
		cur_leaf = cur_leaf->back_node;
	}

	char back_time[21];
	back_time[20] = '\0';
	sprintf(back_time, "%20ld", begin_time);
	back_log.write(back_time, 20);
	back_log.write(file_name, 12);
	back_log.write(BACK_END, 8);

	/*Only last time backup will record in this file*/
	/*It is used for fast recovery. If it lost, system will check back log.*/
	ofstream last_log(LAST_LOG, ios::trunc);
	if(last_log)
	{
		sprintf(back_time, "%20ld", begin_time);
		last_log.write(back_time, 20);
        	last_log.write(file_name, 12);
        	last_log.write(BACK_END, 8);
		last_log.clear();
		last_log.close();
	}

	write_file.clear();
	write_file.close();
	back_log.clear();
	back_log.close();

	is_force_backup = FALSE;
	
	return(RUN_SUCCESS);
}

/*This is used to data recovery.*/
INDEX_NODE *
data_recovery(void)
{
	BACK_INFO *back_info;

	back_info = search_backup_info();

	if(!back_info)
	{
		cout<<"Cannot find file! Recovery failed!\n"<<endl;
		return(NULL);
	}

	/*Read data from backup file.*/
	DATA_INFO *data_info;
	data_info = exec_read_data(back_info->filename);

	if(!data_info)
	{
		cout<<"Error! Cannot recover data!\n"<<endl;
		return(NULL);
	}

	test_list(data_info->data_list, data_info->num, OFF);
	/*Re-build the whole tree*/
	INDEX_NODE *mid_root = mk_index(data_info->data_list, data_info->num);

	//free_data_info_mem(data_info);
	//data_info = NULL;

	/*Get logs after backup begin time and re-do the modifications.*/
	LOG_INFO *redo_log;
	redo_log = exec_read_log(back_info->begin_time);
	
	free(back_info);
	back_info = NULL;

	INDEX_NODE *ret_root;
	if(redo_log)
	{
		ret_root = redo_according_log(mid_root, redo_log);
		free_log_info_mem(redo_log);
        	redo_log = NULL;
	}
	else
	{
		ret_root = mid_root;
	}

	return(ret_root);
}

/*This is used to soft backup*/
/*In this mod, no data need to be lock.*/
RUN_RESULT
auto_backup(void)
{
	if(is_force_backup)
	{
		cout<<"Another backup runnig.\n"<<endl;
		return(RUN_FAILED);
	}
	is_auto_backup = TRUE;

	BACK_INFO *back_info;
	uint64_t exec_time;

        back_info = search_backup_info();

        if(!back_info)
        {
                cout<<"No backinfo found! Do force backup!\n"<<endl;
		is_auto_backup = FALSE;
                return(RUN_FAILED);
        }
	
	ifstream write_file(back_info->filename);

	if(!write_file)
	{
		cout<<"Cannot open file! Auto backup failed.\n"<<endl;
		is_auto_backup = FALSE;
		return(RUN_FAILED);
	}

	LOG_INFO *commit_log;
        commit_log = exec_read_log(back_info->begin_time);
	if(!commit_log)
	{
		return(RUN_SUCCESS);
	}

	struct timeval now;	
	gettimeofday(&now, NULL);
	
	exec_time = now.tv_sec * 1000 + now.tv_usec / 1000;;

	int success_num;
	success_num = write_file_according_log(back_info->filename, commit_log);
	if(success_num == -1)
	{
		is_auto_backup = FALSE;
		return(RUN_FAILED);
	}

	char back_time[21];
	back_time[20] = '\0';
	ofstream last_log(LAST_LOG, ios::trunc);
        if(last_log)
        {
                sprintf(back_time, "%20ld", exec_time);
		last_log.write(back_time, 20);
		last_log.write(back_info->filename, 12);
		last_log.write(BACK_END, 8);
		last_log.clear();
                last_log.close();
        }

	ofstream back_log(BACK_LOG, ios::app);

	if(back_log)
        {
                sprintf(back_time, "%20ld", exec_time);
		back_log.write(back_time, 20);
		back_log.write(back_info->filename, 12);
		back_log.write(BACK_END, 8);
		back_log.clear();
                back_log.close();
        }

	free_log_info_mem(commit_log);
        commit_log = NULL;
        free(back_info);
        back_info = NULL;

	is_auto_backup = FALSE;
	return(RUN_SUCCESS);
}

/*This is used to modify data in file according to logs.*/
int
write_file_according_log(char *filename, LOG_INFO *log_info)
{
	if(log_info->log_err)
	{
		//cout<<"Log error!\n"<<endl;
		return(-1);
	}

	if(!log_info->total_num)
	{
		//cout<<"No log record found!\n"<<endl;
		return(0);
	}

	LOG_LIST *cur_log;
	cur_log = log_info->log_list;

	int success = 0, failed = 0, ignore = 0;
	while(cur_log)
	{
		if(cur_log->act == UPDATE)
		{
			if(update_to_file(filename, cur_log->data_record))
			{
				success ++;
			}
			else
			{
				failed ++;
			}
		}
		else if(cur_log->act == DELETE)
		{
			if(delete_from_file(filename, cur_log->data_record))
                        {
                                success ++;
                        }
                        else
                        {
                                failed ++;
                        }
		}
		else if(cur_log->act == INSERT)
		{
			if(insert_to_file(filename, cur_log->data_record))
                        {
                                success ++;
                        }
                        else
                        {
                                failed ++;
                        }
		}
		else
		{
			ignore ++;
		}
		
		/*
		cout<<"Total "<<(success + failed + ignore)<<" logs found. "
			<<success<<" successed. "<<failed<<" failed. "<<
			ignore<<" ignored.\n"<<endl;
		*/
		return(success);	
	}
}

/*This is used to search backup info*/
BACK_INFO *
search_backup_info(void)
{
        char end_mark[9], begin_time[21];
	end_mark[8] = '\0';
	begin_time[20] = '\0';
        int found_file = 0;
        BACK_INFO *back_info;

        back_info = (BACK_INFO *)malloc(sizeof(BACK_INFO));
	back_info->filename = create_n_byte_mem(13);
	back_info->filename[12] = '\0';

        ifstream read_last(LAST_LOG);
        if(read_last)
        {
                read_last.read(begin_time, 20);
                read_last.read(back_info->filename, 12);
                read_last.read(end_mark, 8);
		uint64_t back_time;
		sscanf(begin_time, "%ld", &back_time);
                back_info->begin_time = back_time;
                found_file = 1;
		read_last.clear();
                read_last.close();
        }

        if(strcmp(end_mark, BACK_END))
        {
                back_info->begin_time = 0;
                free(back_info);
                back_info = NULL;

                cout<<"Last log error!\n"<<
                "Need to search whole back up log.\n"<<endl;
                found_file = 0;
        }

        if(!found_file)
        {
                back_info = search_backup_file();
        }
	
	return(back_info);
}

/*This is used to search backup log.*/
BACK_INFO *
search_backup_file(void)
{
	ifstream back_log(BACK_LOG);
	if(!back_log)
	{
		cout<<"Cannot open backup log file!\n"<<endl;
		return(NULL);
	}
	
	BACK_INFO *res;
	res = (BACK_INFO *)malloc(sizeof(BACK_INFO));

	char name_buf[13], end_mark[9], begin_time[21];
	begin_time[20] = '\0';
	name_buf[12] = '\0';
	end_mark[8] = '\0';
	int is_avaliable = 0;

	while(!back_log.eof())
	{
		back_log.read(begin_time, 20);
                back_log.read(name_buf, 12);
                back_log.read(end_mark, 8);
		
		if((!strcmp(end_mark, BACK_END)) && 
			((!strcmp(name_buf, BACK_FILE1)) 
				|| (!strcmp(name_buf, BACK_FILE1))))
		{
			uint64_t back_time;
			sscanf(begin_time, "%ld", &back_time);
			/*This is an available log.*/
	                res->begin_time = back_time;

			strcpy(res->filename, name_buf);
			is_avaliable = 1;
		}
	}

	if(!is_avaliable)
	{
		free(res);
		res = NULL;
		return(NULL);
	}

	back_log.clear();
	back_log.close();
	return(res);
}

/*This is used to execute read data from file*/
DATA_INFO *
exec_read_data(char *file_name)
{
	if(((strcmp(file_name, BACK_FILE1))
                     && (strcmp(file_name, BACK_FILE1))))
	{
		cout<<"Wrong file name!\n"<<endl;
		return(NULL);
	}

	ifstream read_data(file_name);
	if(!read_data)
	{
		cout<<"Cannot open data file!\n"<<endl;
		return(NULL);
	}

	int key, len, is_first = 1, num = 0;

	DATA_RECORD_LIST *res, *new_data, *cur_data;
	res = create_data_record_list_mem();
	DATA_RECORD *new_record;

	char key_buf[11];
	key_buf[10] = '\0';
	char value_len[4];
	value_len[3] = '\0';
	int min_key = KEY_RANGE + 1;
	int max_key = 0;

	while(read_data.read(key_buf, 10))
	{
		char *end_mark = create_n_byte_mem(9);
		end_mark[8] = '\0';
		
		read_data.read(value_len, 3);

		sscanf(key_buf, "%10d", &key);
		sscanf(value_len, "%3d", &len);

		char *value;
		value = create_n_byte_mem(len+1);
		value[len] = '\0';
		read_data.read(value, len);
		read_data.read(end_mark, 8);

	
		/*This is a right data record, recover it.
		Otherwise, ignore it.*/
		if(!strcmp(end_mark, DATA_END))
		{
			new_record = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
			new_record->key = key;
			new_record->value = create_n_byte_mem(len+1);
			(new_record->value)[len] = '\0';
			strncpy(new_record->value, value, len+1);
			num ++;
			
			if(key > max_key)
			{
				max_key = key;
			}
			if(key < min_key)
			{
				min_key = key;
			}

			if(is_first)
			{
				res->data_record = new_record;
				cur_data = res;
				res->next_data = NULL;
				is_first = 0;
			}
			else
			{
				new_data = create_data_record_list_mem();
				new_data->data_record = new_record;
				new_data->next_data = NULL;
				cur_data->next_data = new_data;
				cur_data = cur_data->next_data;
			}
		}

		free(value);
		value = NULL;
		free(end_mark);
		end_mark = NULL;
	}
	DATA_INFO *data_info;
	
	if(num)
	{
		data_info = create_data_info_mem();
		data_info->data_list = res;
		data_info->num = num;
		data_info->max_key = max_key;
		data_info->min_key = min_key;	
	}
	else
	{
		return(NULL);
	}

	return(data_info);
}
