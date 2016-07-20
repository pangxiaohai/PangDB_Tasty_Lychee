#include<iostream>
#include"./BIndex.h"
#include<stdlib.h>
#include<string>
#include<string.h>
#include<fstream>


using namespace std;


/*This file is used to do log and backup file test.*/


RUN_RESULT run_backup_write_read_test(INDEX_NODE *root);
RUN_RESULT run_log_write_read_test(INDEX_NODE *);
void print_log_details(LOG_INFO *);


/*This is used to run backup write and read test.*/
RUN_RESULT
run_backup_write_read_test(INDEX_NODE *root)
{
	if(!exec_write_all_data(root, BACK_FILE1))
	{
		cout<<"Write backup file failed!\n"<<endl;
		return(RUN_FAILED);
	}
	else
	{
		cout<<"Write backup file successed!\n"<<endl;
	}

	DATA_INFO *res;
	res = exec_read_data(BACK_FILE1);

	if(!res)
	{
		cout<<"Read backup file failed!\n"<<endl;
                return(RUN_FAILED);
	}
	else
	{
		cout<<"Read backup file successed!\n"<<endl;
		cout<<"Total records: "<<res->num<<" Max key: "<<res->max_key
			<<" Min key: "<<res->min_key<<" \n"<<endl;
		cout<<"Following are all the data records: "<<endl;
		test_list(res->data_list, res->num, ON);
		free_data_info_mem(res);
		res = NULL;
	}

	return(RUN_SUCCESS);	
}

/*This is used to run log write and read test.*/
RUN_RESULT
run_log_write_read_test(INDEX_NODE *root)
{
	/*First get the time of the first log.*/
	ifstream read_log(LOG_FILE);

	if(!read_log)
	{
		cout<<"Open log file failed!\n"<<endl;
		return(RUN_FAILED);
	}

	char first_time[10];
	int start_time;
	read_log.seekg(0, ios_base::beg);
	read_log.read(first_time, 10);
	
	/*Need to fix the result*/
	char *fix_time = create_n_byte_mem(10);
	strncpy(fix_time, first_time, 10);

	sscanf(first_time, "%10d", &start_time);

	free(fix_time);
	fix_time = NULL;
	
	read_log.close();
	LOG_INFO *all_log;
	all_log = exec_read_log(start_time);

	if(all_log->log_err)
	{
		cout<<"Error in log!\n"<<endl;
		free_log_info_mem(all_log);
		all_log = NULL;
		return(RUN_FAILED);
	}

	cout<<"Total log number: "<<all_log->total_num<<" Begin Time: "
		<<all_log->begin_time<<endl;

	print_log_details(all_log);

	return(RUN_SUCCESS);
}

/*This is used to print log details*/
void
print_log_details(LOG_INFO *log)
{
	LOG_LIST *cur_log;
	cur_log = log->log_list;
	while(cur_log)
	{
		cout<<"User: "<<cur_log->user<<" Time: "<<cur_log->time<<" Action: ";
		switch(cur_log->act)
		{
			case 1:
				cout<<" INSERT";
				break;
			case 2:
				cout<<" UPDATE";
				break;
			default:
				cout<<" DELETE";
				break;
		}
		
		cout<<" Key: "<<cur_log->data_record->key<<" Value: "
			<<cur_log->data_record->value<<endl;
		
		cur_log = cur_log->next_log;
	}

	return;
}
