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
RUN_RESULT run_read_recent_log_test(INDEX_NODE *);
RUN_RESULT read_recent_log_test_1(void);
RUN_RESULT read_recent_log_test_2(void);
RUN_RESULT read_recent_log_test_3(void);


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

	char first_time[21];
	first_time[20] = '\0';
	uint64_t start_time;
	read_log.seekg(0, ios_base::beg);
	read_log.read(first_time, 20);
	
	sscanf(first_time, "%20d", &start_time);

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
	free_log_info_mem(all_log);
        all_log = NULL;

	return(RUN_SUCCESS);
}

/*This is used to run read recent several logs test.*/
RUN_RESULT
run_read_recent_log_test(INDEX_NODE *root)
{
	cout<<"\nBegin read recnet log test:"<<endl;
	
	if(read_recent_log_test_1() && read_recent_log_test_2()
			&& read_recent_log_test_3())
	{
		return(RUN_SUCCESS);
	}
	else
	{
		return(RUN_FAILED);
	}
}

RUN_RESULT
read_recent_log_test_1(void)	
{	/*Read lastest one log*/
	LOG_INFO *res1;
	
	cout<<"Read lastest one log: "<<endl;
	res1 = read_recent_logs(1);

	if(!res1)
	{
		cout<<"Open log file failed!"<<endl;
		return(RUN_FAILED);
	}

	if(res1->log_err)
	{
		cout<<"Not enough log found!\n"<<endl;
	}

	cout<<"Following are the log details: "<<endl;

	cout<<"Total log number: "<<res1->total_num<<" Begin Time: "
                <<res1->begin_time<<endl;

        print_log_details(res1);

	free_log_info_mem(res1);
	res1 = NULL;
	
	return(RUN_SUCCESS);
}

RUN_RESULT
read_recent_log_test_2(void)
{
	/*Read lastest sevral log*/
        LOG_INFO *res2;

        cout<<"Read lastest 4 logs: "<<endl;
        res2 = read_recent_logs(4);

        if(!res2)
        {
                cout<<"Open log file failed!"<<endl;
                return(RUN_FAILED);
        }

        if(res2->log_err)
        {
                cout<<"Not enough log found!\n"<<endl;
        }

	cout<<"Following are the log details: "<<endl;

        cout<<"Total log number: "<<res2->total_num<<" Begin Time: "
                <<res2->begin_time<<endl;

        print_log_details(res2);

        free_log_info_mem(res2);
        res2 = NULL;
	
	return(RUN_SUCCESS);
}

RUN_RESULT
read_recent_log_test_3(void)
{
	/*Not enough logs*/
        LOG_INFO *res3;

        cout<<"Read lastest 15 logs: "<<endl;
        res3 = read_recent_logs(15);

        if(!res3)
        {
                cout<<"Open log file failed!"<<endl;
                return(RUN_FAILED);
        }

        if(res3->log_err)
        {
                cout<<"Not enough log found!\n"<<endl;
        }

        cout<<"Following are the log details: "<<endl;

        cout<<"Total log number: "<<res3->total_num<<" Begin Time: "
                <<res3->begin_time<<endl;

        print_log_details(res3);

        //free_log_info_mem(res3);
        //res3 = NULL;

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
			case READ:
				cout<<" READ";
			case INSERT:
				cout<<" INSERT";
				break;
			case UPDATE:
				cout<<" UPDATE";
				break;
			case DELETE:
				cout<<" DELETE";
				break;
			default:
				cout<<" UNDEFINED";
				break;
		}
		
		cout<<" Key: "<<cur_log->data_record->key<<" Value: "
			<<cur_log->data_record->value<<endl;
		
		cur_log = cur_log->next_log;
	}

	return;
}
