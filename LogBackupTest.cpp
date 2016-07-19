#include<iostream>
#include"./BIndex.h"
#include<stdlib.h>
#include<string>
#include<string.h>


using namespace std;


/*This file is used to do log and backup file test.*/


RUN_RESULT run_log_write_read_test(INDEX_NODE *root);


/*This is used to do log write and read test.*/
RUN_RESULT
run_log_write_read_test(INDEX_NODE *root)
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
