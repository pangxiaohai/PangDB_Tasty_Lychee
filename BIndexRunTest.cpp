/*This file is used for running test cases.*/

#include<iostream>
#include"./BIndex.h"
#include<math.h>
#include<stdlib.h>

using namespace std;

#define TOTAL_CASE 31
#define TESTDATANUM 100


/*Declare Functions*/
TEST_SUMMARY run_all_tests(INDEX_NODE *);
INDEX_NODE *test_init(void);
void run_exist(INDEX_NODE *);
void end_test(INDEX_NODE *);


/*This is used to run test on exist test db.*/
void
run_exist(INDEX_NODE *root)
{
	TEST_SUMMARY result;

        result = run_all_tests(root);

        cout<<"Total case number: "<<result.total_num
                <<"\nSuccess number: "<<result.success_num
                <<"\nFailed number: "<<result.fail_num<<"\n"<<endl;

        return;
}

/*This is used to intial test*/
INDEX_NODE *
test_init(void)
{
	INDEX_NODE *root;
        int data_num = TESTDATANUM;
        DATA_RECORD_LIST *data_list;
        data_list = generate_random_data(data_num);

        root = mk_index(data_list, data_num);

	return(root);
	
}

/*This is used to end test.*/
void
end_test(INDEX_NODE *root)
{
	if(delete_whole_tree(root))
	{
		cout<<"End Test Successfully!\n"<<endl;
	}
	return;
}

/*This is used to run all tests.*/
TEST_SUMMARY
run_all_tests(INDEX_NODE *root)
{
	TEST_SUMMARY result;
	
	result.total_num = TOTAL_CASE;
	result.success_num = 0;
	result.fail_num = 0;

	/*Make index test*/
	if(run_mk_BIndex_test(root))
	{
		result.success_num ++;
	}
	else
	{
		result.fail_num ++;
	}

	/*Range search test.*/
	if(run_range_search_test(root))
        {
                result.success_num +=4;
        }
        else
        {
                result.fail_num +=4;
        }

	/*Top and Bottom operations test*/
	if(run_top_bottom_test(root))
	{
		result.success_num +=4;
	}
	else
        {
                result.fail_num +=4;
        }

	/*Read test*/
	if(run_read_test(root))
	{
                result.success_num +=3;
        }
        else
        {
                result.fail_num +=3;
        }
	
	/*Update test*/
	if(run_update_test(root))
	{
                result.success_num +=3;
        }
        else
        {
                result.fail_num +=3;
        }
	
	/*Delete test*/
	if(run_delete_test(root))
	{
                result.success_num +=3;
        }
        else
        {
                result.fail_num +=3;
        }

	/*Insert test*/
	if(run_insert_test(root))
        {
                result.success_num +=4;
        }
        else
        {
                result.fail_num +=4;
        }

	/*Backup write and read test.*/
	if(run_backup_write_read_test(root))
	{
                result.success_num +=2;
        }
        else
        {
                result.fail_num +=2;
        }

        /*Log write and read test.*/
        if(run_log_write_read_test(root))
        {
                result.success_num +=1;
        }
        else
        {
                result.fail_num +=1;
        }

	if(run_read_recent_log_test(root))
        {
                result.success_num += 3;
        }
        else
        {
                result.fail_num += 3;
        }

	if(run_data_recovery_test())
        {
                result.success_num += 1;
        }
        else
        {
                result.fail_num += 1;
        }

	
	if(run_batch_insert_test(root))
	{
		result.success_num += 1;
	}
	else
	{
                result.fail_num += 1;
        }

	if(run_batch_delete_test(root))
        {
                result.success_num += 1;
        }
        else
        {
                result.fail_num += 1;
        }

	return(result);
}
