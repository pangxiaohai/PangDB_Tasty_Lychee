/*This file is used for running test cases.*/

#include<iostream>
#include"./BIndex.h"
#include<math.h>
#include<stdlib.h>

using namespace std;

#define TOTAL_CASE 22
#define TESTDATANUM 100


/*Declare Functions*/
TEST_SUMMARY run_all_tests(INDEX_NODE *);
void run_test(void);
INDEX_NODE *test_init(void);
void run_exist(void);
void end_test(void);


/*Global Varibles*/
INDEX_NODE *test_root;

/*This is used to run all tests when db not setup.*/
void
run_test(void)
{
	TEST_SUMMARY result;
	test_root = test_init();

	result = run_all_tests(test_root);

	cout<<"Total case number: "<<result.total_num
		<<"\nSuccess number: "<<result.success_num
		<<"\nFailed number: "<<result.fail_num<<"\n"<<endl;

	return;
}

/*This is used to run test on exist test db.*/
void
run_exist(void)
{
	TEST_SUMMARY result;

        result = run_all_tests(test_root);

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
end_test(void)
{
	if(delete_a_tree(test_root))
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

	return(result);
}
