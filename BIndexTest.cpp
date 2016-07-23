#include<iostream>
#include"./BIndex.h"
#include<stdlib.h>
#include<string>
#include<string.h>

using namespace std;

#define ALL_REST_LEAVES -1

/*This file is used for BIndex Test.*/


/*Here is all the functions used for testing.*/
RUN_RESULT run_mk_BIndex_test(INDEX_NODE *);
DATA_RECORD_LIST *generate_random_data(int);
void draw_a_tree(INDEX_NODE *);
void draw_nodes(INDEX_NODE *, int);
void draw_leaves_list(LEAF_NODE *, int, SEARCH_DIR);
void draw_all_rest_leaves(LEAF_NODE *, SEARCH_DIR);
void execute_draw_leaves(LEAF_NODE *, int, SEARCH_DIR);
void draw_a_leaf(LEAF_NODE *, int);
void for_test(int);
void test_list(DATA_RECORD_LIST *, int, ON_OFF);
void test_leaf_link(LEAF_NODE *, ON_OFF);
void test_node_info(NODE_INFO *, ON_OFF);
void test_level_info(LEVEL_INFO *, ON_OFF);
RUN_RESULT run_range_search_test(INDEX_NODE *);
RUN_RESULT run_top_bottom_test(INDEX_NODE *);
int produce_actual_key(INDEX_NODE *, int);
RUN_RESULT run_update_test(INDEX_NODE *);
RUN_RESULT run_delete_test(INDEX_NODE *);
RUN_RESULT run_insert_test(INDEX_NODE *);


/*Run make BIndex Test.*/
RUN_RESULT
run_mk_BIndex_test(INDEX_NODE *root)
{
	draw_a_tree(root);
	return RUN_SUCCESS;	
}

/*Run range search operation test.*/
RUN_RESULT
run_range_search_test(INDEX_NODE *root)
{
	int range[6] = {-100, (int)ceil(KEY_RANGE/10), (int)ceil(0.4*KEY_RANGE), 
		(int)ceil(0.6*KEY_RANGE), (int)ceil(0.9*KEY_RANGE), KEY_RANGE + 100};
	
	/*Range Search Test 1.*/
	cout<<"Range search test1:\n"<<range[0]<<" ~ "<<range[1]<<" ASC:\n"<<endl;
	range_search(root, range[0], range[1], ASC);

	/*Range Search Test 2.*/
        cout<<"Range search test2:\n"<<range[2]<<" ~ "<<range[3]<<" DSC:\n"<<endl;
        range_search(root, range[2], range[3], DSC);

	/*Range Search Test 3.*/
        cout<<"Range search test3:\n"<<range[3]<<" ~ "<<range[4]<<" ASC:\n"<<endl;
        range_search(root, range[3], range[4], ASC);

	/*Range Search Test 4.*/
        cout<<"Range search test4:\n"<<range[4]<<" ~ "<<range[5]<<" DSC:\n"<<endl;
        range_search(root, range[4], range[5], DSC);

	return(RUN_SUCCESS);
}

/*Run Top and Bottom operation test.*/
RUN_RESULT
run_top_bottom_test(INDEX_NODE *root)
{
	/*Top Bottom Test 1.*/
        cout<<"Bottom test1:\n"<<"BOTTOM 12 ASC:\n"<<endl;
        bottom_search(root, 12, ASC);

	/*Top Bottom Test 2.*/
        cout<<"Top test2:\n"<<"TOP 10 DSC:\n"<<endl;
        top_search(root, 10, DSC);

	/*Top Bottom Test 3.*/
        cout<<"Bottom test3:\n"<<"BOTTOM 102 DSC:\n"<<endl;
        bottom_search(root, 102, DSC);

	/*Top Bottom Test 4.*/
        cout<<"Top test4:\n"<<"TOP 107 ASC:\n"<<endl;
        top_search(root, 107, ASC);

        return(RUN_SUCCESS);
}

/*Run read test.*/
RUN_RESULT 
run_read_test(INDEX_NODE *root)
{
	/*Read Test 1*/
	int test1 = (int)(rand()%KEY_RANGE);
	cout<<"Read test1:\n"<<"Read a random key: "<<test1
		<<"\nThis key may not exsit.\n"<<endl;
	read_value(root, test1);

	/*Read Test 2*/
        int test2 = -232;
        cout<<"Read test2:\n"<<"Read an invalid key: "<<test2<<"\n"<<endl;
        read_value(root, test2);

	/*Read Test 3*/
	int near_key = (int)(rand()%KEY_RANGE);
	
	/*Produce Actual Key.*/
        int test3 = produce_actual_key(root, near_key);

        cout<<"Read test3:\n"<<"Read actual key: "<<test3<<"\n"<<endl;
        read_value(root, test3);
	
	return(RUN_SUCCESS);
}

/*Run update test.*/
RUN_RESULT
run_update_test(INDEX_NODE *root)
{
	char str[] = "New Value";
	DATA_RECORD *data1, *data2, *data3;
	data1 = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
	data2 = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
	data3 = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));

	int len = strlen(str);
	data1->value = create_n_byte_mem(len);
	/*Update Test 1.*/
	int rand_key1 = (int)(rand()%KEY_RANGE);
	data1->key = rand_key1;
	strcpy(data1->value, str);
	data1->len = len;
	
        cout<<"Update test1:\n"<<"Update value with a random key: "<<rand_key1
                <<"\nThis key may not exsit.\n"<<endl;
	if(update_value(root, data1))
	{
		cout<<"Key exsits!\n"<<endl;
		read_value(root, rand_key1);
		cout<<"Value has been changed!\n"<<endl;
	}
	free(data1->value);
	data1->value = NULL;
	free(data1);
	data1 = NULL;
	
	 /*Update Test 2.*/
        int near_key2 = (int)(rand()%KEY_RANGE), act_key2;
        act_key2 = produce_actual_key(root, near_key2);
	data2->key = act_key2;
	data2->value = create_n_byte_mem(len);
	strcpy(data2->value, str);
	data2->len = len;

        cout<<"Update test2:\n"<<"Update value with an actual key: "<<act_key2<<endl;
        if(update_value(root, data2))
	{
		read_value(root, act_key2);
                cout<<"Value has been changed!\n"<<endl;
	}
	free(data2->value);
	data2->value = NULL;
	free(data2);
	data2 = NULL;

	/*Update Test 3.*/
        int near_key3 = (int)(rand()%KEY_RANGE), act_key3;
        act_key3 = produce_actual_key(root, near_key3);
        data3->key = act_key3;

	LEAF_NODE *leaf;
	leaf = exec_read_value(root, act_key3);
	data3->value = create_n_byte_mem(len);
	strcpy(data3->value, leaf->data_record->value);
	data3->len = len;

        cout<<"Update test3:\n"<<"Update value with an actual"
		<<" key and its original value: "<<act_key3<<endl;
        if(update_value(root, data3))
        {
                read_value(root, act_key3);
                cout<<"Value has been changed!\n"<<endl;
        }
	
	return(RUN_SUCCESS);
}

/*This is used to delete test*/
RUN_RESULT
run_delete_test(INDEX_NODE *root)
{
	/*Delete Test 1*/
	int key1 = (int)(rand()%KEY_RANGE);
	
	cout<<"Delete test1:\n"<<"Delete by using a random key: "<<key1
                <<"\nThis key may not exsit.\n"<<endl;
	
	if(!(delete_node(root, key1)))
	{
		cout<<"Non-exisit value cannot be deleted!\n"<<endl;
	}

	/*Delete Test 2*/
        int key2 = -782;

	cout<<"Delete test2:\n"<<"Delete by using an invalid key: "<<key2
                <<"\nAn error will be reported!\n"<<endl;

        if(!(delete_node(root, key2)))
        {
		cout<<"No value deleted!\n"<<endl;
	}

	/*Delete Test 3*/
        int near_key3 = (int)(rand()%(KEY_RANGE/10)), act_key3;
        act_key3 = produce_actual_key(root, near_key3);

	cout<<"Delete test3:\n"<<"Delete by using key: "<<act_key3
		<<"\nDelete this data will lead node merge.\n";
	cout<<"Read the original data:\n"<<endl;
	read_value(root, act_key3);
	cout<<"Delete the data.\n";
	if(delete_node(root, act_key3))
	{
		cout<<"Delete the data successfully!\n"
			<<"Read the data again.\n"<<endl;
		read_value(root, act_key3);
		
		cout<<"Draw the tree.\n"<<endl;
		draw_a_tree(root);
	}
	else
	{
		cout<<"Delete failed.\n"<<endl;
	}

	return(RUN_SUCCESS);
}

/*This is used to run insert test.*/
RUN_RESULT 
run_insert_test(INDEX_NODE *root)
{
	char insert_str[] = "Insert Value";
	int len = strlen(insert_str);

	/*Insert Test 1*/
        int key1 = (int)(rand()%KEY_RANGE);
	DATA_RECORD *data1 = create_data_record_mem();
	
	data1->key = key1;
	data1->value = create_n_byte_mem(len);
	data1->len = len;
	strcpy(data1->value, insert_str);
	
        cout<<"Insert test1:\n"<<"Insert data with key: "<<key1
                <<"  value: "<<data1->value<<endl;
	cout<<"Read the data first:\n"<<endl;
	read_value(root, key1);
	cout<<"Data doesn't exist.\n"<<"Now insert the data."<<endl;

	if(insert_node(root, data1))
        {
                cout<<"Insert successfully. Read the data again!"<<endl;
		read_value(root, key1);
		draw_a_tree(root);
        }
	
        /*Insert Test 2*/
        int key2 = -782;
	DATA_RECORD *data2 = create_data_record_mem();

        data2->key = key2;
	data2->value = create_n_byte_mem(len);
	data2->len = len;;
        strcpy(data2->value, insert_str);

        cout<<"Insert test2:\n"<<"Insert data with key: "<<key2
		<<"  value: "<<data2->value
                <<"\nAn error will be reported!\n"<<endl;

        if(!(insert_node(root, data2)))
        {
                cout<<"No value inserted!\n"<<endl;
        }

	free(data2->value);
	data2->value = NULL;
	free(data2);
	data2 = NULL;

        /*Insert Test 3*/
        int near_key3 = (int)(rand()%(KEY_RANGE/10)), act_key3;
        act_key3 = produce_actual_key(root, near_key3);
	
	DATA_RECORD *data3 = create_data_record_mem();
        data3->key = act_key3;
	data3->value = create_n_byte_mem(len);
	data3->len = len;
        strcpy(data3->value, insert_str);

	cout<<"Insert test3:\n"<<"Insert data with key: "<<data3->key
                <<"  value: "<<data3->value<<"\nSince key already exists,"
                <<" an error will be reported!\n"<<endl;

        if(!(insert_node(root, data3)))
        {
                cout<<"No value inserted!\n"<<endl;
        }

	/*Insert Test 4*/
	int key4 = KEY_RANGE - 10;
	int key5 = KEY_RANGE - 12;
	int key6 = KEY_RANGE - 8;
	int key7 = KEY_RANGE - 14;

	DATA_RECORD *data4 = create_data_record_mem();
	DATA_RECORD *data5 = create_data_record_mem();
	DATA_RECORD *data6 = create_data_record_mem();
	DATA_RECORD *data7 = create_data_record_mem();

	data4->key = key4;
	data5->key = key5;
	data6->key = key6;
	data7->key = key7;
	
	data4->len = len;
	data5->len = len;
	data6->len = len;
	data7->len = len;
	
	data4->value = create_n_byte_mem(len);
	data5->value = create_n_byte_mem(len);
	data6->value = create_n_byte_mem(len);
	data7->value = create_n_byte_mem(len);
	
	strcpy(data4->value, insert_str);
	strcpy(data5->value, insert_str);
	strcpy(data6->value, insert_str);
	strcpy(data7->value, insert_str);

	cout<<"Insert test4:\n"<<"Insert 4 data.\n"
	<<"Insert 1st with key: "<<key4<<"  value: "<<data4->value<<endl;
	
	if(insert_node(root, data4))
	{
		cout<<"Insert successfully.\n"<<endl;
		cout<<"Insert this key may change pri_key. So draw the tree."<<endl;
		draw_a_tree(root);
	}
	
        cout<<"Insert 2nd with key: "<<key5<<"  value: "<<data5->value<<endl;

        if(insert_node(root, data5))
        {
                cout<<"Insert successfully.\n"<<endl;
        }

	cout<<"Insert 3rd with key: "<<key6<<"  value: "<<data6->value<<endl;

        if(insert_node(root, data6))
        {
                cout<<"Insert successfully.\n"<<endl;
        }

	cout<<"Insert 4th with key: "<<key7<<"  value: "<<data7->value<<endl;

        if(insert_node(root, data7))
        {
                cout<<"Insert successfully.\n"<<endl;
		cout<<"Insert these 4 data may lead node split.\n"
			<<"So draw the tree again.\n"<<endl;
		draw_a_tree(root);
        }

	return(RUN_SUCCESS);
}

/*This is used to produce an available key using random data.*/
int
produce_actual_key(INDEX_NODE *root, int num)
{
	LEAF_NODE *res;
	res = search_near_leaf(root, num, SUB_MARK);
	if(res)
	{
		return(res->data_record->key);
	}
	else
	{
		return(0);
	}
}

/*Generate a random data record list*/
DATA_RECORD_LIST *
generate_random_data(int data_num)
{
	DATA_RECORD_LIST *ret_list, *cur_data, *new_data;
	DATA_RECORD *new_record;
	
	cur_data = create_data_record_list_mem();
	new_record = create_data_record_mem();
	
	new_record->len = 8;
	new_record->value = create_n_byte_mem(8);
	new_record->key = (int)(rand()%KEY_RANGE);
	sprintf(new_record->value, "%x", rand());

	cur_data->data_record = new_record;
	cur_data->next_data = NULL;
	ret_list = cur_data;

	int len;

	for(int i = 1; i != data_num; i++)
	{
		new_data = create_data_record_list_mem();
		new_record = create_data_record_mem();

		new_record->key = (int)(rand()%KEY_RANGE);
		len = 8;
		new_record->len = len;
		new_record->value = create_n_byte_mem(8);
        	
		sprintf(new_record->value, "%d", (10000000+(int)(rand()%90000000)));


		new_data->data_record = new_record;
		new_data->next_data = NULL;
		cur_data->next_data = new_data;
		cur_data = cur_data->next_data;
	}

	test_list(ret_list, data_num, OFF);	
	return(ret_list);
}

/*Draw a tree.*/
void
draw_a_tree(INDEX_NODE *root)
{
	int node_level = 0;
	draw_nodes(root, node_level);
}

/*Draw the nodes in the tree.*/
void
draw_nodes(INDEX_NODE *node, int node_level)
{
	int num_keys = (int)(node->key_num);
	cout<<" Level: "<<node_level<<" Pri_key: "<<node->pri_key<<" Keys: ";
	for(int key_idx = 0; key_idx != num_keys; ++key_idx)
	{
		cout<<node->key_list[key_idx]<<" ";
	}
	cout<<"\n"<<endl;
	for(int pointer_idx = 0; pointer_idx != num_keys + 1; ++pointer_idx)
	{
		INDEX_NODE *next_node = node->p_node[pointer_idx];

		if(is_leaf_node(next_node))
		{
			draw_a_leaf((LEAF_NODE *)next_node, node_level + 1);
		}
		else
		{
			draw_nodes(node->p_node[pointer_idx], node_level + 1);
		}
	}
}

/*This is used for drawing a single leaf.*/
void
draw_a_leaf(LEAF_NODE *node, int node_level)
{
	cout<<" Level: "<<node_level<<" Pri_key: "<<node->pri_key<<" Key: "
                        <<node->data_record->key<<" Value: "
                                <<node->data_record->value<<"\n"<<endl;
	return;
}

/*Used for drawing leaf nodes linked list.*/
void
draw_all_rest_leaves(LEAF_NODE *begin_node, SEARCH_DIR direction)
{
	execute_draw_leaves(begin_node, ALL_REST_LEAVES, direction);
}

/*Draw a fixed length of nodes*/
void 
draw_leaves_list(LEAF_NODE *begin_node, int length, SEARCH_DIR direction)
{	
	int drawn_nodes = 0;
	if(length <= 0)
	{
		cout<<"Length must be a positive integer!\n"<<endl;
		return;
	}
	execute_draw_leaves(begin_node, length, direction);
	return;
}

/*Execute draw leaves.*/
void
execute_draw_leaves(LEAF_NODE *begin_node, int length, SEARCH_DIR direction)
{
        int drawn_nodes = 0, nodes_in_line = 0;
        if(direction == BACKWARD)
        {
                while(begin_node)
                {
                        cout<<" Pri_key: "<<begin_node->pri_key<<
				" Key: "<<begin_node->data_record->key<<" Value: "
                                <<begin_node->data_record->value<<"----->";
                        nodes_in_line ++;
                        drawn_nodes ++;
                        begin_node = begin_node->back_node;
                        if(nodes_in_line == 5)
                        {
                                cout<<"\n";
                                nodes_in_line = 0;
                        }
			if(drawn_nodes == length)
			{
				break;
			}
                }
        }
        else
        {
                while(begin_node)
                {
                        cout<<" Pri_key: "<<begin_node->pri_key<<
				" Key: "<<begin_node->data_record->key<<" Value: "
                                <<begin_node->data_record->value<<"<-----";
                        nodes_in_line ++;
                        drawn_nodes ++;
                        begin_node = begin_node->front_node;
                        if(nodes_in_line == 5)
                        {
                                cout<<"\n";
                                nodes_in_line = 0;
                        }
			if(drawn_nodes == length)
			{
				break;
			}
                }
        }
        cout<<"\n"<<drawn_nodes<<" nodes have been drawn!\n";
        return;
}

/*This is used for test by print.*/
void
for_test(int num)
{
	cout<<"Program has already run here. "<<"Tag: "<<num<<"\n"<<endl;
	return;
}

/*This function is used to check list.*/
void
test_list(DATA_RECORD_LIST *data_list, int length, ON_OFF on_off)
{
	if(!length)
	{
		return;
	}

	DATA_RECORD_LIST *cur_data;
	cur_data = data_list;
	if(on_off)
	{
		for(int i = 0; i != length; ++i)
		{
			cout<<cur_data->data_record->key<<" ";
			cur_data = cur_data->next_data;
		}
		cout<<endl;
	}
	return;
}

/*This function is used to check leaf link list.*/
void 
test_leaf_link(LEAF_NODE *begin_node, ON_OFF on_off)
{
	if(on_off)
	{
		execute_draw_leaves(begin_node, ALL_REST_LEAVES, BACKWARD );
	}
	return;
}

/*This function is used to check node info list.*/
void 
test_node_info(NODE_INFO *node_info, ON_OFF on_off)
{
	if(on_off)
	{
		NODE_INFO *cur_node;
		cur_node = node_info;
		while(cur_node)
		{
			cout<<"Node Addr: "<<cur_node->node_addr<<" Node Type: "
				<<cur_node->node_type<<"   ";
			cur_node = cur_node->next_node_info;
		}
	}
	return;
}

/*This function is used to check level info.*/
void
test_level_info(LEVEL_INFO *level_info, ON_OFF on_off)
{
	if(on_off)
        {
		cout<<"Node Number: "<< level_info->node_num<<endl;
		test_node_info(level_info->node_info_addr, ON);
        }
        return;
}
