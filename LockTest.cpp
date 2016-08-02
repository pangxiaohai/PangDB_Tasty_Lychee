#include<iostream>
#include"./BIndex.h"
#include<math.h>
#include<stdlib.h>
#include<time.h>
#include<sys/time.h>


using namespace std;


/*This is file is used for lock related test.*/

RUN_RESULT run_auto_clean_lock_test(INDEX_NODE *);
RUN_RESULT run_lock_block_test(INDEX_NODE *);
void show_node_lock_status(NODE_ANALYZE_RES *);


/*This is used to run auto clean lock test.*/
RUN_RESULT 
run_auto_clean_lock_test(INDEX_NODE *root)
{
	/*Search some nodes.*/
	INDEX_NODE *cur_node;
	cur_node = root;
	int pos = 0;
	int key1 = (int)ceil(0.6*KEY_RANGE);
	while(!is_leaf_node(cur_node))
	{
		pos = fetch_pos(cur_node, key1);
		cur_node = cur_node->p_node[pos];
		apply_read_node_lock(321, cur_node);
	}

	cur_node = root;
	int key2 = (int)ceil(0.5*KEY_RANGE);
	while(!is_leaf_node(cur_node))
        {
                pos = fetch_pos(cur_node, key2);
                cur_node = cur_node->p_node[pos];
                apply_read_node_lock(322, cur_node);
        }
	
	
	cout<<"Following are all lock applied:"<<endl;
	show_all_lock_record();
	cout<<"Wait 2 sec. And check the lock record again."<<endl;
	sleep(2);
	show_all_lock_record();
	/*For Debug.*/
	//show_all_lock_info();
	return(RUN_SUCCESS);
}

/*This is used to test lock block behavior.*/
RUN_RESULT
run_lock_block_test(INDEX_NODE *root)
{
	NODE_ANALYZE_RES *res = create_node_analyze_res_mem();
	res->leaf_link = create_single_leaf_link_mem();
	res->index_link = create_index_node_link_mem();
	INDEX_NODE_LINK *new_index, *cur_index;
	res->index_link->index_node = root;
	res->index_link->next_link = NULL;
	res->index_link->front_link = NULL;
	cur_index = res->index_link;

	cout<<"Apply some read locks for user 100 firstly: "<<endl;
	INDEX_NODE *cur_node;
        cur_node = root;
        int pos = 0;
        int key1 = (int)ceil(0.6*KEY_RANGE);
        while(!is_leaf_node(cur_node))
        {
		if(cur_node != root)
		{
			new_index = create_index_node_link_mem();
			new_index->index_node = cur_node;
			new_index->front_link = cur_index;
			new_index->next_link = NULL;
			cur_index->next_link = new_index;
			cur_index = cur_index->next_link;
		}

                pos = fetch_pos(cur_node, key1);
                cur_node = cur_node->p_node[pos];
                apply_read_node_lock(100, cur_node);
        }

	res->leaf_link->leaf_node = (LEAF_NODE *)cur_node;
	res->leaf_link->next_link = NULL;
	
	cout<<"Following are current lock records:"<<endl;
	show_all_lock_record();
	
	cout<<"Try to apply these nodes' write locks for another user 101:"<<endl;
	if(!apply_write_links_lock(101, res))
	{
		cout<<"Apply write locks failed!"<<endl;
	}
	else
	{
		cout<<"Test failed!"<<endl;
		free_node_analyze_res_mem(res);
        	res = NULL;
		return(RUN_FAILED);
	}

	cout<<"Following are current lock records:"<<endl;
	show_all_lock_record();
	/*For Debug.*/
	//show_all_lock_info();
	//show_node_lock_status(res);

	cout<<"Wait 2 sec. Then apply again."<<endl;
	sleep(2);
	//show_node_lock_status(res);
	
	/*For Debug.*/
	//show_all_lock_info();
	if(apply_write_links_lock(101, res))
        {
                cout<<"Apply write locks successed!"<<endl;
        }
	else
        {
                cout<<"Test failed!"<<endl;
                free_node_analyze_res_mem(res);
                res = NULL;
                return(RUN_FAILED);
        }
	
	cout<<"Following are current lock records:"<<endl;
        show_all_lock_record();

	cout<<"Free these write locks.\n"<<endl;
	free_write_links_lock(101, res);

	cout<<"Following are current lock records:"<<endl;
        show_all_lock_record(); 
	/*For Debug.*/
	//show_all_lock_info();
	//show_node_lock_status(res);

	cout<<"Then apply some read locks again."<<endl;
	cur_node = root;
	pos = 0;

	while(!is_leaf_node(cur_node))
        {
                pos = fetch_pos(cur_node, key1);
                cur_node = cur_node->p_node[pos];
                apply_read_node_lock(102, cur_node);
        }	

	cout<<"Following are current lock records:"<<endl;
        show_all_lock_record();
	//show_all_lock_info();

	cout<<"Then free these read locks."<<endl;
	free_read_links_lock(102, res);
	
	cout<<"Following are current lock records:"<<endl;
        show_all_lock_record();

	free_node_analyze_res_mem(res);
	res = NULL;
	
	return(RUN_SUCCESS);
}

/*This is used to show node lock status.*/
void
show_node_lock_status(NODE_ANALYZE_RES *res)
{
	SINGLE_LEAF_LINK *cur_leaf;
	INDEX_NODE_LINK *cur_index;
	cur_leaf = res->leaf_link;
	cur_index = res->index_link;

	while(cur_leaf)
	{
		cout<<"Leaf Node: "<<cur_leaf->leaf_node<<" Key: "<<cur_leaf->leaf_node->pri_key<<
			" Lock Type: "<<get_lock_type(cur_leaf->leaf_node->node_lock)<<endl;
		cur_leaf = cur_leaf->next_link;
	}

	while(cur_index)
        {
                cout<<"Index Node: "<<cur_index->index_node<<" Key: "<<cur_index->index_node->pri_key<<
                        " Lock Type: "<<get_lock_type(cur_index->index_node->node_lock)<<endl;
                cur_index = cur_index->next_link;
        }

	return;
}

