#include<iostream>
#include"./BIndex.h"
#include<math.h>
#include<stdlib.h>
#include<string>
#include<string.h>
#include<fstream>

using namespace std;


/*This file is used for operations.*/


NODE_PATH_INFO *scan_node(INDEX_NODE *, int);
LEAF_NODE *exec_read_value(INDEX_NODE *, int);
RUN_RESULT update_value(INDEX_NODE *, DATA_RECORD *);
NODE_PATH_INFO *search_special_leaf(INDEX_NODE *, FIRST_OR_LAST);
SINGLE_LEAF_LINK *exec_top_search(INDEX_NODE *, int, ASC_DSC);
SINGLE_LEAF_LINK *exec_bottom_search(INDEX_NODE *,int, ASC_DSC);
LEAF_NODE *search_near_leaf(INDEX_NODE *, int, SUB_UP);
DOUBLE_LEAF_LINK *generate_double_leaf_link(LEAF_NODE *, LEAF_NODE *);
DOUBLE_LEAF_LINK *exec_range_search(INDEX_NODE *, int, int);
NODE_ANALYZE_RES *analyze_influnce_nodes(NODE_PATH_INFO *, ACTION);
int fetch_pos(INDEX_NODE *, int);
INDEX_NODE *split_node(INDEX_NODE *, int, int *, INDEX_NODE **);
RUN_RESULT delete_node(INDEX_NODE *, int);
RUN_RESULT insert_node(INDEX_NODE *, DATA_RECORD *);
RUN_RESULT range_search(INDEX_NODE *, int, int, ASC_DSC);
RUN_RESULT top_search(INDEX_NODE *, int, ASC_DSC);
RUN_RESULT bottom_search(INDEX_NODE *, int, ASC_DSC);
RUN_RESULT delete_whole_tree(INDEX_NODE *);
RUN_RESULT read_value(INDEX_NODE *, int);
NODE_ANALYZE_RES *analyze_insert_influnce_nodes(NODE_PATH_INFO *, int);
IDX_BOOL need_modify_pri(INDEX_NODE *, int);
RUN_RESULT insert_to_file(char *, DATA_RECORD *);
RUN_RESULT delete_from_file(char *, DATA_RECORD *);
RUN_RESULT exec_delete_from_file(DATA_RECORD *, fstream &);
RUN_RESULT update_to_file(char *, DATA_RECORD *);
void exec_insert_index_node(INDEX_NODE *, INDEX_NODE_LINK *, INDEX_NODE *);
RUN_RESULT insert_sub_tree(INDEX_NODE *, INDEX_NODE *, INDEX_NODE *);
void exec_delete_a_sub_tree(INDEX_NODE *, INDEX_NODE_LINK *, IDX_BOOL, 
		INDEX_NODE *, LEAF_NODE *, LEAF_NODE *, LEAF_NODE *);
INDEX_NODE *batch_insert(INDEX_NODE *, DATA_RECORD_LIST *);
INDEX_NODE *bactch_delete(INDEX_NODE *, int, int);
SUB_TREE_LIST_INFO *get_sub_tree_info(INDEX_NODE *, LEAF_NODE *, LEAF_NODE *);
SUB_TREE_INFO_LINK *analyze_influence_sub_tree(INDEX_NODE *, INDEX_NODE *, int, int);
LEAF_NODE *quick_search_special(INDEX_NODE *, FIRST_OR_LAST);
void exec_delete_sub_trees(INDEX_NODE *, SUB_TREE_LIST_INFO *);
NODE_PATH_INFO *fetch_index_path(INDEX_NODE *, INDEX_NODE *);
DATA_INFO *fetch_leaf_list_data_info(LEAF_NODE *, LEAF_NODE *);



/*This is used to read value.*/
RUN_RESULT
read_value(INDEX_NODE *root, int key)
{
	LEAF_NODE *res;
	res = exec_read_value(root, key);
	if(!res)
	{
		cout<<"No value with key: "<<key<<" found!\n"<<endl;
		return(RUN_FAILED);
	}
	else
	{
		cout<<"Key: "<<key<<" Value: "<<res->data_record->value<<"\n"<<endl;
		return(RUN_SUCCESS);
	}
}

/*This is used to execute read value from a tree with related key.*/
LEAF_NODE *
exec_read_value(INDEX_NODE *root, int key)
{
        if((key <= 0) || (key > KEY_RANGE))
        {
		cout<<"Wrong key!\n"<<endl;
                return(NULL);
        }

	NODE_PATH_INFO *node_path_info;
	node_path_info = scan_node(root, key);
	LEAF_NODE *ret;

	//free_read_locks(leaf_path->node_path);

	//if(!leaf_path->status)
	//{
	//	cout<<"Apply lock fail!\n"<<endl;
	//	return;
	//}

	if(node_path_info->is_include_leaf)
	{
		ret = node_path_info->end_leaf;

		/*Wrong node found*/
		if(ret->data_record->key != key)
		{
			cout<<"No value found!\n"<<endl;
			ret = NULL;
		}
	}
	else
	{
		cout<<"No value found!\n"<<endl;
		ret = NULL;
	}

	free_node_path_info_mem(node_path_info);
	return(ret);
}

/*This is used to scan a node.*/
NODE_PATH_INFO *
scan_node(INDEX_NODE *root, int key)
{
        if(key <= 0)
        {
                cout<<"Wrong key!\n"<<endl;
                return(NULL);
        }

	INDEX_NODE_LINK *index_node_link, *new_index_node;
	INDEX_NODE *current = root;
	NODE_PATH_INFO *retval;
	int i, next_set = 0;

	//APPLY LOCK HERE!
	//NEED TO ADD CODE!

	index_node_link = create_index_node_link_mem();
	index_node_link->index_node = root;
	index_node_link->front_link = NULL;
	index_node_link->next_link = NULL;

	retval = create_node_path_info_mem();
	retval->index_node_link = index_node_link;
	retval->is_include_leaf = FALSE;
	retval->status = RUN_FAILED;

	int key_num;

	key_num = current->key_num;
	for(i = 0; i != key_num; ++i)
	{
 		if(current->key_list[i] >= key)
		{
			current = current->p_node[i];
			next_set = 1;
			break;
		}
	}
	if(!next_set)
	{
		current = current->p_node[key_num];
	}

	while(!is_leaf_node(current))
	{
		//if(!apply_read_lock(current))
		//{
		//	retval->status = RUN_FAILED;
		//	return(retval);
		//}

		next_set = 0;
		new_index_node = create_index_node_link_mem();
        	new_index_node->index_node = current;
        	new_index_node->next_link = NULL;
		new_index_node->front_link = index_node_link;
		
		index_node_link->next_link = new_index_node;
		index_node_link = index_node_link->next_link;

		key_num = current->key_num;	
		for(i = 0; i != key_num; ++i)
		{
			if(current->key_list[i] >= key)
			{
				current = current->p_node[i];
				next_set = 1;
				break;
			}
		}
		if(!next_set)
		{
			current = current->p_node[key_num];
		}
	}

	//if(!apply_leaf_lock(current))
        //       {
        //                retval->status = RUN_FAILED;
        //                return(retval);
        //        }

	retval->end_leaf = (LEAF_NODE *)current;
	retval->status = RUN_SUCCESS;
	retval->is_include_leaf = TRUE;
	

	return(retval);
}

/*This function is used for update a value.*/
/*Update log will be written.*/
RUN_RESULT 
update_value(INDEX_NODE *root, DATA_RECORD *data_record)
{
	int search_key = data_record->key;
	if(search_key <= 0)
        {
                cout<<"Wrong key!\n"<<endl;
                return(RUN_FAILED);
        }

	NODE_PATH_INFO *node_path;
	node_path = scan_node(root, search_key);
	
	if(!node_path->is_include_leaf)
	{
		cout<<"No value with key: "<<search_key<<" found!\n"<<endl;
		//free_read_lock(node_path);
		free_node_path_info_mem(node_path);
		return(RUN_FAILED);
	}

	/*Wrong node found!*/
	if(node_path->end_leaf->data_record->key != search_key)
	{
		cout<<"No value with key: "<<search_key<<" found!\n"<<endl;
                //free_read_lock(node_path);
                free_node_path_info_mem(node_path);
                return(RUN_FAILED);
	}

	if(!strcmp(node_path->end_leaf->data_record->value, data_record->value))
	{
		cout<<"Value has not been changed!\n"<<endl;
		//free_read_lock(node_path);
		free_node_path_info_mem(node_path);
		return(RUN_FAILED);
	}
	
	/*Analyze and apply write lock.*/
	//NODE_ANALYZE_RES *write_lock_analyze = analyze_influnce_nodes(node_path, UPDATE);
	
	/*Will free some write lock here*/
	/*
	if(apply_write_node_lock(write_lock_analyze->index_link) && 
			apply_write_leaf_lock(write_lock_analyze->leaf_link))
	{
		cout<<"Apply write lock failed!\n"<<endl;
        	free_read_lock(node_path);
        	return(RUN_FAILED);
	}
	*/

	/*Write log*/
	//if(!write_log(UPDATE, data_record))
	//{
	//	cout<<"Write log fail!\n"<<endl;
	//	return(RUN_FAILED);
	//}

	/*Do modification*/
	node_path->end_leaf->data_record->len = data_record->len;
	free(node_path->end_leaf->data_record->value);
	node_path->end_leaf->data_record->value = create_n_byte_mem(data_record->len);
	strcpy(node_path->end_leaf->data_record->value, data_record->value);

	exec_write_log(FAKE_PID, UPDATE, data_record);
	
	/*Free write lock*/
	//free_write_node_lock(write_lock_analyze->index_link);
	//free_write_leaf_lock(write_lock_analyze->leaf_link);

	free_node_path_info_mem(node_path);
	//free_node_analyze_res_mem(write_lock_analyze);
	
	return(RUN_SUCCESS);
}

/*This is used for searching the first leaf or the last leaf.*/
NODE_PATH_INFO *
search_special_leaf(INDEX_NODE *root, FIRST_OR_LAST fir_last)
{
	NODE_PATH_INFO *ret_node_path = create_node_path_info_mem();
	INDEX_NODE_LINK *index_node_link = create_index_node_link_mem();

	/*
	if(!apply_read_lock(root))
	{
		cout<<"ALl data have been locked!\n"<<endl;
		
		free(ret_node_path);
		free(index_node_link);
		ret_node_path = NULL;
		index_node_link = NULL;

		return(NULL);
	}
	*/
	
	ret_node_path->index_node_link = index_node_link;
	ret_node_path->is_include_leaf = FALSE;
	ret_node_path->status = RUN_FAILED;
	ret_node_path->end_leaf = NULL;

	index_node_link->index_node = root;
	index_node_link->front_link = NULL;
	index_node_link->next_link = NULL;
	
	INDEX_NODE *cur_node = create_index_mem();

	/*Search the first node or last node.*/
	if(fir_last == FIRST_NODE)
	{
		cur_node = root->p_node[0];
	}
	else
	{
		int key_num = root->key_num;
		cur_node = root->p_node[key_num];
	}
	
	while(!is_leaf_node(cur_node))
	{
		/*
		if(!apply_read_lock(cur_node))
		{
			cout<<"Some data can not get!\n"<<endl;
			free_read_lock(ret_node_path);

			free(ret_node_path);
                	free(index_node_link);
               		ret_node_path = NULL;
                	index_node_link = NULL;

                	return(NULL);
		}
		*/

		INDEX_NODE_LINK *new_node_link;
		new_node_link = create_index_node_link_mem();
		new_node_link->index_node = cur_node;
		new_node_link->next_link = NULL;
		new_node_link->front_link = index_node_link;
		
		index_node_link->next_link = new_node_link;
		index_node_link = index_node_link->next_link;
		
		if(fir_last == FIRST_NODE)
		{
			cur_node = cur_node->p_node[0];
		}
		else
		{
			int key_num = cur_node->key_num;
			cur_node = cur_node->p_node[key_num];
		}
	}
	
	ret_node_path->end_leaf = (LEAF_NODE *)cur_node;
	ret_node_path->is_include_leaf = TRUE;
	ret_node_path->status = RUN_SUCCESS;

	return(ret_node_path);
}

/*This is used for TOP research*/
RUN_RESULT 
top_search(INDEX_NODE *root, int num, ASC_DSC asc_dsc)
{
	SINGLE_LEAF_LINK *res;
	
	res = exec_top_search(root, num, asc_dsc);
	if(!res)
	{
		return(RUN_FAILED);
	}
	
	int res_num = 0;
	SINGLE_LEAF_LINK *cur;
	cur = res;

	while(cur)
	{
		cout<<"Key: "<<cur->leaf_node->data_record->key<<"   "
			<<"Value: "<<cur->leaf_node->data_record->value
			<<"   ";
		res_num ++;
		cur = cur->next_link;

		if(!(res_num%5))
		{
			cout<<"\n";
		}
	}

	cout<<"\nTotal: "<<res_num<<" records found!\n";

	if(res_num<num)
	{
		cout<<"Not enough records!\n";
	}
	cout<<endl;
	return(RUN_SUCCESS);
}

/*This is used to execute TOP operation.*/
SINGLE_LEAF_LINK *
exec_top_search(INDEX_NODE *root, int num, ASC_DSC asc_dsc)
{
	NODE_PATH_INFO *node_path;
	node_path = search_special_leaf(root, LAST_NODE);
	
	if(node_path->status == RUN_FAILED)
	{
		//free_read_lock(node_path);
		free_node_path_info_mem(node_path);
		cout<<"Search fail!\n"<<endl;
		return(NULL);
	}

	LEAF_NODE *cur_node = node_path->end_leaf;

	free_node_path_info_mem(node_path);
	//free_read_lock_except_leaf(node_path);
	
	SINGLE_LEAF_LINK *cur_link, *ret_link;
	cur_link = create_single_leaf_link_mem();
	cur_link->next_link = NULL;
	
	ret_link = cur_link;

	int node_num = 0;

	if(asc_dsc == DSC)
	{	
		ret_link->leaf_node = cur_node;
		node_num = 1;
		cur_node = cur_node->front_node;
		
		while(cur_node && (node_num != num))
		{
			/*
			if(!apply_leaf_read_lock(cur_node))
			{
				free_leaf_read_lock(ret_link);
				cout<<"Some data can not be read!\n"<<endl;
				free(cur_link);
				cur_link = NULL:
				cur_node = NULL;
				ret_link = NULL;
				
				return(NULL);
			}
			*/

			SINGLE_LEAF_LINK *new_link;
			new_link = create_single_leaf_link_mem();
			new_link->next_link = NULL;
			new_link->leaf_node = cur_node;
			cur_link->next_link = new_link;

			cur_link = cur_link->next_link;
			node_num ++;
			cur_node = cur_node->front_node;
		}

		if(node_num < num)
		{
			return(ret_link);
		}
		
		//free_leaf_read_lock(ret_link);

		return(ret_link);
	}
	else
	{
		int mark = 0;
		while((mark != num) && (cur_node->front_node))
		{
			/*
                        if(!apply_leaf_read_lock(cur_node))
                        {
                                free_leaf_read_lock(ret_link);
                                cout<<"Some data can not be read!\n"<<endl;
                                free(cur_link);
                                cur_link = NULL:
                                cur_node = NULL;
                                ret_link = NULL;
                                
                                return(NULL);
                        }
                        */
			cur_node = cur_node->front_node;
			mark ++;
		}
		
		ret_link->leaf_node = cur_node;
                node_num = 1;
                cur_node = cur_node->back_node;

                while(cur_node)
                {
                        SINGLE_LEAF_LINK *new_link;
                        new_link = create_single_leaf_link_mem();
                        new_link->next_link = NULL;
                        new_link->leaf_node = cur_node;
                        cur_link->next_link = new_link;

                        cur_link = cur_link->next_link;
                        cur_node = cur_node->back_node;
                }

                //free_leaf_read_lock(ret_link);

                return(ret_link);
	}
}

/*This is used for Bottom research*/
RUN_RESULT
bottom_search(INDEX_NODE *root, int num, ASC_DSC asc_dsc)
{
        SINGLE_LEAF_LINK *res;

        res = exec_bottom_search(root, num, asc_dsc);
        if(!res)
        {
                return(RUN_FAILED);
        }

        int res_num = 0;
        SINGLE_LEAF_LINK *cur;
        cur = res;

        while(cur)
        {
                cout<<"Key: "<<cur->leaf_node->data_record->key<<"   "
                        <<"Value: "<<cur->leaf_node->data_record->value
                        <<"   ";
                res_num ++;
                cur = cur->next_link;

                if(!(res_num%5))
                {
                        cout<<"\n";
                }
        }

        cout<<"\nTotal: "<<res_num<<" records found!\n";

        if(res_num<num)
        {
                cout<<"Not enough records!\n";
        }
        cout<<endl;
        return(RUN_SUCCESS);
}

/*This is used to execute BOTTOM value search.*/
SINGLE_LEAF_LINK *
exec_bottom_search(INDEX_NODE *root,int num, ASC_DSC asc_dsc)
{
        NODE_PATH_INFO *node_path;
        node_path = search_special_leaf(root, FIRST_NODE);

        if(node_path->status == RUN_FAILED)
        {
                //free_read_lock(node_path);
                cout<<"Search fail!\n"<<endl;
                return(NULL);
        }

        LEAF_NODE *cur_node = node_path->end_leaf;

        free_node_path_info_mem(node_path);
        //free_read_lock_except_leaf(node_path);

        SINGLE_LEAF_LINK *cur_link, *ret_link;
        cur_link = create_single_leaf_link_mem();
        cur_link->next_link = NULL;

        ret_link = cur_link;

        int node_num = 0;

        if(asc_dsc == ASC)
        {
                ret_link->leaf_node = cur_node;
                node_num = 1;
                cur_node = cur_node->back_node;

                while(cur_node && (node_num != num))
                {
                        /*
                        if(!apply_leaf_read_lock(cur_node))
                        {
                                free_leaf_read_lock(ret_link);
                                cout<<"Some data can not be read!\n"<<endl;
                                free(cur_link);
                                cur_link = NULL:
                                cur_node = NULL;
                                ret_link = NULL;
                                
                                return(NULL);
                        }
                        */
                        SINGLE_LEAF_LINK *new_link;
                        new_link = create_single_leaf_link_mem();
                        new_link->next_link = NULL;
                        new_link->leaf_node = cur_node;
                        cur_link->next_link = new_link;

                        cur_link = cur_link->next_link;
                        node_num ++;
                        cur_node = cur_node->back_node;
                }

                if(node_num < num)
                {
                        return(ret_link);
                }

                //free_leaf_read_lock(ret_link);

                return(ret_link);
        }
        else
        {
                int mark = 1;
                while((mark != num) && (cur_node->back_node))
                {
                        /*
                        if(!apply_leaf_read_lock(cur_node))
                        {
                                free_leaf_read_lock(ret_link);
                                cout<<"Some data can not be read!\n"<<endl;
                                free(cur_link);
                                cur_link = NULL:
                                cur_node = NULL;
                                ret_link = NULL;
                                
                                return(NULL);
                        }
                        */

                        cur_node = cur_node->back_node;
                        mark ++;
                }

                ret_link->leaf_node = cur_node;
                node_num = 1;
                cur_node = cur_node->front_node;

                while(cur_node)
                {
                        SINGLE_LEAF_LINK *new_link;
                        new_link = create_single_leaf_link_mem();
                        new_link->next_link = NULL;
                        new_link->leaf_node = cur_node;
                        cur_link->next_link = new_link;

                        cur_link = cur_link->next_link;
                        cur_node = cur_node->front_node;
                }

                //free_leaf_read_lock(ret_link);

                return(ret_link);
        }
}

/*This is used for range search.*/
RUN_RESULT
range_search(INDEX_NODE *root, int low, int high, ASC_DSC asc_dsc)
{
	DOUBLE_LEAF_LINK *result_link;
	
	result_link = exec_range_search(root, low, high);

	if(!result_link)
	{
		return(RUN_FAILED);
	}
	
	DOUBLE_LEAF_LINK *cur_link = result_link;
	int num = 0;
	
	if(asc_dsc == ASC)
	{
		cout<<"Search result is: \n";
		while(cur_link)
		{
			cout<<"Key: "<<cur_link->leaf_node->data_record->key
				<<"  Value: "<<cur_link->leaf_node->data_record->value<<"  ";
			num ++;
			cur_link = cur_link->next_link;
			if(!(num%5))
			{
				cout<<"\n";
			}
		}
		cout<<"\nTotal: "<<num<<" records found.\n"<<endl;
	}
	else
	{
		while(cur_link->next_link)
		{
			cur_link = cur_link->next_link;
		}
		
		cout<<"Search result is: \n";
                while(cur_link)
                {
                        cout<<"Key: "<<cur_link->leaf_node->data_record->key
                                <<"  Value: "<<cur_link->leaf_node->data_record->value<<"  ";
                        num ++;
                        cur_link = cur_link->front_link;
                        if(!(num%5))
                        {
                                cout<<"\n";
                        }
                }
                cout<<"\nTotal: "<<num<<" records found.\n"<<endl;
	}

	free_double_leaf_link_mem(result_link);
	return(RUN_SUCCESS);
}


/*This is used for execute range search.*/
DOUBLE_LEAF_LINK *
exec_range_search(INDEX_NODE *root, int low, int high)
{
	if((low <= 0) || (high <= 0))
        {
                cout<<"Wrong key!\n"<<endl;
                return(NULL);
        }
	if(low > high)
	{
		cout<<"Invaild input!\n"<<endl;
		return(NULL);
	}
	else if(low == high)
	{
		NODE_PATH_INFO *result_node_path;
		result_node_path = scan_node(root, low);
		if(!result_node_path->is_include_leaf)
		{
			cout<<"No value found!\n"<<endl;
			//free_node_path_read_lock(result_node_path);
			free_node_path_info_mem(result_node_path);
			result_node_path = NULL;
			return(NULL);
		}
		else
		{
			DOUBLE_LEAF_LINK *ret;
			ret = create_double_leaf_link_mem();
			ret->leaf_node = result_node_path->end_leaf;
			ret->next_link = NULL;
			ret->front_link = NULL;
			
			//free_node_path_read_lock(result_node_path);
                        free_node_path_info_mem(result_node_path);
                        result_node_path = NULL;
			return(ret);
		}
	}
	else
	{
		LEAF_NODE *begin_node = search_near_leaf(root, low, SUB_MARK);
		LEAF_NODE *end_node = search_near_leaf(root, high, UP_MARK);
	
		if(!end_node)
		{
			//free_read_leaf_lock(begin_node);
			cout<<"No value can be found!\n"<<endl;
			return(NULL);
		}
	
		if(begin_node->data_record->key > end_node->data_record->key)
		{
			//free_read_leaf_lock(begin_node);
			//free_read_leaf_lock(end_node);
			cout<<"No value can be found!\n"<<endl;
			return(NULL);
		}
		
		DOUBLE_LEAF_LINK *ret;
		ret = generate_double_leaf_link(begin_node, end_node);
		//free_read_leaf_list_lock(ret);

		return(ret);
	}
}

/*This is used for fuzzy search a nearest leaf.*/
/*No index node lock will be applied, only leaf lock will be applied.*/
LEAF_NODE *
search_near_leaf(INDEX_NODE *root, int num, SUB_UP mark)
{
	INDEX_NODE *cur_node = root;
	int node_set;	

	while(!is_leaf_node(cur_node))
	{
		node_set  = 0;
		//apply_read_index_lock(cur_node);
		for(int i = 0; i != cur_node->key_num; ++i)
		{
			if(cur_node->key_list[i] >= num)
			{
				//free_read_index_lock(cur_node);
				cur_node = cur_node->p_node[i];
				node_set = 1;
				break;
			}
		}
		if(!node_set)
		{
			int key_num = cur_node->key_num;
			//free_read_index_lock(cur_node);
			cur_node = cur_node->p_node[key_num];
			node_set = 1;
		}
	}
	
	LEAF_NODE *mark_leaf;
	mark_leaf = (LEAF_NODE *)cur_node;

	if(mark == SUB_MARK)
	{
		if(num <= mark_leaf->data_record->key)
		{
			//apply_read_leaf_lock(mark_leaf);
			return(mark_leaf);
		}
		else
		{
			if(mark_leaf->back_node)
			{
				//apply_read_leaf_lock(mark_leaf->back_node);
                        	return(mark_leaf->back_node);
			}
			else
			{
				return(NULL);
			}
		}
	}
	else
	{	
		if(num >= mark_leaf->data_record->key)
		{
			//apply_read_leaf_lock(mark_leaf);
			return(mark_leaf);
		}
		else
		{
			if(mark_leaf->front_node)
			{
				//apply_read_leaf_lock(mark_leaf->front_node);
                        	return(mark_leaf->front_node);
			}
			else
			{
				return(NULL);
			}
		}
	}
}

/*This is used to generate a double leaf link.*/
DOUBLE_LEAF_LINK *
generate_double_leaf_link(LEAF_NODE *begin_node, LEAF_NODE *end_node)
{
	DOUBLE_LEAF_LINK *ret = create_double_leaf_link_mem();
	DOUBLE_LEAF_LINK *cur_link;

	ret->leaf_node = begin_node;
	ret->next_link = NULL;
	ret->front_link = NULL;
	
	cur_link = ret;

	LEAF_NODE *cur_leaf;
	cur_leaf = begin_node;

	while(cur_leaf != end_node)
	{
		cur_leaf = cur_leaf->back_node;

		DOUBLE_LEAF_LINK *new_link = create_double_leaf_link_mem();

		new_link->leaf_node = cur_leaf;
		new_link->next_link = NULL;
		new_link->front_link = cur_link;

		cur_link->next_link = new_link;
		cur_link = cur_link->next_link;
	}
	
	return(ret);
}

/*This is used to delete leaf.*/
RUN_RESULT 
delete_node(INDEX_NODE *root, int key)
{
        if(key <= 0)
        {
                cout<<"Wrong key!\n"<<endl;
                return(RUN_FAILED);
        }

	NODE_PATH_INFO *node_path_info = scan_node(root, key);
	
	if(!((node_path_info->status == RUN_SUCCESS) && 
		(node_path_info->end_leaf->data_record->key == key)))
	{
		/*No valid data node found!*/
		//free_read_lock(node_path_info);
		free_node_path_info_mem(node_path_info);
		cout<<"No data found!\n"<<endl;
		return(RUN_FAILED);
	}
	
	LEAF_NODE *target_leaf = node_path_info->end_leaf;

	NODE_ANALYZE_RES *analyze_res;
	analyze_res = analyze_influnce_nodes(node_path_info, DELETE);

	LEAF_NODE *front_leaf, *back_leaf;
	SINGLE_LEAF_LINK *end_link;
	front_leaf = analyze_res->leaf_link->leaf_node;
	end_link = analyze_res->leaf_link;
	
	while(end_link->next_link)
	{
		end_link = end_link->next_link;
	}
	back_leaf = end_link->leaf_node;

	/*Apply write locks.*/
	/*
	if(!(apply_write_index_list_lock(analyze_res->index_link) && 
		apply_write_leaf_list_lock(analyze->leaf_link)))
	{
		cout<<"Delete failed!\n"<<endl;
		free_read_lock(node_path_info);		

		free(target_leaf);
                free_node_analyze_res_mem(analyze_res);
                free_node_path_info_mem(node_path_info);

                target_leaf = front_leaf = back_leaf = NULL;
                analyze_res = NULL;
                node_path_info = NULL;
		return(RUN_FAILED);
	}
	
	free_read_lock(node_path_info);	
	*/

	/*If there is only one leaf left.*/
	if(front_leaf == back_leaf)
	{
		target_leaf->data_record->key = 0;
		target_leaf->data_record = NULL;

		free(target_leaf);
		free_node_analyze_res_mem(analyze_res);
		free_node_path_info_mem(node_path_info);
		
		target_leaf = front_leaf = back_leaf = NULL;
		analyze_res = NULL;
		node_path_info = NULL;
		
		return(RUN_SUCCESS);
	}

	INDEX_NODE_LINK *cur_linked_index;
	cur_linked_index = analyze_res->index_link;

	/*Get the index node linked to target leaf.*/
	while(cur_linked_index->next_link)
	{
		cur_linked_index = cur_linked_index->next_link;
	}
	
	INDEX_NODE *target_node = (INDEX_NODE *)target_leaf;
	exec_delete_a_sub_tree(root, cur_linked_index, FALSE, 
		target_node, front_leaf, back_leaf, target_leaf);

	exec_write_log(FAKE_PID, DELETE, node_path_info->end_leaf->data_record);
		
	/*
        free_write_index_lock(analyze_res->index_link);
        /How to deal with lost nodes?/
        free_write_leaf_lock(analyze_res->leaf_link);
        */

        free_node_analyze_res_mem(analyze_res);
        free_node_path_info_mem(node_path_info);
	analyze_res = NULL;
	node_path_info = NULL;
		
        return(RUN_SUCCESS);
}
 
/*This is used for insert a leaf.*/
RUN_RESULT 
insert_node(INDEX_NODE *root, DATA_RECORD *data_record)
{
	int key = data_record->key;
	if(key <= 0)
	{
		cout<<"Wrong key!\n"<<endl;
		return(RUN_FAILED);
	}

	NODE_PATH_INFO *node_path_info;
        node_path_info = scan_node(root, key);

	if(node_path_info->status != RUN_SUCCESS)
	{
		cout<<"Insert failed!\n"<<endl;
		free_node_path_info_mem(node_path_info);
		node_path_info = NULL;
		return(RUN_FAILED);
	}

	int find_key = node_path_info->end_leaf->pri_key;
	if(find_key == key)
	{
		cout<<"The key already exists!\n"<<endl;
		free_node_path_info_mem(node_path_info);
                node_path_info = NULL;
                return(RUN_FAILED);
	}

	NODE_ANALYZE_RES *analyze_res;
        analyze_res = analyze_insert_influnce_nodes(node_path_info, key);

	/*Apply write locks.*/
        /*
	if(!apply_write_index_list_lock(node_path_info->index_node_link)
		 && apply_write_leaf_list_lock(analyze->leaf_link)))
	{
                cout<<"Delete failed!\n"<<endl;
                free_read_lock(node_path_info);

                free(target_leaf);
                free_node_analyze_res_mem(analyze_res);
                free_node_path_info_mem(node_path_info);

                target_leaf = front_leaf = back_leaf = NULL;
                analyze_res = NULL;
                node_path_info = NULL;
                return(RUN_FAILED);
	}

        free_read_lock(node_path_info);
        */
	
	INDEX_NODE_LINK *cur_link = analyze_res->index_link;
	SINGLE_LEAF_LINK *leaf_link = analyze_res->leaf_link;

	int insert_pos, cur_key_num, i;
	
	LEAF_NODE *new_leaf = create_leaf_mem();
	new_leaf->data_record = data_record;
	new_leaf->node_type = LEAF_NODE_TYPE;
	new_leaf->node_lock = NO_LOCK;
	new_leaf->pri_key = key;
	
	/*Link the leaf*/
	LEAF_NODE *first = leaf_link->leaf_node;
	LEAF_NODE *second = leaf_link->next_link->leaf_node;
	
	if(!first)
	{
		second->front_node = new_leaf;
		new_leaf->back_node = second;
	}
	else if(!second)
	{
		first->back_node = new_leaf;
		new_leaf->front_node = first;
	}
	else
	{
		first->back_node = new_leaf;
                new_leaf->front_node = first;
		second->front_node = new_leaf;
                new_leaf->back_node = second;
	}
	
	INDEX_NODE *new_node;
	new_node = (INDEX_NODE *)new_leaf;

	while(cur_link->next_link)
	{
		cur_link = cur_link->next_link;
	}

	exec_insert_index_node(root, cur_link, new_node);

	exec_write_log(FAKE_PID, INSERT, data_record);

	/*
	free_write_index_lock(analyze_res->index_link);
        free_write_leaf_lock(analyze_res->leaf_link);
        */

        free_node_analyze_res_mem(analyze_res);
        free_node_path_info_mem(node_path_info);
       	analyze_res = NULL;
       	node_path_info = NULL;

        return(RUN_SUCCESS);	
}

/*This is used to split index node.*/
INDEX_NODE *
split_node(INDEX_NODE *target_node, int key_num, int *key_list, INDEX_NODE **p_list)
{
	int key_num_1 = (int)ceil(0.5*(key_num/2));
	int key_num_2 = key_num - key_num_1;
	int i;
	
	INDEX_NODE *new_node;
	new_node = create_index_mem();
	for(i = 0; i < key_num_1; ++i)
	{
		new_node->key_list[i] = key_list[i];
		new_node->p_node[i] = p_list[i];
	}
	new_node->p_node[key_num_1] = p_list[key_num_1];

	new_node->pri_key = key_list[key_num_1];
	new_node->node_type = INDEX_NODE_TYPE;
	new_node->node_lock = NO_LOCK;
	new_node->key_num = key_num_1;

	/*A key will be removed. It will be new node's pri_key*/
	for(i = 0; i< key_num_2; ++i)
	{
		target_node->key_list[i] = key_list[key_num_1 + i];
		target_node->p_node[i] = p_list[key_num_1 + i];
	}
	target_node->p_node[key_num_2] = p_list[key_num_1 + key_num_2];
	target_node->key_num = key_num_2;

	return(new_node);
}

/*This is used for analyzing influnced nodes.
But insert need to be treated specially.*/
NODE_ANALYZE_RES *
analyze_influnce_nodes(NODE_PATH_INFO *node_path, ACTION action)
{
	if(action == READ)
	{
		return(NULL);
	}
	
	NODE_ANALYZE_RES *ret = create_node_analyze_res_mem();
        SINGLE_LEAF_LINK *leaf_link = create_single_leaf_link_mem();
       	INDEX_NODE_LINK *index_link = create_index_node_link_mem();
	
	SINGLE_LEAF_LINK *cur_leaf_link = leaf_link;
	INDEX_NODE_LINK *cur_index_link = index_link;

	ret->leaf_link = leaf_link;
	ret->index_link = index_link;

	if(action == UPDATE)
	{
		leaf_link->leaf_node = node_path->end_leaf;
		leaf_link->next_link = NULL;
	
		INDEX_NODE_LINK *target_index_link;
		target_index_link = node_path->index_node_link;

		while(target_index_link->next_link)
		{
			target_index_link = target_index_link->next_link;
		}
		cur_index_link->index_node = target_index_link->index_node;
		cur_index_link->front_link = NULL;
		cur_index_link->next_link = NULL;
		
		return(ret);
	}
	
	if(action == DELETE)
	{
		LEAF_NODE *begin_leaf, *target_leaf, *end_leaf;
		target_leaf = node_path->end_leaf;
		begin_leaf = target_leaf;
		end_leaf = target_leaf;
		
		if(target_leaf->front_node)
		{
			begin_leaf = target_leaf->front_node;
		}

		if(target_leaf->back_node)
		{
			end_leaf = target_leaf->back_node;
		}
		
		leaf_link->leaf_node = begin_leaf;
		leaf_link->next_link = NULL;

		begin_leaf = begin_leaf->back_node;
		while(begin_leaf && (begin_leaf != end_leaf->back_node))
		{
			SINGLE_LEAF_LINK *new_leaf_link = create_single_leaf_link_mem();
			new_leaf_link->leaf_node = begin_leaf;
			new_leaf_link->next_link = NULL;
			
			cur_leaf_link->next_link = new_leaf_link;
			cur_leaf_link = cur_leaf_link->next_link;
			begin_leaf = begin_leaf->back_node;
		}

		/*At least last index node in node path will be influenced.*/
		INDEX_NODE_LINK *target_index_link;
                target_index_link = node_path->index_node_link;

                while(target_index_link->next_link)
                {
                        target_index_link = target_index_link->next_link;
                }
                cur_index_link->index_node = target_index_link->index_node;
                cur_index_link->front_link = NULL;
                cur_index_link->next_link = NULL;

		/*Need to mearge nodes.*/
		if(target_index_link->index_node->key_num - 1 < LEAST_P_NUM -1)
		{
			int key_to_move = target_index_link->index_node->key_num - 1;
			target_index_link = target_index_link->front_link;
			
			/*Check whether need to further merge.*/
			/*Each split or move will generate a new key.*/
			while(target_index_link && (key_to_move + 
					target_index_link->index_node->key_num > MAXKEYNUM))
			{
				INDEX_NODE_LINK *new_index_link = create_index_node_link_mem();
                        	new_index_link->index_node = target_index_link->index_node;
                        	new_index_link->front_link = NULL;
                        	new_index_link->next_link = cur_index_link;
                        	cur_index_link->front_link = new_index_link;
                        	cur_index_link = cur_index_link->front_link;

				target_index_link = target_index_link->front_link;

				/*Split will only generate a new key.*/
				key_to_move = 1;
			}

			/*Then last node will also be influenced.*/
			if(target_index_link)
			{
				INDEX_NODE_LINK *new_index_link = create_index_node_link_mem();
                                new_index_link->index_node = target_index_link->index_node;
                                new_index_link->front_link = NULL;
                                new_index_link->next_link = cur_index_link;
                                cur_index_link->front_link = new_index_link;
			}
		}
		
		return(ret);
	}

	return(NULL);
}

/*This used to analyze insert influence.
Since insert may change pri_key, it need to be considered specially.*/
NODE_ANALYZE_RES *
analyze_insert_influnce_nodes(NODE_PATH_INFO *node_path, int key)
{
	if((!node_path) || (!node_path->status))
	{
		return(NULL);
	}

	LEAF_NODE *find_leaf = node_path->end_leaf;

	if(find_leaf->pri_key == key)
	{
		return(NULL);
	}

	NODE_ANALYZE_RES *ret = create_node_analyze_res_mem();
        SINGLE_LEAF_LINK *leaf_link = create_single_leaf_link_mem();
        INDEX_NODE_LINK *index_link = create_index_node_link_mem();

        INDEX_NODE_LINK *cur_index_link = index_link;

        ret->leaf_link = leaf_link;
        ret->index_link = index_link;

	
	/*Analyze the leaf link.*/
	/*Since it is uncertain whether we should insert a leaf
	before the leaf find or after it, we should analyze it here.*/
	if(find_leaf->pri_key > key)
	{
		if(find_leaf->front_node)
		{
			leaf_link->leaf_node = find_leaf->front_node;
		}
		else
		{
			leaf_link->leaf_node = NULL;
		}
	
		SINGLE_LEAF_LINK *second_leaf = create_single_leaf_link_mem();
		leaf_link->next_link = second_leaf;
		second_leaf->leaf_node = find_leaf;
		second_leaf->next_link = NULL;
	}
	else
	{
		leaf_link->leaf_node = find_leaf;
		SINGLE_LEAF_LINK *second_leaf = create_single_leaf_link_mem();
                leaf_link->next_link = second_leaf;
		if(find_leaf->back_node)
		{
                	second_leaf->leaf_node = find_leaf->back_node;
		}
		else
		{
			second_leaf->leaf_node = NULL;
		}
                second_leaf->next_link = NULL;
	}
	
	/*Analyze the index link.*/
	INDEX_NODE_LINK *target_node_link;
        target_node_link = node_path->index_node_link;

        while(target_node_link->next_link)
        {
        	target_node_link = target_node_link->next_link;
        }

        cur_index_link->index_node = target_node_link->index_node;
        cur_index_link->front_link = NULL;
        cur_index_link->next_link = NULL;

        target_node_link = target_node_link->front_link;
        while(target_node_link &&
               ((target_node_link->index_node->key_num + 1> MAXKEYNUM)
		|| need_modify_pri(target_node_link->index_node, key)))
        {
		INDEX_NODE_LINK *new_index_link = create_index_node_link_mem();
                new_index_link->index_node = target_node_link->index_node;
                new_index_link->front_link = NULL;
                new_index_link->next_link = cur_index_link;
                cur_index_link->front_link = new_index_link;
                cur_index_link = cur_index_link->front_link;

                target_node_link = target_node_link->front_link;
        }

	return(ret);
}

/*This used to determine whether need to modify pri_key or not during insert.*/
IDX_BOOL
need_modify_pri(INDEX_NODE *node, int key)
{
	if(key > node->pri_key)
	{
		return(TRUE);
	}
	else
	{
		return(FALSE);
	}
}

/*This is used to fetch key position.*/
int
fetch_pos(INDEX_NODE *node, int key)
{
	int is_set = 0, ret, i;
	for(i = 0; i != node->key_num; ++i)
	{	
		if(node->key_list[i] >= key)
		{
			ret = i;
			is_set = 1;
			break;
		}
	}
	if(!is_set)
	{
		ret = node->key_num;
	}
	return(ret);

}

/*This is used to delete and free a whole tree.*/
RUN_RESULT
delete_whole_tree(INDEX_NODE *root)
{
	//There is a double free mem problem here
	//Need to debug
        //free_a_tree_mem(root);
        return(RUN_SUCCESS);
}

/*Following are operators to modify files.*/

/*This is used to insert a data record to the file.*/
RUN_RESULT
insert_to_file(char *filename, DATA_RECORD *data_record)
{
	/*Directly append data to the end of the file.*/
	
	ofstream file(filename, ios::app);
	if(!file)
	{
		cout<<"Cannot open backup file!\n"<<endl;
		return(RUN_FAILED);
	}

	char key_buf[10], len_buf[3];
	int value_len;

	sprintf(key_buf, "%10d", data_record->key);
	value_len = data_record->len;
        sprintf(len_buf,"%3d", value_len, 3);

	file.write(key_buf, 10);
	file.write(len_buf, 3);
	file.write(data_record->value, value_len);
	file.write(DATA_END, 8);

	file.clear();	
	file.close();
	
	return(RUN_SUCCESS);
}

/*This is used to delete a data record from the file.*/
RUN_RESULT
delete_from_file(char *filename, DATA_RECORD *data_record)
{
	fstream file(filename);
	if(!file)
        {
                cout<<"Cannot open backup file!\n"<<endl;
                return(RUN_FAILED);
        }

	if(!exec_delete_from_file(data_record, file))
	{
		file.clear();
		file.close();
		cout<<"Cannot find record!\n"<<endl;
		return(RUN_FAILED);
	}
	else
	{
		file.clear();
		file.close();
		return(RUN_SUCCESS);
	}
}

/*This is used to update a data record in file.*/
RUN_RESULT
update_to_file(char *filename, DATA_RECORD *data_record)
{
	fstream file(filename);
        if(!file)
        {
                cout<<"Cannot open backup file!\n"<<endl;
                return(RUN_FAILED);
        }
	
	if(exec_delete_from_file(data_record, file))
	{
		file.seekg(0,ios::end);
		char len_buf[3], key_buf[10];
        	int value_len;

	        sprintf(key_buf, "%10d", data_record->key);
        	value_len = data_record->len;
        	sprintf(len_buf,"%3d", value_len);

		file.write(key_buf,10);
		file.write(len_buf,3);
		file.write(data_record->value, value_len);
		file.write(DATA_END, 8);
		
		file.clear();
        	file.close();

        	return(RUN_SUCCESS);
	}
	else
	{
		return(RUN_FAILED);
	}
}

/*This is used to execute delete a data record from file.*/
RUN_RESULT
exec_delete_from_file(DATA_RECORD *data_record, fstream &file)
{
        char value_len[3], key_buf[10], end_buf[8];
        int key, delete_done = 0, len;

        while(!file.eof())
        {
		char *value_buf;
                file.read(key_buf, 10);
                file.read(value_len, 3);
                sscanf(value_len, "%d", len);
		value_buf = create_n_byte_mem(len);
                file.read(value_buf, len);
                file.read(end_buf, 8);

                sscanf(key_buf, "%d", key);
                if((key == data_record->key)
                        && (strcmp(end_buf, INVALID)))
                {
                        file.seekg(-8, ios::cur);
                        file.write(INVALID, 8);
                        delete_done = 1;
                        break;
                }
		free(value_buf);
		value_buf = NULL;
        }

        if(!delete_done)
	{
		return(RUN_FAILED);
	}
	else
	{
		return(RUN_SUCCESS);
	}
}

/*This is used to do batch insert.*/
/*Consider lock whole tree during batch insert and delete*/
INDEX_NODE *
batch_insert(INDEX_NODE *root, DATA_RECORD_LIST *data_list)
{
	DATA_INFO *data_info = get_data_info(data_list);
	
	if(!data_info->num)
	{
		cout<<"No avalible data!"<<endl;
		return(NULL);
	}

	/*Sort the data*/
	DATA_RECORD_LIST *sorted_list;
	sorted_list = quick_sort(data_list, data_info->num);

	if(check_duplicate_data(sorted_list, SORT))
	{
		cout<<"Duplicated keys!\n"<<endl;
		return(NULL);
	}

	int low = data_info->min_key;
	int high = data_info->max_key;

	if(low == high)
	{
		/*Only onew data insert.*/
		insert_node(root, data_list->data_record);
	}

	/*Analyze and lock leaf link list*/
	LEAF_NODE *begin_leaf, *end_leaf;
	begin_leaf = search_near_leaf(root, low, SUB_MARK);
	end_leaf = search_near_leaf(root, high, UP_MARK);

	SUB_TREE_LIST_INFO *sub_tree_list_info;
	sub_tree_list_info = get_sub_tree_info(root, begin_leaf, end_leaf);

	/*
	if(!apply_write_sub_tree_lock(sub_tree_list_info->sub_tree_info_link)) 
	{
		cout<<"Apply write lock failed.\n"<<endl;
		free_sub_tree_list_info_mem(sub_tree_list_info);
		sub_tree_list_info = NULL;
		return(RUN_FAILED);
	}
	*/

	int leaf_num = sub_tree_list_info->leaf_num + data_info->num;
	DATA_RECORD_LIST *new_data_list;
	new_data_list = merge_sort(sub_tree_list_info->data_list, sorted_list);

	/*Need not to use mk_index since it will sort data list again.*/
	LEAF_NODE *new_leaf_list = link_list(new_data_list, leaf_num);
	
	INDEX_NODE *new_sub;
	new_sub = generate_non_leaf(new_leaf_list, leaf_num);	

	/*The whole tree is covered, just replace it.*/
	if((!(begin_leaf->front_node)) && (!(end_leaf->back_node)))
        {
		free_a_tree_mem(root);
		root = NULL;
		return(new_sub);
	}
	
	/*Execute delete theses sub trees*/
	exec_delete_sub_trees(root, sub_tree_list_info);

	free_sub_tree_list_info_mem(sub_tree_list_info);
	sub_tree_list_info = NULL;

	NODE_PATH_INFO *new_sub_path = scan_node(root, new_sub->pri_key);
	
	/*Execute insert the new sub tree.*/
	INDEX_NODE_LINK *new_place_link = new_sub_path->index_node_link;
	while(new_place_link->next_link)
	{
		new_place_link = new_place_link->next_link;
	}

	exec_insert_index_node(root, new_place_link, new_sub);

	/*Need to re-balance the whole tree after batch delete.*/
        //re_balance_tree(root);

	//free lock, need to be design carefully here.*/
	return(root);
}

/*This is used to execute delete sub trees one by one.*/
void
exec_delete_sub_trees(INDEX_NODE *root, SUB_TREE_LIST_INFO *sub_tree_list_info)
{
	SUB_TREE_INFO_LINK *cur_link = sub_tree_list_info->sub_tree_info_link;

        /*Delete all these sub trees.*/
        while(cur_link)
        {
                NODE_PATH_INFO *sub_path_info;
                sub_path_info = fetch_index_path(root, cur_link->sub_root);
                INDEX_NODE_LINK *cur_linked_index = sub_path_info->index_node_link;
                while(cur_linked_index->next_link)
                {
                        cur_linked_index = cur_linked_index->next_link;
                }

                LEAF_NODE *front = cur_link->begin_leaf;
                if(!front->front_node)
                {
                        front = front->front_node;
                }

                LEAF_NODE *back = cur_link->end_leaf;
                if(!back->back_node)
                {
                        back = back->back_node;
                }

                exec_delete_a_sub_tree(root, cur_linked_index, cur_link->is_leaf,
                        cur_link->sub_root, front, back, cur_link->begin_leaf);

                cur_link = cur_link->next_link;

                free_node_path_info_mem(sub_path_info);
                sub_path_info = NULL;

        }

        cur_link = sub_tree_list_info->sub_tree_info_link;

        /*After delete a sub tree, free its memory immediately may lead problem that
        cannot find front node before the begin leaf.*/
        while(cur_link)
        {
                free_a_tree_mem(cur_link->sub_root);
                cur_link->sub_root = NULL;
                cur_link = cur_link->next_link;
        }
	
	return;
}

/*This is used to get a sub tree info from a tree.*/
SUB_TREE_LIST_INFO *
get_sub_tree_info(INDEX_NODE *root, LEAF_NODE *begin, LEAF_NODE *end)
{
	if(begin->data_record->key >= end->data_record->key)
	{
		cout<<"Wrong input format!\n"<<endl;
		return(NULL);
	}

	SUB_TREE_LIST_INFO *ret;
	ret = create_sub_tree_list_info_mem();
	
	DATA_INFO *data_info;
	data_info = fetch_leaf_list_data_info(begin, end);

	ret->leaf_num = data_info->num;
	ret->data_list = data_info->data_list;
	ret->sub_tree_info_link = analyze_influence_sub_tree(root, 
		root, begin->data_record->key, end->data_record->key);

	return(ret);
}

/*This is used to analyze influenced sub tree when batch insert and delete.*/
SUB_TREE_INFO_LINK *
analyze_influence_sub_tree(INDEX_NODE *sub_root, INDEX_NODE *parent, int low, int high)
{
	SUB_TREE_INFO_LINK *ret;
	
	/*If it is a leaf node.*/
	if(is_leaf_node(sub_root))
	{
		if((sub_root->pri_key < low) || (sub_root->pri_key > high))
		{
			cout<<"No sub tree found!\n"<<endl;
			return(NULL);
		}
		
		LEAF_NODE *leaf;
		leaf = (LEAF_NODE *)sub_root;
		ret = create_sub_tree_info_link_mem();
                ret->sub_root = sub_root;
                ret->parent = parent;
                ret->begin_leaf = leaf;
                ret->end_leaf = leaf;
		if(!leaf->front_node)
		{
			ret->is_first = TRUE;
		}
		else
		{
                	ret->is_first = FALSE;
                }
		if(!leaf->back_node)
                {
                        ret->is_last = TRUE;
                }
                else
                {
                        ret->is_last = FALSE;
                }
                ret->is_leaf = TRUE;
                ret->next_link = NULL;
                return(ret);
	}

	LEAF_NODE *first, *last;
	first = quick_search_special(sub_root, FIRST_NODE);
	last = quick_search_special(sub_root, LAST_NODE);
	
	IDX_BOOL is_first = FALSE;
	IDX_BOOL is_last = FALSE;
	int real_low, real_high;

	if(low <= first->data_record->key)
	{
		real_low = first->data_record->key;
		is_first = TRUE;
	}
	else
	{
		real_low = low;
	}

	if(high >= last->data_record->key)
	{
		real_high = last->data_record->key;
		is_last = TRUE;
	}
	else
	{
		real_high = high;
	}

	/*The whole sub tree is covered.*/
	if(is_last && is_first)
	{
		ret = create_sub_tree_info_link_mem();
		ret->sub_root = sub_root;
		ret->parent = parent;
		ret->begin_leaf = first;
		ret->end_leaf = last;
		if(!first->front_node)
                {
                        ret->is_first = TRUE;
                }
                else
                {
                        ret->is_first = FALSE;
                }
                if(!last->back_node)
                {
                        ret->is_last = TRUE;
                }
                else
                {
                        ret->is_last = FALSE;
                }
		ret->is_leaf = FALSE;
		ret->next_link = NULL;
		return(ret);
	}

	int low_pos = fetch_pos(sub_root, real_low);
	int high_pos = fetch_pos(sub_root, real_high);

	if(low_pos == high_pos)
	{
		ret = analyze_influence_sub_tree(sub_root->p_node[low_pos], 
			sub_root, real_low, real_high);
		return(ret);
	}
	else
	{
		int sub_num = high_pos - low_pos + 1;
		int range_list[sub_num+1], i;
		range_list[0] = real_low;
		range_list[sub_num] = real_high;
		
		for(i = 1; i != sub_num; ++i)
		{
			range_list[i] = sub_root->key_list[low_pos + i];
		}

		SUB_TREE_INFO_LINK *cur_sub, *new_sub;
	
		/*Seach all sub trees and link the info.*/	
		cur_sub = analyze_influence_sub_tree(sub_root->p_node[low_pos + i],
                        sub_root, range_list[i], range_list[i+1]);
		i++;
		ret = cur_sub;

		while(i != sub_num)
		{
			new_sub = analyze_influence_sub_tree(sub_root->p_node[low_pos + i],
                        	sub_root, range_list[i], range_list[i+1]);
			cur_sub->next_link = new_sub;
			cur_sub = cur_sub->next_link;
			i ++;
		}
		
		return(ret);
	}
}

/*This is used to quick search first or last leaf in a sub root.*/
/*No node path info will generate, no lock will apply.*/
/*Mainly used to analyze sub root.*/
LEAF_NODE *
quick_search_special(INDEX_NODE *root, FIRST_OR_LAST first_last)
{
	INDEX_NODE *cur;
	cur = root;
	if(first_last == FIRST_NODE)
	{
		while(!is_leaf_node(cur))
		{
			cur = cur->p_node[0];
		}
	}
	else
	{
		while(!is_leaf_node(cur))
                {
			int key_num = cur->key_num;
                        cur = cur->p_node[key_num];
                }
	}
	
	LEAF_NODE *ret;
	ret = (LEAF_NODE *)cur;
	return(ret);
}

/*This is used to fetch data info from leaf list.*/
DATA_INFO *
fetch_leaf_list_data_info(LEAF_NODE *begin, LEAF_NODE *end)
{
	DATA_INFO *ret = create_data_info_mem();
	int is_first = 1, data_num = 0;

	LEAF_NODE *cur_leaf;
	cur_leaf = begin;
	DATA_RECORD_LIST *data_list, *cur_data, *new_data;	

	while(cur_leaf != end->back_node)
	{
		new_data = (DATA_RECORD_LIST *)malloc(sizeof(DATA_RECORD_LIST));
                new_data->data_record = cur_leaf->data_record;
                new_data->next_data = NULL;
	
		if(is_first)
		{
			data_list = new_data;
			cur_data = data_list;
			is_first = 0;
		}
		else
		{
			cur_data->next_data = new_data;
			cur_data = cur_data->next_data;
		}

		cur_leaf = cur_leaf->back_node;
		data_num ++;
	}

	ret->data_list = data_list;
	ret->num = data_num;
	ret->min_key = begin->data_record->key;
	ret->max_key = end->data_record->key;
	
	return(ret);
}

/*This is ued to insert a sub tree in a INDEX_NODE.*/
RUN_RESULT
insert_sub_tree(INDEX_NODE *root, INDEX_NODE *target, INDEX_NODE *sub_tree)
{
	NODE_PATH_INFO *node_path;
	node_path = fetch_index_path(root, target);

	if(!node_path->status)
	{
		cout<<"Invalid index node!\n"<<endl;
		return(RUN_FAILED);
	}

	INDEX_NODE_LINK *cur_link = node_path->index_node_link;

	while(cur_link->next_link)
	{
		cur_link = cur_link->next_link;
	}

	exec_insert_index_node(root, cur_link, sub_tree);
	
	return(RUN_SUCCESS);
}

/*This is used to get a node path info to a index node.*/
NODE_PATH_INFO *
fetch_index_path(INDEX_NODE *root, INDEX_NODE *target)
{
	NODE_PATH_INFO *ret;
	ret = create_node_path_info_mem();
	ret->is_include_leaf = FALSE;
	ret->end_leaf = NULL;
	
	INDEX_NODE_LINK *index_link = create_index_node_link_mem();
	ret->index_node_link = index_link;
	ret->status = RUN_FAILED;

	index_link->index_node = root;
	index_link->next_link = NULL;
	index_link->front_link = NULL;
	
	INDEX_NODE_LINK *cur_link, *new_link;
	cur_link = index_link;

	INDEX_NODE *cur_node;
	cur_node = root;

	if(target == root)
	{
		ret->status = RUN_SUCCESS;
		return(ret);
	}

	int key = target->pri_key;
	int pos = fetch_pos(cur_node, key);
	cur_node = cur_node->p_node[pos];

	while(!is_leaf_node(cur_node))
	{
		
		if(cur_node == target)
		{
			ret->status = RUN_SUCCESS;
			break;
		}
		
		new_link = create_index_node_link_mem();
		new_link->index_node = cur_node;
		new_link->next_link = NULL;
		new_link->front_link = cur_link;
		cur_link->next_link = new_link;
		cur_link = cur_link->next_link;
		pos = fetch_pos(cur_node, key);
		cur_node = cur_node->p_node[pos];
	}
	
	return(ret);
}

/*This is used to execute insert a index node.*/
void
exec_insert_index_node(INDEX_NODE *root, INDEX_NODE_LINK *target_link, INDEX_NODE *new_node)
{
        int new_key = new_node->pri_key;
	INDEX_NODE_LINK *cur_link = target_link;
	int insert_pos, cur_key_num, i;
	int key = new_node->pri_key;

        int new_key_list[MAXKEYNUM];
        INDEX_NODE *new_p_list[MAXKEYNUM + 1];
        while(cur_link)
        {
                insert_pos = fetch_pos(cur_link->index_node, new_key);
                cur_key_num = cur_link->index_node->key_num;
                int old_pri = cur_link->index_node->pri_key;

                for(i=0; i < insert_pos; ++i)
                {
                        new_key_list[i] = cur_link->index_node->key_list[i];
                        new_p_list[i] = cur_link->index_node->p_node[i];
                }

		/*Consider the situation that new key will not be added. 
		The new added key is produced from a node which is not included before.
		This situation happens when inserting largest key to a node.*/
		if(insert_pos == cur_key_num &&
                        key > cur_link->index_node->key_list[cur_key_num - 1])
		{
			new_key_list[cur_key_num] = 
				cur_link->index_node->p_node[cur_key_num]->pri_key;
			new_p_list[cur_key_num] = cur_link->index_node->p_node[cur_key_num];
			new_p_list[cur_key_num + 1] = new_node;
		}
		else
		{
                	new_key_list[insert_pos] = new_key;
                	new_p_list[insert_pos] = new_node;

	                for(i = insert_pos+1; i < cur_key_num + 1; ++i)
        	        {
                	        new_key_list[i] = cur_link->index_node->key_list[i-1];
                        	new_p_list[i] = cur_link->index_node->p_node[i-1];
                	}
                	new_p_list[cur_key_num + 1] = cur_link->index_node->p_node[cur_key_num];
		}

                /*No need to split node*/
                if(cur_key_num + 1 <= MAXKEYNUM)
                {
                        for(i=0; i< cur_key_num + 1; ++i)
                        {
                                cur_link->index_node->key_list[i] = new_key_list[i];
                                cur_link->index_node->p_node[i] = new_p_list[i];
                        }
                        cur_link->index_node->p_node[cur_key_num + 1] = new_p_list[cur_key_num + 1];
                        cur_link->index_node->key_num = cur_key_num + 1;
                        cur_link->index_node->pri_key = old_pri > key ? old_pri : key;

                        cur_link = cur_link->front_link;

                        /*Although no need to split node anymore, need to
                        check whether pri_key need to change.*/
                        while(cur_link)
                        {
                                old_pri = cur_link->index_node->pri_key;
                                cur_link->index_node->pri_key = old_pri > key ? old_pri : key;
                                cur_link = cur_link->front_link;
                        }

                        break;
                }
                else
                {
                        /*Need further split*/
                        new_node = split_node(cur_link->index_node,
                                        cur_key_num, new_key_list, new_p_list);
                        new_key = new_node->pri_key;

                        if(cur_link->index_node != root)
                        {
                                cur_link = cur_link->front_link;
                        }
                        else
                        {
                                INDEX_NODE *old_root = cur_link->index_node;
                                int old_key_num = old_root->key_num;

                                old_root->p_node[old_key_num+1] = old_root->p_node[old_key_num];
                                for(i = old_key_num ; i != 0; i--)
                                {
                                        old_root->key_list[i] = old_root->key_list[i-1];
                                        old_root->p_node[i] = old_root->p_node[i-1];
                                }
                                old_root->key_list[0] = new_key;
                                old_root->p_node[0] = new_node;

                                /*Do not need further split*/
                                break;
                        }
                }
        }

	return;	
}

/*This is used to delete a sub tree.*/
/*A leaf is treated as a special sub tree.*/
/*Delete will not change pri_key.*/
void
exec_delete_a_sub_tree(INDEX_NODE *root, INDEX_NODE_LINK *cur_linked_index, IDX_BOOL is_leaf, 
	INDEX_NODE *sub_root, LEAF_NODE *front_leaf, LEAF_NODE *back_leaf, LEAF_NODE *target_leaf)
{
	/*If it is a leaf, remove the leaf*/	
	if(is_leaf)
	{
        	/*If it is the first leaf*/
        	if(front_leaf == target_leaf)
        	{
                	target_leaf->back_node = NULL;
	                back_leaf->front_node = NULL;
        	        free(target_leaf);
                	target_leaf = NULL;
	        }

	        /*If it is the last leaf.*/
        	else if(target_leaf == back_leaf)
	        {
        	        front_leaf->back_node = NULL;
                	target_leaf->front_node = NULL;
	                free(target_leaf);
        	        target_leaf = NULL;
	        }
        	else
        	{
                	front_leaf->back_node = back_leaf;
	                back_leaf->front_node = front_leaf;
        	        target_leaf->back_node = NULL;
                	target_leaf->front_node = NULL;
	                free(target_leaf);
        	        target_leaf = NULL;
	        }
	}
	else
	{
		/*Dislink the leaf list.*/
		LEAF_NODE *last_target;
		last_target = back_leaf->front_node;
		
		if(front_leaf == target_leaf)
                {
			back_leaf->front_node = NULL;
			last_target->back_node = NULL;
		}
		else if(target_leaf == back_leaf)
		{
			front_leaf->back_node = NULL;
			target_leaf->front_node = NULL;
		}
		else
		{
			front_leaf->back_node = back_leaf;
			back_leaf->front_node = front_leaf;
			target_leaf->front_node = NULL;
			last_target->back_node = NULL;
		}
	}

	
	int cur_key_num =  cur_linked_index->index_node->key_num;

	int key = sub_root->pri_key;

        int pos = fetch_pos(cur_linked_index->index_node, key);

	int i;

        /*Remove the link to target node firstly.*/
        for(i = pos; i != cur_key_num; ++i)
        {
                cur_linked_index->index_node->key_list[i]
                        = cur_linked_index->index_node->key_list[i+1];
                cur_linked_index->index_node->p_node[i] =
                        cur_linked_index->index_node->p_node[i+1];
        }
	
        /*Remove the last key.*/
        cur_linked_index->index_node->key_list[cur_key_num - 1] = 0;
        cur_linked_index->index_node->p_node[cur_key_num] = NULL;

        cur_linked_index->index_node->key_num-- ;
        cur_key_num--;


        /*Then merge nodes.*/
        if(cur_key_num < LEAST_P_NUM - 1)
        {
                /*Need to merge node.*/
                INDEX_NODE_LINK *next_linked_index;
                next_linked_index = cur_linked_index->front_link;

                int key_to_move = cur_key_num;
                int insert_pos = fetch_pos(next_linked_index->index_node,
                                cur_linked_index->index_node->pri_key);
                int next_key_num = next_linked_index->index_node->key_num;

                int child_pri = cur_linked_index->index_node->pri_key;
                int father_pri = next_linked_index->index_node->pri_key;

                int new_key_list[2*MAXKEYNUM];
                INDEX_NODE *new_p_list[2*MAXKEYNUM];

                for(i = 0; i < insert_pos; ++i)
                {
                        new_key_list[i] = next_linked_index->index_node->key_list[i];
                        new_p_list[i] = next_linked_index->index_node->p_node[i];
                }

                for(i = 0; i < key_to_move; ++i)
                {
                        new_key_list[i + insert_pos] =
                                cur_linked_index->index_node->key_list[i];
                        new_p_list[i + insert_pos] =
                                cur_linked_index->index_node->p_node[i];
                }

                new_p_list[insert_pos + key_to_move] =
                                cur_linked_index->index_node->p_node[key_to_move];


                /*p_node[insert_pos] in next node will be delete.*/
                for(i = 0; i < next_key_num  - insert_pos; ++i)
                {
                        new_key_list[i + insert_pos + key_to_move] =
                                next_linked_index->index_node->key_list[insert_pos + i];
                        new_p_list[i + insert_pos + key_to_move + 1] =
                                next_linked_index->index_node->p_node[insert_pos + 1 + i];
                }

                /*For Test*/
                /*
                for(i = 0; i != next_key_num + key_to_move; ++i)
                {
                        cout<<new_key_list[i]<<"\n";
                }
                cout<<"********************"<<endl;
                cout<<key_to_move<<"###"<<next_key_num<<endl;
                */

                /*No need to split.*/
                if(next_key_num + key_to_move <= MAXKEYNUM)
                {
                        for(i = 0; i < next_key_num + key_to_move; ++i)
                        {
                                next_linked_index->index_node->key_list[i]
                                        = new_key_list[i];
                                next_linked_index->index_node->p_node[i]
                                        = new_p_list[i];
                        }
                        next_linked_index->index_node->p_node[next_key_num + key_to_move]
                                = new_p_list[next_key_num + key_to_move];
                        next_linked_index->index_node->pri_key = child_pri > father_pri ? child_pri : father_pri;
                        next_linked_index->index_node->key_num = next_key_num + key_to_move;

                        return;
                }


                /*Need to split nodes.*/
                INDEX_NODE *new_index_node;
                new_index_node = split_node(next_linked_index->index_node,
                                next_key_num + key_to_move, new_key_list, new_p_list);

                free(cur_linked_index->index_node);
                cur_linked_index = next_linked_index;

                /*Since the node need to split, next node must exsit.*/
                next_linked_index = next_linked_index->front_link;
                while(next_linked_index)
                {
                        insert_pos = fetch_pos(next_linked_index->index_node, new_index_node->pri_key);

                        next_key_num = next_linked_index->index_node->key_num;

                        for(i = 0; i < insert_pos; ++i)
                        {
                                new_key_list[i] = next_linked_index->index_node->key_list[i];
                                new_p_list[i] = next_linked_index->index_node->p_node[i];
                        }

                        new_key_list[insert_pos] = new_index_node->pri_key;
                        new_p_list[insert_pos] = new_index_node;

                        for(i = insert_pos; i < next_key_num; i++)
                        {
                                new_key_list[i+1] =
                                        next_linked_index->index_node->key_list[i];
                                new_p_list[i+1] = next_linked_index->index_node->p_node[i];
                        }
                        new_p_list[next_key_num + 1] = next_linked_index->index_node->p_node[next_key_num];

                        key_to_move = 1;

                        /*Need further split.*/
                        if(next_key_num + 1 > MAXKEYNUM)
                        {
                                new_index_node = split_node(next_linked_index->index_node,
                                        next_key_num + 1, new_key_list, new_p_list);


                                if(next_linked_index->index_node != root)
                                {
                                        next_linked_index = next_linked_index->front_link;
                                }
                                else
                                {
                                        /*Need to split root.*/
                                        INDEX_NODE *old_root;
                                        old_root = next_linked_index->index_node;

                                        int old_key_num = old_root->key_num;

                                        old_root->p_node[old_key_num+1] = old_root->p_node[old_key_num];
                                        for(i = old_key_num; i != 0; i--)
                                        {
                                                old_root->key_list[i] = old_root->key_list[i - 1];
                                                old_root->p_node[i] = old_root->p_node[i - 1];
                                        }
                                        old_root->key_list[0] = new_index_node->pri_key;
                                        old_root->p_node[0] = new_index_node;

                                        /*Root address will not be changed.*/
                                        break;
                                }
                        }
                        else
                        {
                                for(i = 0; i < next_key_num + 1; ++i)
                                {
                                        next_linked_index->index_node->key_list[i]
                                                = new_key_list[i];
                                        next_linked_index->index_node->p_node[i]
                                                = new_p_list[i];
                                }
                                next_linked_index->index_node->p_node[next_key_num + 1]
                                                = new_p_list[next_key_num + 1];
                                next_linked_index->index_node->pri_key = new_key_list[next_key_num + 1];
                                next_linked_index->index_node->key_num = next_key_num + 1;

                                /*Do not need further split.*/
                                break;
                        }
                }
        }

	/*Need to free the whole sub tree mem*/
	if(!is_leaf)
	{
		free_a_tree_mem(sub_root);
	}

	return;
}

/*This is used to batch delete.*/
INDEX_NODE *
bactch_delete(INDEX_NODE *root, int low, int high)
{
	/*Analyze and lock leaf link list*/
        LEAF_NODE *begin_leaf, *end_leaf;
        begin_leaf = search_near_leaf(root, low, SUB_MARK);
        end_leaf = search_near_leaf(root, high, UP_MARK);

	/*The whole tree is covered, just replace it.*/
        if((!(begin_leaf->front_node)) && (!(end_leaf->back_node)))
        {
                free_a_tree_mem(root);
                root = NULL;
                return(NULL);
        }	

        SUB_TREE_LIST_INFO *sub_tree_list_info;
        sub_tree_list_info = get_sub_tree_info(root, begin_leaf, end_leaf);

	/*Execute delete theses sub trees*/
        exec_delete_sub_trees(root, sub_tree_list_info);

        free_sub_tree_list_info_mem(sub_tree_list_info);
        sub_tree_list_info = NULL;

	/*Need to re-balance the whole tree after batch delete.*/
	//re_balance_tree(root);
	
	return(root);
}

/*This is used to rebalance a tree.*/
/*Need to design algorithm carefully here.*/
/*
DEPTH_INFO *
re_balance_tree(INDEX_NODE *node)
{
	DEPTH_INFO *ret = create_dapth_info_mem();

	if(is_leaf_node(node))
	{
		ret->lagest = 0;
		ret->smallest = 0;
		return(ret);
	}

	int i, node_num = node->key_num;
	DEPTH_INFO *depth_info[node_num];
	for(i = 0; i != node_num+1; ++i)
	{
		depth_info[i] = re_balance_tree(node->p_node[i]);
	}
	
	int smallest, largest;
	smallest = depth_info[0]->smallest;
	largest = depth_info[0]->largest;

	for(i = 1; i != node_num+1; ++i)
	{
		
	}
}
*/
