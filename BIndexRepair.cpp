#include<iostream>
#include"./BIndex.h"
#include<math.h>
#include<stdlib.h>
#include<string>
#include<string.h>
#include<fstream>


using namespace std;


/*This file is used to do dignose and repair index tree.*/



RUN_RESULT check_and_repair_leaf_link(INDEX_NODE *);
RUN_RESULT exec_repair_leaf_link(INDEX_NODE *, LEAF_NODE *, LEAF_NODE *, SEARCH_DIR);
RUN_RESULT relink_leaf(INDEX_NODE *, LEAF_NODE *, LEAF_NODE *,  LEAF_NODE *, SEARCH_DIR);
LEAF_NODE *get_next_leaf_by_index_tree(INDEX_NODE *, LEAF_NODE *, SEARCH_DIR);
IDX_BOOL exist_circle_link(LEAF_NODE *, LEAF_NODE *, LEAF_NODE *, SEARCH_DIR);
LEAF_NODE *get_next_leaf_by_key(INDEX_NODE *, LEAF_NODE *, LEAF_NODE *, LEAF_NODE *, SEARCH_DIR);
INDEX_NODE *re_generate_index_tree(INDEX_NODE *, LEAF_NODE *, LEAF_NODE *);
INDEX_NODE *repair_and_rebuild_index_tree(INDEX_NODE *);
RUN_RESULT exec_repair_double_leaf_link(INDEX_NODE *, LEAF_NODE *, LEAF_NODE *);
RUN_RESULT repair_lost_ends(INDEX_NODE *, LEAF_NODE *, LEAF_NODE *);


/*This is used to check bronken link in leaf link.*/
RUN_RESULT
check_and_repair_leaf_link(INDEX_NODE *root)
{
	LEAF_NODE *first, *last;
	first = quick_search_special(root, FIRST_NODE);
	last = quick_search_special(root, LAST_NODE);

	RUN_RESULT ret;
	ret = exec_repair_double_leaf_link(root, first, last);
	return(ret);
}

/*This is used to execute repair leaf link.*/
RUN_RESULT
exec_repair_leaf_link(INDEX_NODE *root, LEAF_NODE *begin
		, LEAF_NODE *end, SEARCH_DIR dirction)
{
	if(!begin || !end)
	{
		cout<<"Wrong input!\n"<<endl;
		return(RUN_FAILED);
	}	

	LEAF_NODE *cur_leaf;
	cur_leaf = begin;

	int num = 0;
	while(cur_leaf != end)
	{
		if(dirction == FORWARD)
		{
			/*Problem may be caused by lost link or circle link.*/
			if(cur_leaf->back_node && 
				!exist_circle_link(cur_leaf->back_node, begin, cur_leaf,  FORWARD))
			{
				cur_leaf = cur_leaf->back_node;
			}
			else
			{
				/*This link is broken, repair it.*/
				if(relink_leaf(root, cur_leaf, begin, end, FORWARD))
				{
					cout<<"Repair a leaf link:\n"<<"Addr: "
						<<cur_leaf<<" Key: "<<cur_leaf->data_record->key
						<<" Value: "<<cur_leaf->data_record->value<<endl;
					num ++;
					cur_leaf = cur_leaf->back_node;
				}
				else
				{
					cout<<"Repair this leaf failed:\n"<<"Addr: "
                                                <<cur_leaf<<" Key: "<<cur_leaf->data_record->key
                                                <<" Value: "<<cur_leaf->data_record->value<<endl;
					return(RUN_FAILED);
				}
			}
		}
		else
		{
			/*Problem may be caused by lost link or circle link.*/
			if(cur_leaf->front_node &&
				!exist_circle_link(cur_leaf->front_node, begin, cur_leaf,  BACKWARD))
                        {
                                cur_leaf = cur_leaf->front_node;
                        }
                        else
                        {
                                /*This link is broken, repair it.*/
                                if(relink_leaf(root, cur_leaf, begin, end, BACKWARD))
                                {
                                        cout<<"Repair a leaf link:\n"<<"Addr: "
                                                <<cur_leaf<<" Key: "<<cur_leaf->data_record->key
                                                <<" Value: "<<cur_leaf->data_record->value<<endl;
                                        num ++;
                                        cur_leaf = cur_leaf->front_node;
                                }
                                else
                                {
                                        cout<<"Repair this leaf failed:\n"<<"Addr: "
                                                <<cur_leaf<<" Key: "<<cur_leaf->data_record->key
                                                <<" Value: "<<cur_leaf->data_record->value<<endl;
                                        return(RUN_FAILED);
                                }
                        }
		}
	}


	cout<<"Total "<<num<<" leaf links repaired.\n"<<endl;
	return(RUN_SUCCESS);
}

/*This is used to do relink a leaf.*/
RUN_RESULT
relink_leaf(INDEX_NODE *root, LEAF_NODE *leaf, LEAF_NODE *begin,  LEAF_NODE *end, SEARCH_DIR direction)
{
	LEAF_NODE *next_leaf;
	next_leaf = get_next_leaf_by_index_tree(root, leaf, direction);
	
	IDX_BOOL found = TRUE;
		
	if(!next_leaf)
	{
		cout<<"Index tree is broken. Please re-build index tree.\n"<<endl;
		found = FALSE;
		cout<<"Now try to relink leaf by searching key.\n"<<endl;
	}

	if(!found)
	{
		next_leaf = get_next_leaf_by_key(root, leaf, begin, end, direction);
		if(!next_leaf)
		{
			cout<<"Searching by key failed! Please do data recovery by reading files.\n"<<endl;
			found = FALSE;
			return(RUN_FAILED);
		}
		else
		{
			found = TRUE;
		}
	}

	if(direction == FORWARD)
	{
		leaf->back_node = next_leaf;
		next_leaf->front_node = leaf;
	}
	else
	{
		next_leaf->back_node = leaf;
                leaf->front_node = next_leaf;
	}
	
	return(RUN_SUCCESS);
}

/*This is used to get next leaf.*/
LEAF_NODE *
get_next_leaf_by_index_tree(INDEX_NODE *root, LEAF_NODE *cur_leaf, SEARCH_DIR direction)
{
	LEAF_NODE *ret;
	NODE_PATH_INFO *node_path = scan_node(root, cur_leaf->data_record->key, FAKE_PID);

	if(!node_path->status)
	{
		return(NULL);
	}
	
	INDEX_NODE_LINK *linked_index;
	linked_index = node_path->index_node_link;
	
	while(linked_index->next_link)
	{
		linked_index = linked_index->next_link;
	}

	int pos = fetch_pos(linked_index->index_node, cur_leaf->data_record->key);
	
	if(direction == FORWARD)
	{
		/*Not the last leaf linked to this index node.*/
		if(pos != linked_index->index_node->key_num)
		{
			ret = (LEAF_NODE *)(linked_index->index_node->p_node[pos + 1]);
			free_node_path_info_mem(node_path);
			return(ret);
		}
		else
		{
			linked_index = linked_index->next_link;
			if(!linked_index)
			{
				/*If there is only one level index tree. And one leaf
				 which is not the last one has no back node, there must be
				 error in the index tree.*/
				cout<<"Index tree error!\n"<<endl;
				free_node_path_info_mem(node_path);
				return(NULL);
			}

			IDX_BOOL found = FALSE;
			while(linked_index->next_link)
			{
				pos = fetch_pos(linked_index->index_node, cur_leaf->data_record->key);
				if(pos != linked_index->index_node->key_num)
				{
					ret =  quick_search_special(
						linked_index->index_node->p_node[pos + 1], FIRST_NODE);
					found = TRUE;
					break;
				}
				else
				{
					linked_index = linked_index->next_link;
				}
			}

			if(found)
			{
				free_node_path_info_mem(node_path);
				return(ret);
			}
			else
			{
				/*If not found, there must sth wrong with index tree.*/
				free_node_path_info_mem(node_path);
				return(NULL);
			}
		}
	}
	else
	{
		/*Not the first leaf linked to this index node.*/
                if(!pos)
                {
                        ret = (LEAF_NODE *)(linked_index->index_node->p_node[pos - 1]);
			free_node_path_info_mem(node_path);
                        return(ret);
                }
                else
                {
                        linked_index = linked_index->next_link;
                        if(!linked_index)
                        {
                                /*If there is only one level index tree. And one leaf
                                 which is not the first one has no front node, there must be
                                 error in the index tree.*/
                                cout<<"Index tree error!\n"<<endl;
				free_node_path_info_mem(node_path);
                                return(NULL);
                        }

                        IDX_BOOL found = FALSE;
                        while(linked_index->next_link)
                        {
                                pos = fetch_pos(linked_index->index_node, cur_leaf->data_record->key);
                                if(!pos)
                                {
                                        ret =  quick_search_special(
                                                linked_index->index_node->p_node[pos - 1], LAST_NODE);
                                        found = TRUE;
                                        break;
                                }
                                else
                                {
                                        linked_index = linked_index->next_link;
                                }
                        }

                        if(found)
                        {
				free_node_path_info_mem(node_path);
                                return(ret);
                        }
                        else
                        {
                                /*If not found, there must sth wrong with index tree.*/
				free_node_path_info_mem(node_path);
                                return(NULL);
                        }
                }
	}
}

/*This is used to check whether there is circle link or not.*/
IDX_BOOL
exist_circle_link(LEAF_NODE *target, LEAF_NODE *begin, LEAF_NODE *stop, SEARCH_DIR direction)
{
	LEAF_NODE *cur_leaf;
	IDX_BOOL ret = FALSE;
	cur_leaf = begin;

	/*Target leaf cannot be stop leaf.*/
	while(cur_leaf != stop)
	{
		if(cur_leaf == target)
		{
			ret = TRUE;
			break;
		}
		else
		{
			if(direction == FORWARD)
			{
				cur_leaf = cur_leaf->back_node;
			}
			else
			{
				cur_leaf = cur_leaf->front_node;
			}
		}
	}
	
	return(ret);
} 

/*This is used to get next leaf by searching key.*/
LEAF_NODE *
get_next_leaf_by_key(INDEX_NODE *root, LEAF_NODE *leaf, LEAF_NODE *begin,
		 LEAF_NODE *end, SEARCH_DIR direction)
{
	int dirct;
	IDX_BOOL found = FALSE;

	if(direction == FORWARD)
	{
		dirct = 1;
	}
	else
	{
		dirct = -1;
	}

	LEAF_NODE *cur_leaf;
	int search_key = leaf->data_record->key + dirct; 
	while(search_key != end->data_record->key)
	{
		if(direction == FORWARD)
		{
			cur_leaf = search_near_leaf(root, search_key, SUB_MARK);
		}
		else
		{
			cur_leaf = search_near_leaf(root, search_key, UP_MARK);
		}
	
		/*There may exist circle link due to key sort error!*/	
		if(cur_leaf == leaf || !cur_leaf 
			||exist_circle_link(cur_leaf, begin, leaf,  direction))
		{
			search_key = search_key + dirct;
		}
		else
		{
			found = TRUE;
			break;
		}
	}

	/*Further fix it.*/
	if(direction == BACKWARD)
	{
		while(!cur_leaf->back_node && cur_leaf->back_node != leaf)
		{
			cur_leaf = cur_leaf->back_node;
		}
	}
	else
	{
		while(!cur_leaf->front_node && cur_leaf->front_node != leaf)
                {
                        cur_leaf = cur_leaf->front_node;
                }
	}

	if(!found)
	{
		return(NULL);
	}
	else
	{
		return(cur_leaf);
	}
}

/*This is used to re-generate index tree.*/
/*Before re-generating, leaf link must be repaired.*/
INDEX_NODE *
re_generate_index_tree(INDEX_NODE *root, LEAF_NODE *begin, LEAF_NODE *end)
{
	DATA_INFO *data_info;
	data_info = fetch_leaf_list_data_info(begin, end);

	/*For test.*/
	test_list(data_info->data_list, data_info->num, OFF);
	INDEX_NODE *ret;
        LEAF_NODE *leaf_node_list;
        leaf_node_list = generate_leaf_node(data_info->data_list, data_info->num);
	free_broken_index_tree(root);
        free_leaf_list_mem(begin, end, FORWARD);
        root = NULL;
        begin = NULL;
        end = NULL;

        ret = generate_non_leaf(leaf_node_list, data_info->num);
	free(data_info);
	data_info = NULL;
	return(ret);
}

/*This is used to repair the leaf link and re-build index tree.*/
INDEX_NODE *
repair_and_rebuild_index_tree(INDEX_NODE *root)
{
        LEAF_NODE *first, *last;
        first = quick_search_special(root, FIRST_NODE);
        last = quick_search_special(root, LAST_NODE);

        if(exec_repair_double_leaf_link(root, first, last))
        {
		cout<<"Now re-generate index tree.\n"<<endl;
        }
        else
        {
                return(NULL);
        }

	test_leaf_link(first, OFF);

	/*First node and last node may be changed.*/
	while(first->front_node)
	{
		first = first->front_node;
	}

	while(last->back_node)
	{
		last = last->back_node;
	}

	test_leaf_link(first, OFF);
	
	INDEX_NODE *ret;
	ret = re_generate_index_tree(root, first, last);
	return(ret);
}


/*This is used to repair double leaf link.*/
RUN_RESULT
exec_repair_double_leaf_link(INDEX_NODE *root, LEAF_NODE *first, LEAF_NODE *last)
{
        if(exec_repair_leaf_link(root, first, last, FORWARD)
                && exec_repair_leaf_link(root, last, first, BACKWARD))
        {
		if(repair_lost_ends(root, first, last))
                {
			/*For test*/
			//draw_all_rest_leaves(first, FORWARD);
			//draw_all_rest_leaves(last, BACKWARD);

			cout<<"Repair leaf link finish!\n"<<endl;
                	return(RUN_SUCCESS);
		}
		else
		{
			cout<<"Repair single direction leaf link successed.\n";
			cout<<"But repair double direction leaf link failed.\n";
			cout<<"There may exist lost data, please do data recovery.\n"<<endl;
			return(RUN_FAILED);
		}
        }
        else
        {
                cout<<"Repair leaf link error."<<
                        " You'd better do data recovery.\n"<<endl;
                return(RUN_FAILED);
        }
}

/*This is used to link lost link ends.*/
RUN_RESULT
repair_lost_ends(INDEX_NODE *root, LEAF_NODE *first, LEAF_NODE *last)
{
	LEAF_NODE *cur_leaf, *next_node;
	next_node = last;
	cur_leaf = first;
	while(cur_leaf != last)
	{
		/*There exists lost link.*/
		if(!cur_leaf->back_node)
		{
			while(next_node->front_node != cur_leaf)
			{
				next_node = next_node->front_node;
				if(!next_node)
				{
					/*Cannot found! Data lost!*/
					cout<<"Data lost!\n"<<endl;
					return(RUN_FAILED);
				}
			}
			cout<<"Re-link a lost link: "<<cur_leaf<<" and "<<next_node
				<<"\nKeys: "<<cur_leaf->data_record->key<<" "
				<<next_node->data_record->key<<endl;
			cur_leaf->back_node = next_node;
			/*All the rest nodes are not repaired.
			So dislink all of them and re-link again.
			Since all the next nodes can be found, 
			this process is avaliable.*/
			next_node->back_node = NULL;
			next_node = last;
		}

		cur_leaf = cur_leaf->back_node;
	}

	cur_leaf = last;
	next_node = first;
	while(cur_leaf != first)
        {
                /*There exists lost link.*/
                if(!cur_leaf->front_node)
                {
                        while(next_node->back_node != cur_leaf)
                        {
                                next_node = next_node->back_node;
                                if(!next_node)
                                {
                                        /*Cannot found! Data lost!*/
                                        cout<<"Data lost!\n"<<endl;
                                        return(RUN_FAILED);
                                }
                        }
			cout<<"Re-link a lost link: "<<cur_leaf<<" and "<<next_node
                                <<"\nKeys: "<<cur_leaf->data_record->key<<" "
                                <<next_node->data_record->key<<endl;
                        cur_leaf->front_node = next_node;
			/*All the rest nodes are not repaired.
                        So dislink all of them and re-link again.
                        Since all the next nodes can be found,
                        this process is avaliable.*/
			next_node->front_node = NULL;
			next_node = first;
                }

                cur_leaf = cur_leaf->front_node;
        }

	return(RUN_SUCCESS);
}
