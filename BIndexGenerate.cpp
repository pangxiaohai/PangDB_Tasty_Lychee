#include<iostream>
#include"./BIndex.h"
#include<math.h>
#include<stdlib.h>

using namespace std;


/* This file is used for generating new BIndex tree.*/


INDEX_NODE *mk_index(DATA_RECORD_LIST *, int);
LEAF_NODE *generate_leaf_node(DATA_RECORD_LIST *, int);
DATA_RECORD_LIST *quick_sort(DATA_RECORD_LIST*, int);
LEAF_NODE *link_list(DATA_RECORD_LIST *, int);
INDEX_NODE *generate_non_leaf(LEAF_NODE *, int);
LEVEL_INFO *generate_level(LEVEL_INFO*);
LEVEL_INFO *generate_rest_level(LEVEL_INFO*);
DATA_RECORD_LIST *fetch_last_data_record_list(DATA_RECORD_LIST *, int);
DATA_RECORD_LIST *merge_sort(DATA_RECORD_LIST*, DATA_RECORD_LIST*);


/*This function is called during initialize.*/
INDEX_NODE *
mk_index(DATA_RECORD_LIST *data_list, int record_num)
{
	INDEX_NODE *root;
	LEAF_NODE *leaf_node_list;
	leaf_node_list = generate_leaf_node(data_list, record_num);
	root = generate_non_leaf(leaf_node_list, record_num);

	/*For test. 	
	draw_a_tree(root);
	cout<<"\n\n\n"<<endl;
	*/

	return(root);
}

/*Sort data record by key and link them together.*/
LEAF_NODE *
generate_leaf_node(DATA_RECORD_LIST *data_list, int record_num)
{	
	LEAF_NODE *retval;
	DATA_RECORD_LIST *sorted_list;
	
	sorted_list = quick_sort(data_list, record_num);
	/*For Test.*/
	test_list(sorted_list, record_num, OFF);

	retval = link_list(sorted_list, record_num);
	/*For Test.*/
	test_leaf_link(retval, OFF);

	/*For Debugging Last Pri_key Lost.*/
	//draw_all_rest_leaves(retval, BACKWORD);	

	return(retval);
}

/*Link the data record together.*/
LEAF_NODE *
link_list(DATA_RECORD_LIST *data_list, int record_num)
{
	LEAF_NODE *retval_list, *cur_node, *new_node;
	DATA_RECORD_LIST *cur_data;
	int i;
	cur_data = data_list;

	cur_node = create_leaf_mem();
	cur_node->data_record = cur_data->data_record;
	cur_node->pri_key = cur_data->data_record->key;
	cur_node->front_node = NULL;
	cur_node->back_node = NULL;
	cur_node->node_type = LEAF_NODE_TYPE;
	retval_list = cur_node;

	cur_data = cur_data->next_data;

	for(i = 1; i != record_num; i++)
	{
		new_node = create_leaf_mem();
		new_node->data_record = cur_data->data_record;
		new_node->pri_key = cur_data->data_record->key;
		new_node->back_node=NULL;
		new_node->node_type = LEAF_NODE_TYPE;
		new_node->front_node = cur_node;

		//if(!set_node_lock(retval_list, NO_LOCK))
		//{
		//	return;
		//}
		
		cur_node->back_node = new_node;

		cur_data = cur_data->next_data;
		cur_node = cur_node->back_node;
	}

	/*For Debugging Last Pri_key Lost.*/
        //draw_all_rest_leaves(retval_list, BACKWORD);

	free_data_record_list_mem(data_list);
	data_list = NULL;

	return(retval_list);
}

/*Quik sort to sort dara records.*/
DATA_RECORD_LIST *
quick_sort(DATA_RECORD_LIST *data_list, int record_num)
{
	/*Record number cannot be 0,
	because NULL has no member.*/
        if(record_num == 1)
        {
                return(data_list);
        }

        if(record_num == 2)
        {
		DATA_RECORD_LIST *first, *second;
		first = data_list;
		second = data_list->next_data;
		
		if(first->data_record->key > second->data_record->key)
		{
			second->next_data = first;
			first->next_data = NULL;
			data_list = second;
		}

		return(data_list);		
        }

	DATA_RECORD_LIST *mark_data;
	DATA_RECORD_LIST *right_data, *left_data, *cur_data;
	DATA_RECORD_LIST  *new_data, *cur_left, *cur_right;
	
	int i, j, left = 0, right = 0, mark_key;

	/*Save the first one.*/
	mark_data = create_data_record_list_mem();
	mark_data->data_record = data_list->data_record;
	mark_data->next_data = NULL;

	mark_key = mark_data->data_record->key;
	cur_data = data_list->next_data;

	/*For Test*/	
	test_list(data_list, record_num, OFF);

	for(i = 1; i != record_num; ++i)
	{
		if(mark_key > cur_data->data_record->key)
		{
			if(!left)
			{
				left_data = create_data_record_list_mem();
				left_data->data_record = cur_data->data_record;
				left_data->next_data = NULL;
				cur_left = left_data;
			}
			else
			{
				new_data =  create_data_record_list_mem();
                                new_data->data_record = cur_data->data_record;
                                new_data->next_data = NULL;
				cur_left->next_data = new_data;
				cur_left = cur_left->next_data;
			}
			left ++;
			cur_data = cur_data->next_data;
		}
		else
		{
			if(!right)
                        {
                                right_data = create_data_record_list_mem();
                                right_data->data_record = cur_data->data_record;
                                right_data->next_data = NULL;
                                cur_right = right_data;
                        }
                        else
                        {
                                new_data =  create_data_record_list_mem();
                                new_data->data_record = cur_data->data_record;
                                new_data->next_data = NULL;
                                cur_right->next_data = new_data;
                                cur_right = cur_right->next_data;
                        }
			right++;
			cur_data = cur_data->next_data;
		}
	}
	
	free_data_record_list_mem(data_list);	
	data_list = NULL;

	DATA_RECORD_LIST *ret_left, *ret_right, *last_left, *ret;

	/*For Test*/
	test_list(left_data, left, OFF);
	test_list(mark_data, 1, OFF);
	test_list(right_data, right, OFF);

	if(left)
	{	
		ret_left = quick_sort(left_data, left);
	}
	if(right)
	{
		ret_right = quick_sort(right_data, right);
	}

	if(left)
	{
		DATA_RECORD_LIST *last_left;
		last_left = fetch_last_data_record_list(ret_left, left);
		
		/*For Test*/
		test_list(last_left, 1, OFF);
		last_left->next_data = mark_data;
	}
	
	if(right)
	{
		mark_data->next_data = ret_right;
	}

	if(left)
	{
		ret = ret_left;
	}
	else
	{
		ret = mark_data;
	}

	/*For Test*/
	test_list(ret, record_num, OFF);
	return(ret);
}

/*This is used to fetch last data in data record list.*/
DATA_RECORD_LIST *
fetch_last_data_record_list(DATA_RECORD_LIST *data_list, int left)
{
	if(left == 1)
	{
		return(data_list);
	}
	DATA_RECORD_LIST *ret;
	ret = data_list;
	left --;
	while(left)
	{
		ret = ret->next_data;
		left --;
		test_list(ret, 1, OFF);
	}
	return(ret);
}

/*Generate non leaf nodes.*/
INDEX_NODE *
generate_non_leaf(LEAF_NODE *leaf_node_list, int record_num)
{
	int tree_level = (int)ceil(log(record_num)/log(LEAST_P_NUM));
	int extra_node, secLast_node_part1, secLast_node_part2, last_level_node;
	INDEX_NODE *root;

	/*Caculate number of nodes in last level.*/
	extra_node = record_num - (int)pow(LEAST_P_NUM, tree_level - 1);
	secLast_node_part1 = (int)ceil(extra_node/(LEAST_P_NUM-1));
	secLast_node_part2 = (int)pow(LEAST_P_NUM, tree_level - 1) - secLast_node_part1;
	last_level_node = record_num - secLast_node_part2;

	/*Generate tree.*/
	NODE_INFO *lastLevelList_info, *new_node_info, *cur_node_info;
	int last_idx;
	LEAF_NODE *cur_leaf_node;
	
	cur_leaf_node = leaf_node_list;
	
	cur_node_info = create_node_info_mem();
	cur_node_info->node_type = LEAF_NODE_TYPE;
	cur_node_info->node_addr = (INDEX_NODE *)cur_leaf_node;
	cur_node_info->next_node_info = NULL;
 
	lastLevelList_info = cur_node_info;

	cur_leaf_node = cur_leaf_node->back_node;

	for(last_idx = 1; last_idx != last_level_node; ++last_idx)
	{
		new_node_info = create_node_info_mem();
		new_node_info->node_type = LEAF_NODE_TYPE;
		new_node_info->node_addr = (INDEX_NODE *)cur_leaf_node;
		new_node_info->next_node_info = NULL;
		cur_node_info->next_node_info = new_node_info;
		cur_node_info = cur_node_info->next_node_info;
		cur_leaf_node = cur_leaf_node->back_node;
	}
	
	/*For Test.*/
	test_node_info(lastLevelList_info, OFF);

	LEVEL_INFO *lastLevel_info, *secLast_info;

	lastLevel_info = create_level_info_mem();
	secLast_info = create_level_info_mem();

	lastLevel_info->node_info_addr = lastLevelList_info;

	lastLevel_info->node_num = last_level_node;

	secLast_info = generate_level(lastLevel_info);

	/*For Test*/
	test_level_info(secLast_info, OFF);
		
	NODE_INFO *last_node_info;

	last_node_info = secLast_info->node_info_addr;

	while(last_node_info->next_node_info)
	{
		last_node_info = last_node_info->next_node_info;
	}

	int sec_idx;
	
	for(sec_idx = 0; sec_idx != secLast_node_part2; ++sec_idx)
	{
		new_node_info = create_node_info_mem();
		new_node_info->node_type = LEAF_NODE_TYPE;
                new_node_info->node_addr = (INDEX_NODE *)cur_leaf_node;

		/*For Test*/
		//cout<<sec_idx<<"  "<<cur_leaf_node->data_record->key<<"  "<<endl;
		//cout<<sec_idx<<"  "<<cur_leaf_node->data_record->value<<"  "<<endl;

		last_node_info->next_node_info = new_node_info;

		last_node_info = last_node_info->next_node_info;
		cur_leaf_node = cur_leaf_node->back_node;

		secLast_info->node_num ++;
	}

	/*For Test*/	
	test_level_info(secLast_info, OFF);

	LEVEL_INFO *root_level_info;

	root_level_info = generate_rest_level(secLast_info);

	INDEX_NODE *ret_node;
	ret_node = root_level_info->node_info_addr->node_addr;

	free_level_info_mem(root_level_info);	
	root_level_info = NULL;

	/*For Test. The tree is drawn correctly here.*/
	//draw_a_tree(ret_node);
	
	return(ret_node);
}

/*Generate non leaf nodes.*/
LEVEL_INFO * 
generate_level(LEVEL_INFO *level_info)
{
	int left_node = level_info->node_num, ret_num = 0, mark = 0;
	NODE_INFO *ret_node_info, *cur_node_info, *new_node_info;
	INDEX_NODE *new_node;
	
	LEVEL_INFO *ret_level_info;

	ret_level_info = create_level_info_mem();
	ret_node_info = create_node_info_mem();
	
	cur_node_info = level_info->node_info_addr;
	ret_level_info->node_info_addr = ret_node_info;	
	ret_level_info->node_num = 0;

	while(left_node)
	{
		//For Monitor When Testing
		//cout<<left_node<<endl;

		if(left_node > MAXKEYNUM + 1)
		{
			new_node = create_index_mem();
			new_node_info = create_node_info_mem();

			while(mark != LEAST_P_NUM - 1)
			{
				new_node->p_node[mark] = cur_node_info->node_addr;
				new_node->key_list[mark] = cur_node_info->node_addr->pri_key;
				mark ++;
				cur_node_info = cur_node_info->next_node_info;
			}

			new_node->p_node[mark] = cur_node_info->node_addr;
			new_node->pri_key = cur_node_info->node_addr->pri_key;
			new_node->key_num = LEAST_P_NUM - 1;
			new_node->node_type = INDEX_NODE_TYPE;
			//if(!set_node_lock(new_node, NO_LOCK))
			//{
			//	return;
			//}
			
			new_node_info->node_addr = new_node;
			new_node_info->node_type = INDEX_NODE_TYPE;
			new_node_info->next_node_info = NULL;

			ret_node_info->next_node_info =  new_node_info;
			ret_node_info = ret_node_info->next_node_info;
			
			ret_num ++;
			left_node -= mark + 1;
			mark = 0;
			cur_node_info = cur_node_info->next_node_info;
		}
		else
		{
			new_node = create_index_mem();
			new_node_info = create_node_info_mem();

                        while(mark != left_node - 1)
                        {
                                new_node->p_node[mark] = cur_node_info->node_addr;
                                new_node->key_list[mark] = cur_node_info->node_addr->pri_key;
                                mark ++;
                                cur_node_info = cur_node_info->next_node_info;
                        }

                        new_node->p_node[mark] = cur_node_info->node_addr;
                        new_node->pri_key = cur_node_info->node_addr->pri_key;
                        new_node->key_num = left_node - 1;
			new_node->node_type = INDEX_NODE_TYPE;
                        //if(!set_node_lock(new_node, NO_LOCK))
			//{
			//	return;
			//}

                        new_node_info->node_addr = new_node;
                        new_node_info->node_type = INDEX_NODE_TYPE;
                        new_node_info->next_node_info = NULL;
			ret_node_info->next_node_info = new_node_info;

                        ret_num ++;
			left_node -= left_node;
			mark = 0;
		}
	}
	
	ret_level_info->node_num = ret_num;

	/*Remove empty header.*/
	NODE_INFO *null_ptr;

	null_ptr = ret_level_info->node_info_addr;
	ret_level_info->node_info_addr = ret_level_info->node_info_addr->next_node_info;

	free(null_ptr);
	null_ptr = NULL;

	/*For Test*/
	test_node_info(ret_level_info->node_info_addr, OFF);
	test_level_info(ret_level_info, OFF);
	
	free_level_info_mem(level_info);

	return(ret_level_info);
}


/*Generate rest levels.*/
LEVEL_INFO *
generate_rest_level(LEVEL_INFO *level_info)
{
	LEVEL_INFO *ret_level_info;
	ret_level_info = level_info;
	while(ret_level_info->node_num != 1)
	{
		ret_level_info = generate_level(ret_level_info);
	
		/*For Test*/
		test_level_info(ret_level_info, OFF); 
	}
	return(ret_level_info);
}

/*This is used to merge sort two sorted data list*/
/*Two input data record list must have been sorted.*/
DATA_RECORD_LIST *
merge_sort(DATA_RECORD_LIST* data_list_1, DATA_RECORD_LIST *data_list_2)
{
	DATA_RECORD_LIST *cur_base, *cur_comp, *ret, *mov;
	
	if(data_list_1->data_record->key <= data_list_2->data_record->key)	
	{
		ret = data_list_1;
		cur_comp = data_list_2;
		cur_base = data_list_1;
	}
	else
	{
		ret = data_list_2;
                cur_comp = data_list_1;
                cur_base = data_list_2;
	}
	
	mov = cur_base->next_data;

	while(mov)
	{
		if(mov->data_record->key <= cur_comp->data_record->key)
		{
			cur_base = cur_base->next_data;
			mov = mov->next_data;
		}
		else
		{
			cur_base->next_data = cur_comp;
			cur_base = cur_base->next_data;
			cur_comp = mov;
			mov = cur_base->next_data;
		}
	}
	
	return(ret);
}
