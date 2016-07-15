#include<iostream>
#include"./BIndex.h"
#include<math.h>
#include<stdlib.h>

using namespace std;


/*This file is used for supporting functions.*/


unsigned char node_type(INDEX_NODE *);
IDX_BOOL is_leaf_node(INDEX_NODE *);
LEAF_NODE *create_leaf_mem(void);
INDEX_NODE *create_index_mem(void);
NODE_INFO *create_node_info_mem(void);
LEAF_NODE *create_leaf_mem(void);
INDEX_NODE *create_index_mem(void);
LEVEL_INFO *create_level_info_mem(void);
INDEX_NODE_LINK *create_index_node_link_mem(void);
NODE_PATH_INFO *create_node_path_info_mem(void);
SINGLE_LEAF_LINK *create_single_leaf_link_mem(void);
DOUBLE_LEAF_LINK *create_double_leaf_link_mem(void);
void free_index_node_link_mem(INDEX_NODE_LINK *);
void free_node_path_info_mem(NODE_PATH_INFO *);
void free_node_analyze_res_mem(NODE_ANALYZE_RES *);
void free_single_leaf_link_mem(SINGLE_LEAF_LINK *);
NODE_ANALYZE_RES *create_node_analyze_res_mem(void);
void free_double_leaf_link_mem(DOUBLE_LEAF_LINK *);
void free_level_info_mem(LEVEL_INFO *);
DATA_RECORD_LIST *create_data_record_list_mem(void);
void free_data_record_list_mem(DATA_RECORD_LIST *);
DATA_RECORD *create_data_record_mem(void);
LOG_LIST *create_log_list_mem(void);
LOG_INFO *create_log_info_mem(void);
void free_log_list_mem(LOG_LIST *);
void free_log_info_mem(LOG_INFO *);
DATA_INFO *create_data_info_mem(void);
void free_data_info_mem(DATA_INFO *);
DATA_INFO *get_data_info(DATA_RECORD_LIST *);
IDX_BOOL check_duplicate_data(DATA_RECORD_LIST *, SORT_STAT);
SUB_TREE_LIST_INFO *create_sub_tree_list_info__mem(void);
void free_sub_tree_info_mem(SUB_TREE_LIST_INFO *);
SUB_TREE_INFO_LINK *create_sub_tree_info_link_mem(void);
void free_sub_tree_info_link_mem(SUB_TREE_INFO_LINK *);
//DEPTH_INFO *create_depth_info_mem(void);
//void free_depth_info_mem(DEPTH_INFO *);


/*This is used to determine the node type.*/
unsigned char
node_type(INDEX_NODE *target_node)
{
        if(!target_node->node_type)
        {
                return(LEAF_NODE_TYPE);
        }
        else
        {
                return(INDEX_NODE_TYPE);
        }
}

/*This is used to determine a node whether a leaf node or not.*/
IDX_BOOL
is_leaf_node(INDEX_NODE *target_node)
{
        if(node_type(target_node) == LEAF_NODE_TYPE)
        {
                return(TRUE);
        }
        else
        {
                return(FALSE);
        }
}

/*This is used to get data info of a data list.*/
DATA_INFO *
get_data_info(DATA_RECORD_LIST *data_list)
{
	DATA_INFO *res = create_data_info_mem();
	res->data_list = data_list;
	res->num = 0;
	res->max_key = 0;
	res->min_key = KEY_RANGE;
	
	DATA_RECORD_LIST *cur;
	cur = data_list;
	
	while(cur)
	{
		if((cur->data_record->key > res->max_key)
			&& (cur->data_record->key <= KEY_RANGE))
		{
			res->max_key = cur->data_record->key;
		}

		if((cur->data_record->key < res->min_key)
                        && (cur->data_record->key > 0))
                {
                        res->min_key = cur->data_record->key;
                }
	
		cur = cur->next_data;	
		res->num ++;
	}

	return(res);
}

/*This is used to check duplicated keys in a data record list.*/
IDX_BOOL
check_duplicate_data(DATA_RECORD_LIST *data_list, SORT_STAT sort_unsort)
{
	if(!sort_unsort)
	{
		DATA_RECORD_LIST *sorted_list;
		DATA_INFO *data_info = get_data_info(data_list);
		sorted_list = quick_sort(data_list, data_info->num);
		free_data_info_mem(data_info);
		check_duplicate_data(sorted_list, SORT);
	}
	else
	{
		int last_key;
		DATA_RECORD_LIST *cur;
		cur = data_list;
		last_key = cur->data_record->key;
		cur = cur->next_data;		

		if(!cur)
		{
			cout<<"At least two data records needed!"<<endl;
			return(FALSE);
		}

		IDX_BOOL res = FALSE;
		while(cur)
		{
			if(cur->data_record->key == last_key)
			{
				res = TRUE;
				break;
			}
			else
			{
				last_key = cur->data_record->key;
				cur = cur->next_data;
			}
		}

		return(res);
	}
}

/*This is used for memory allocate for new leaf node.*/
LEAF_NODE *
create_leaf_mem(void)
{
	LEAF_NODE *ret;
	ret = (LEAF_NODE *)malloc(sizeof(LEAF_NODE));
	return ret;
}

/*This is used for memory allocate for new index node.*/
INDEX_NODE *
create_index_mem(void)
{
        INDEX_NODE *ret;
        ret = (INDEX_NODE *)malloc(sizeof(INDEX_NODE));
        return ret;
}

/*This is used for memory allocate for new node info.*/
NODE_INFO *
create_node_info_mem(void)
{
	NODE_INFO *ret;
	ret = (NODE_INFO *)malloc(sizeof(NODE_INFO));
	return ret;
}

/*This is used for memory allocate for new level info.*/
LEVEL_INFO *
create_level_info_mem(void)
{
	LEVEL_INFO *ret;
	ret = (LEVEL_INFO *)malloc(sizeof(LEVEL_INFO));
	return ret;
}

/*This is used for memory allocate for new index node link.*/
INDEX_NODE_LINK *
create_index_node_link_mem(void)
{
	INDEX_NODE_LINK *ret;
	ret = (INDEX_NODE_LINK *)malloc(sizeof(INDEX_NODE_LINK));
	return ret;
}

/*This is used for memory allocate for new node path info.*/
NODE_PATH_INFO *
create_node_path_info_mem(void)
{
	NODE_PATH_INFO *ret;
	ret = (NODE_PATH_INFO *)malloc(sizeof(NODE_PATH_INFO));
	return ret;
}

/*This is used for memory allocate for new single leaf link.*/
SINGLE_LEAF_LINK *
create_single_leaf_link_mem(void)
{
	SINGLE_LEAF_LINK *ret_link;
	ret_link = (SINGLE_LEAF_LINK *)malloc(sizeof(SINGLE_LEAF_LINK));
	return ret_link;
}

/*This is used for memory allocate for new double leaf link.*/
DOUBLE_LEAF_LINK *
create_double_leaf_link_mem(void)
{
        DOUBLE_LEAF_LINK *ret_link;
        ret_link = (DOUBLE_LEAF_LINK *)malloc(sizeof(DOUBLE_LEAF_LINK));
        return ret_link;
}

/*This is used for free index node link memory.*/
void 
free_index_node_link_mem(INDEX_NODE_LINK *index_node_link)
{
	INDEX_NODE_LINK *cur_node, *target;
	cur_node = index_node_link;
	while(cur_node)
	{
		target = cur_node;
		cur_node = cur_node->next_link;
		free(target);
	}

	target = NULL;
	index_node_link = NULL;
	return;
}

/*This is used for free node path info memory.*/
void 
free_node_path_info_mem(NODE_PATH_INFO *node_path_info)
{
	free_index_node_link_mem(node_path_info->index_node_link);
	free(node_path_info);
	node_path_info = NULL;
	return;
}

/*This is used for free node analyze result memory.*/
void
free_node_analyze_res_mem(NODE_ANALYZE_RES *analyze_res)
{
	free_index_node_link_mem(analyze_res->index_link);
	free_single_leaf_link_mem(analyze_res->leaf_link);
	free(analyze_res);
	analyze_res = NULL;
	return;
}

/*Thi is used for free single leaf link memory.*/
void
free_single_leaf_link_mem(SINGLE_LEAF_LINK *leaf_link)
{
	SINGLE_LEAF_LINK *cur_link, *target;
	cur_link = leaf_link;
	while(cur_link)
	{
		target = cur_link;
		cur_link = cur_link->next_link;
		free(target);
	}
	target = NULL;
	leaf_link = NULL;
	return;
}

/*This is used for create node analyze result memory.*/
NODE_ANALYZE_RES *
create_node_analyze_res_mem(void)
{
	NODE_ANALYZE_RES *ret;
	ret = (NODE_ANALYZE_RES *)malloc(sizeof(NODE_ANALYZE_RES));
	return ret;
}

/*This is used for free double leaf link.*/
void 
free_double_leaf_link_mem(DOUBLE_LEAF_LINK *leaf_link)
{
	DOUBLE_LEAF_LINK *cur_link, *target;
	cur_link = leaf_link;
        while(cur_link)
        {
                target = cur_link;
                cur_link = cur_link->next_link;
                free(target);
        }
        target = NULL;
        leaf_link = NULL;
        return;
}

/*This is used for free level info memory.*/
void
free_level_info_mem(LEVEL_INFO *level_info)
{
	NODE_INFO *cur_node_info, *target_node_info;
	cur_node_info = level_info->node_info_addr;
	
	while(cur_node_info)
	{
		target_node_info = cur_node_info;
		cur_node_info = cur_node_info->next_node_info;
		free(target_node_info);
	}
	
	target_node_info = NULL;
	level_info = NULL;
	return;
}

/*This is used for create data record list memory.*/
DATA_RECORD_LIST *
create_data_record_list_mem(void)
{
	DATA_RECORD_LIST *ret;
	ret = (DATA_RECORD_LIST *)malloc(sizeof(DATA_RECORD_LIST));
	return ret;
}

/*This is used for free data record list memory.*/
void
free_data_record_list_mem(DATA_RECORD_LIST *data_list)
{
	DATA_RECORD_LIST *cur_data, *target_data;
	cur_data = data_list;
	
	while(cur_data)
	{
		target_data = cur_data;
		cur_data = cur_data->next_data;
		free(target_data);
	}

	target_data = NULL;
	cur_data = NULL;
	data_list = NULL;
	return;
}

/*This is used to create data record memeory.*/
DATA_RECORD *
create_data_record_mem(void)
{
	DATA_RECORD *ret;
	ret = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
	return ret;
}

/*This is used to free a tree's memory.*/
void
free_a_tree_mem(INDEX_NODE *root)
{
	if(is_leaf_node(root))
	{
		LEAF_NODE *leaf;
		leaf = (LEAF_NODE *)root;
		free(leaf);
		leaf = NULL;
		root = NULL;
		return;
	}
	else
	{
		int i = 0, key_num = 0;
		key_num = root->key_num;
		for(i = 0; i != key_num+1; ++i)
		{
			INDEX_NODE *sub_root;
			sub_root = (INDEX_NODE *)root->p_node[i];
			free_a_tree_mem(sub_root);
			sub_root = NULL;
		}
		free(root);
		root = NULL;
		return;
	}
}

/*This is used to create log list memory.*/
LOG_LIST *
create_log_list_mem(void)
{
	LOG_LIST *ret;
	ret = (LOG_LIST *)malloc(sizeof(LOG_LIST));
	return ret;
}

/*This is used to create log info memory.*/
LOG_INFO *
create_log_info_mem(void)
{
	LOG_INFO *ret;
	ret = (LOG_INFO *)malloc(sizeof(LOG_INFO));
	return ret;
}

/*This is used to free log list memory.*/
void
free_log_list_mem(LOG_LIST *log_list)
{
	LOG_LIST *cur_log, *target_log;
	cur_log = log_list;
	
	while(cur_log)
	{
		target_log = cur_log;
		cur_log = cur_log->next_log;
		free(target_log);
	}

	cur_log = NULL;
	target_log = NULL;
	log_list = NULL;
	return;
}

/*This is used to free log info memory.*/
void
free_log_info_mem(LOG_INFO *log_info)
{
	free(log_info->log_list);
	free(log_info);
	log_info = NULL;
	return;
}

/*This is used to create data info memory.*/
DATA_INFO *
create_data_info_mem(void)
{
	DATA_INFO *ret;
	ret = (DATA_INFO *)malloc(sizeof(DATA_INFO));
	return ret;
}

/*This is used to free data info memory.*/
void
free_data_info_mem(DATA_INFO *data_info)
{
	free_data_record_list_mem(data_info->data_list);
	free(data_info);
	data_info = NULL;
	return;
}

/*This is used to create sub tree list info memory*/
SUB_TREE_LIST_INFO *
create_sub_tree_list_info_mem(void)
{
	SUB_TREE_LIST_INFO *ret;
	ret = (SUB_TREE_LIST_INFO *)malloc(sizeof(SUB_TREE_LIST_INFO));
	return ret;
}

/*This is used to free sub tree list info memory.*/
void
free_sub_tree_list_info_mem(SUB_TREE_LIST_INFO *sub)
{
	free_data_record_list_mem(sub->data_list);
	free_sub_tree_info_link_mem(sub->sub_tree_info_link);
	free(sub);
	sub = NULL;
	return;
}

/*This is used to create sub tree info link memory.*/
SUB_TREE_INFO_LINK *
create_sub_tree_info_link_mem(void)
{
	SUB_TREE_INFO_LINK *ret;
	ret = (SUB_TREE_INFO_LINK *)malloc(sizeof(SUB_TREE_INFO_LINK));
	return ret;
}

/*This is used to free sub tree info link memory.*/
void
free_sub_tree_info_link_mem(SUB_TREE_INFO_LINK *link)
{
	SUB_TREE_INFO_LINK *cur_link, *target_link;
	cur_link = link;

	while(cur_link)
	{
		target_link = cur_link;
		cur_link = cur_link->next_link;
		free(target_link);
	}
	
	target_link = NULL;
	cur_link = NULL;
	link = NULL;
	return;
}

/*This is used to create depth info mem.*/
/*
DEPTH_INFO *
create_depth_info_mem(void)
{
	DEPTH_INFO *ret;
	ret = (DEPTH_INFO *)malloc(sizeof(DEPTH_INFO));
	return ret;
}
*/

/*This is used to free depth info mem.*/
/*
void
free_depth_info_mem(DEPTH_INFO *depth)
{
	free(depth);
	depth = NULL;
	return;
}
*/
