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
