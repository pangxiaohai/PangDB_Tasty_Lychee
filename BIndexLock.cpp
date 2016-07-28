#include<iostream>
#include"./BIndex.h"
#include<math.h>
#include<stdlib.h>
#include<time.h>
#include<sys/time.h>


using namespace std;


LOCK_INFO *lock_info = NULL;
LOCK_RECORD *lock_record = NULL;


/*This file is used for lock manager.*/


RUN_RESULT apply_read_node_link_lock(PID, INDEX_NODE_LINK *);
void add_lock_record(PID, INDEX_NODE *, unsigned char);
void clean_overtime_lock(void);
void remove_lock_info(PID, INDEX_NODE *);
void free_read_node_link_lock(PID, INDEX_NODE_LINK *);
void remove_lock_record(PID, INDEX_NODE *, unsigned char);
IDX_BOOL can_apply_write_lock(PID, INDEX_NODE *);
RUN_RESULT apply_write_links_lock(PID, NODE_ANALYZE_RES *);



/*This is used to apply read locks for a node.*/
RUN_RESULT
apply_read_node_lock(PID user, INDEX_NODE *node)
{
	if(node->node_lock == WRITE_LOCK)
	{
		return(RUN_FAILED);
	}

	if(!lock_info)
	{
		lock_info = create_lock_info_mem();
		lock_info->node = node;
		node->node_lock = READ_LOCK;
		lock_info->user_list = create_user_list_mem();
		lock_info->user_list->user = user;
		lock_info->user_list->next_user = NULL;
		lock_info->next_info = NULL;

		add_lock_record(user, node, READ_LOCK);
	}

	LOCK_INFO *cur_info = lock_info, *new_info;
	IDX_BOOL found = FALSE, duplicated = FALSE;
	USER_LIST *cur_user;
	
	cur_info = lock_info;
	while(cur_info)
	{	
		if(cur_info->node == node)
		{
			found = TRUE;
			break;
		}
			
		if(!cur_info->next_info ||
			cur_info->next_info->node->pri_key < node->pri_key)
		{
			found = FALSE;
			break;
		}
		cur_info = cur_info->next_info;
	}

	if(!found)
	{
		new_info = create_lock_info_mem();
		new_info->node = node;
		node->node_lock = READ_LOCK;
		new_info->user_list = create_user_list_mem();
		new_info->user_list->user = user;
		new_info->user_list->next_user = NULL;
		new_info->next_info = cur_info->next_info;
		cur_info->next_info = new_info;

		add_lock_record(user, node, READ_LOCK);
	}
	else
	{
		cur_user = cur_info->user_list;
		while(cur_user->next_user)
		{
			if(cur_user->user == user)
			{
				duplicated = TRUE;
				break;
			}
			cur_user = cur_user->next_user;
		}
			
		if(!duplicated)
		{
			cur_user->next_user = create_user_list_mem();
			cur_user->next_user->user = user;
			cur_user->next_user->next_user = NULL;
			add_lock_record(user, node, READ_LOCK);
		}
	}

	return(RUN_SUCCESS);
}

/*This is used to add lock record.*/
void
add_lock_record(PID user, INDEX_NODE *node, unsigned char lock_type)
{
        struct timeval now_t;
        gettimeofday(&now_t, NULL);
        uint64_t now = now_t.tv_sec * 1000 + now_t.tv_usec / 1000;

	if(!lock_record)
	{
		lock_record = create_lock_record_mem();
		lock_record->user = user;
		lock_record->node = node;
		lock_record->lock_type = lock_type;
		lock_record->next_record = NULL;
		lock_record->time = now;
		return;
	}

	LOCK_RECORD *cur_record, *new_record;
	cur_record = lock_record;

	while(cur_record->next_record)
	{
		if((cur_record->user == user) && (cur_record->node == node))
		{
			/*Would not change the timestamp.*/
		
			if(lock_type == READ_LOCK)
			{
				return;
			}
			
			if(lock_type == WRITE_LOCK)
			{
				cur_record->lock_type = WRITE_LOCK;
				return;
			}
		}
		cur_record = cur_record->next_record;
	}

	/*Add new recordi at the end.*/
	new_record = create_lock_record_mem();
	new_record->user = user;
	new_record->node = node;
	new_record->lock_type = lock_type;
	new_record->time = now;
	new_record->next_record = NULL;
	cur_record->next_record = new_record;
	return;
}

/*This is used to auto clean overtime locks.*/
void
clean_overtime_lock(void)
{
	if(!lock_record)
	{
		return;
	}

	LOCK_RECORD *cur_record, *target;
	cur_record = lock_record;
	struct timeval now_t;
	uint64_t now;

	while(cur_record)
	{
        	gettimeofday(&now_t, NULL);
		now = now_t.tv_sec * 1000 + now_t.tv_usec / 1000;

		if(now <= (cur_record->time + MAXLOCKTIME))
		{
			return;
		}
		else
		{
			target = cur_record;
			lock_record = lock_record->next_record;
			cur_record = cur_record->next_record;
	
			/*Write lock info will not be written into lock info.*/
			if(target->lock_type == WRITE_LOCK)
			{
				target->node->node_lock = NO_LOCK;
			}
			else
			{
				remove_lock_info(target->user, target->node);
			}
		
			/*Records are sorted. So all former records have been removed.*/	
			free(target);
			target = NULL;
		}
	}
}

/*This is used to free a lock.*/
void
remove_lock_info(PID user, INDEX_NODE *node)
{
	if(!lock_info)
	{
		return;
	}

	LOCK_INFO *cur, *target, *last;
	USER_LIST *cur_user, *target_user, *last_user;
	cur = lock_info;
	last = lock_info;

	/*Only one node locked.*/
	if(!cur->next_info)
	{
		/*Node found*/
		if(cur->node == node)
		{
			cur_user = cur->user_list;
			/*Only one user*/
			if(!cur_user->next_user)
			{
				if(cur_user->user == user)
				{
					free(cur_user);
					if(node->node_lock != WRITE_LOCK)
					{
						node->node_lock = NO_LOCK;
					}
					free(cur);
					cur_user = NULL;
					cur = NULL;
					lock_info = NULL;
					return;
				}
				else
				{
					return;
				}
			}
			else
			{
				last_user = cur_user;
				cur_user = cur_user->next_user;
				while(cur_user)
				{
					if(cur_user->user == user)
					{
						last_user->next_user = cur_user->next_user;
						free(cur_user);
						cur_user = NULL;
						return;
					}
					last_user = cur_user;
                                	cur_user = cur_user->next_user;
				}
			}
		}
		else
		{
			return;
		}
	}
	
	last = cur;
	cur = cur->next_info;
	while(cur)
	{
		/*Not found.*/
		if(cur->node->pri_key > node->pri_key)
		{
			return;
		}
		
		if(cur->node == node)
		{
			cur_user = cur->user_list;
                        /*Only one user*/
                        if(!cur_user->next_user)
                        {
                                if(cur_user->user == user)
                                {
                                        free(cur_user);
					if(node->node_lock != WRITE_LOCK)
                                        {
						node->node_lock = NO_LOCK;
					}
					last->next_info = cur->next_info;
                                        free(cur);
                                        cur_user = NULL;
                                        cur = NULL;
                                        return;
                                }
                                else
                                {
                                        return;
                                }
                        }
                        else
                        {
                                last_user = cur_user;
                                cur_user = cur_user->next_user;
                                while(cur_user)
                                {
                                        if(cur_user->user == user)
                                        {
                                                last_user->next_user = cur_user->next_user;
                                                free(cur_user);
                                                cur_user = NULL;
                                                return;
                                        }
                                        last_user = cur_user;
                                        cur_user = cur_user->next_user;
                                }
                        }
		}
		
		last = cur;
        	cur = cur->next_info;
	}

	return;
}

/*This is used to free read lock for index node link.*/
void 
free_read_node_link_lock(PID user, INDEX_NODE_LINK *index_link)
{
	INDEX_NODE_LINK *cur_link = index_link;
	while(cur_link)
	{
		remove_lock_info(user, cur_link->index_node);
		remove_lock_record(user, cur_link->index_node, READ_LOCK);
	}

	return;
}

/*This is used to rmove a lock record.*/
void
remove_lock_record(PID user, INDEX_NODE *node, unsigned char lock_type)
{
	LOCK_RECORD *cur, *last;
	if(!lock_record)
	{
		return;
	}

	if(!(lock_record->next_record))
	{
		if((lock_record->user == user) && (lock_record->node == node))
		{
			if((lock_record->lock_type == WRITE_LOCK)
				 && (lock_type == READ_LOCK))
			{
				return;
			}
			
			node->node_lock = NO_LOCK;
			free(lock_record);
			lock_record = NULL;
		}
		return;
	}

	cur = lock_record->next_record;
	last = lock_record;

	while(cur)
	{
		if((cur->user == user) && (cur->node == node))
                {
                        if((cur->lock_type == WRITE_LOCK)
                                 && (lock_type == READ_LOCK))
                        {
                                return;
                        }

                        node->node_lock = NO_LOCK;
			last->next_record = cur->next_record;
                        free(lock_record);
                        lock_record = NULL;
                }	

		last = cur;
		cur = cur->next_record;
	}
	return;
}

/*This is used to apply write link lock.*/
RUN_RESULT
apply_write_links_lock(PID user, NODE_ANALYZE_RES *res)
{
	SINGLE_LEAF_LINK *cur_leaf = res->leaf_link;
	INDEX_NODE_LINK *cur_link = res->index_link;

	INDEX_NODE *node;
	while(cur_leaf)
	{
		node = (INDEX_NODE *)(cur_leaf->leaf_node);
		if(!can_apply_write_lock(user, node))
		{
			return(RUN_FAILED);
		}
		cur_leaf = cur_leaf->next_link;
	}

	while(cur_link)
	{
		if(!can_apply_write_lock(user, cur_link->index_node))
                {
                        return(RUN_FAILED);
                }
                cur_link = cur_link->next_link;
	}

	cur_leaf = res->leaf_link;
	cur_link = res->index_link;

	while(cur_leaf)
	{
		cur_leaf->leaf_node->node_lock = WRITE_LOCK;
		node = (INDEX_NODE *)(cur_leaf->leaf_node);
		add_lock_record(user, node, WRITE_LOCK);
		cur_leaf = cur_leaf->next_link;
	}

	while(cur_link)
	{
		cur_link->index_node->node_lock = WRITE_LOCK;
		add_lock_record(user, cur_link->index_node, WRITE_LOCK);
                cur_link = cur_link->next_link;
	}
	return(RUN_SUCCESS);
}

/*This is used to check whether a node can apply write lock.*/
IDX_BOOL
can_apply_write_lock(PID user, INDEX_NODE *node)
{
	/*If there is no lock info for this node,
	node_lock should be NO_LOCK.*/
	if(node->node_lock == NO_LOCK)
	{
		return(TRUE);
	}

	if(node->node_lock == READ_LOCK)
	{
		LOCK_INFO *cur = lock_info;
		while(cur)
		{
			if((cur->node == node) && (cur->user_list->user == user)
				&& (!cur->user_list->next_user))
			{
				return(TRUE);
			}
			cur = cur->next_info;
		}
	}

	return(FALSE);
}
