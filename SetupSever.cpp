#include<iostream>
#include"./BIndex.h"
#include<math.h>
#include<stdlib.h>
#include<string>
#include<string.h>
#include<pthread.h>
#include<unistd.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<arpa/inet.h>


using namespace std;


void *setup_consol(void *);
void operate_exist(INDEX_NODE *, USER_INFO*);
void *setup_auto_backup(void *);
IDX_BOOL already_login(PID);
void exit_client(USER_INFO *);


/*Declare run operation functions.*/
void run_read_data_operation(INDEX_NODE *, USER_INFO *);
void run_insert_data_operation(INDEX_NODE *, USER_INFO *);
void run_delete_data_operation(INDEX_NODE *, USER_INFO *);
void run_range_search_operation(INDEX_NODE *, USER_INFO *);
void run_update_data_operation(INDEX_NODE *, USER_INFO *);
void run_top_bottom_search_operation(INDEX_NODE *, USER_INFO *, int);
void run_batch_insert_operation(INDEX_NODE *, USER_INFO *);
void run_batch_delete_operation(INDEX_NODE *, USER_INFO *);
void run_show_data_draw_tree_operation(INDEX_NODE *);
void run_show_logs(INDEX_NODE *);
INDEX_NODE *run_diagnose_and_repair(INDEX_NODE *);


pthread_t backup_thread;
pthread_t lock_clean_thread;

INDEX_NODE *test_root;
USER_INFO_LIST *user_info_list = NULL;


int main(void)
{
	cout<<"PangDB is setting up now...\n"<<endl;

	if(!create_sys_files())
	{
		cout<<"Create system files failed!\n"<<endl;
		return -1;
	}
	else
	{
		cout<<"Create system files successed!\n"<<endl;
	}
	
	pthread_t client_thread;

	string id_input = "Please input your user id:";
	char buf[6];
	buf[5] = '\0';
	PID user_id;
	
	test_root = test_init();
        pthread_create(&backup_thread,NULL, setup_auto_backup, (void *)test_root);
        pthread_create(&lock_clean_thread,NULL, auto_clean_lock, NULL);

	struct sockaddr_in server_addr;
	struct sockaddr_in client_addr;

	int server_sockfd, client_sockfd;
	unsigned int client_len;

	bzero((void *)&server_addr, sizeof(server_addr));
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = INADDR_ANY;
	server_addr.sin_port = htons(PORT);

	server_sockfd = socket(AF_INET, SOCK_STREAM, 0);
	bind(server_sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr));
	
	listen(server_sockfd, CLIENT_NUM);
	USER_INFO *user_info = NULL;

	cout<<"Waiting..."<<endl;

	char con_signal;
	
	while(1)
	{
		client_sockfd = accept(server_sockfd, (struct sockaddr *)&client_addr, &client_len);

		cout<<"Connected..."<<endl;
		read(client_sockfd, &con_signal, 1);

		write(client_sockfd, RECV_CMD, 8);
		read(client_sockfd, buf, 5);
		sscanf(buf, "%5d", &user_id);
		
		if(!strcmp(buf, ERR_CMD))
		{
			close(client_sockfd);
		}	
		else if(already_login(user_id))
		{
			write(client_sockfd, ALREADY_LOG, 8);
                     	close(client_sockfd);
		}
		else
		{
			user_info = (USER_INFO *)malloc(sizeof(USER_INFO));
			user_info->user_id = user_id;
			user_info->sockfd = client_sockfd;
		
			pthread_create(&client_thread, NULL, setup_consol, (void *)user_info);
		}
	}

	return 0;
}

/*This is used to check login.*/
IDX_BOOL 
already_login(PID user)
{
	USER_INFO_LIST *cur;
	cur = user_info_list;

	while(cur)
	{
		if(cur->user_info->user_id == user)
		{
			return(TRUE);
		}
		cur = cur->next_user;
	}
	return(FALSE);
}

/*This is used to setup a consol.*/
void * 
setup_consol(void *arg)
{
	USER_INFO *user_info = (USER_INFO *)arg;

	USER_INFO_LIST *new_user = NULL;
	new_user = (USER_INFO_LIST *)malloc(sizeof(USER_INFO_LIST));
        new_user->user_info = user_info;	

	new_user->thread = pthread_self();
        new_user->next_user = user_info_list;
        user_info_list = new_user;

	PID user = user_info->user_id;
	int client_sockfd = user_info->sockfd;
	char choose[2];
	choose[1] = '\0';

	write(client_sockfd, DISPLAY_CMD, 8);

	int input;

	while(1)
	{
		read(client_sockfd, choose, 1);
		sscanf(choose, "%d", &input);
		switch(input)
		{
			case 1:
				run_exist(test_root);
				write(client_sockfd, RIGHT_SIGNAL, 6);
				break;
			case 2:
				operate_exist(test_root, user_info);
				break;
			case 3:
				exit_client(user_info);
			default:
				write(client_sockfd, INVALID_CMD, 8); 
				break;
		}
	}
}

/*This is used to exit a client.*/
void
exit_client(USER_INFO *user_info)
{
	USER_INFO_LIST *cur, *last;
        cur = user_info_list;
	int client_sockfd = user_info->sockfd;

	if(user_info_list->user_info == user_info)
	{
		user_info_list = user_info_list->next_user;
		free(user_info);
		user_info = NULL;
		free(cur);
		cur = NULL;
	}
	else
	{
		last = user_info_list;
		cur = user_info_list->next_user;

        	while(cur)
	        {
        	        if(cur->user_info == user_info)
                	{
				last->next_user = cur->next_user;
				free(user_info);
				user_info = NULL;
				free(cur);
				cur = NULL;
				break;
			}
			last = cur;
			cur = cur->next_user;
		}
	}

	write(client_sockfd, QUIT, 5);
	close(client_sockfd);
	pthread_exit((void *)NULL);
	return;
}

/*This is used to operations on exist db.*/
void
operate_exist(INDEX_NODE *root, USER_INFO *user_info)
{
	char choose[2];
	choose[1] = '\0';
	int input;
	while(1)
	{
		read(user_info->sockfd, choose, 1);
		sscanf(choose, "%d", &input);

		switch(input)
		{
			case 0:
				run_read_data_operation(root, user_info);
				break;
			case 1:
				run_insert_data_operation(root, user_info);
				break;
			case 2:
				run_delete_data_operation(root, user_info);
				break;
			case 3:
                                run_range_search_operation(root, user_info);
                                break;
                        case 4:
                                run_update_data_operation(root, user_info);
                                break;
                        case 5:
                                run_top_bottom_search_operation(root, user_info, 1);
                                break;
			case 6:
                                run_top_bottom_search_operation(root, user_info, 0);
                                break;
                        case 7:
                                run_batch_insert_operation(root, user_info);
                                break;
                        case 8:
                                run_batch_delete_operation(root, user_info);
                                break;
			/*Following operations not support for client.*/
			/*
			case 9:
				run_show_data_draw_tree_operation(root);
				operate_consol_display(user_info->sockfd);
				break;
			case 10:
				run_show_logs(root);
				operate_consol_display(user_info->sockfd);
                                break;
			case 11:
                                root = run_diagnose_and_repair(root);
                                operate_consol_display(user_info->sockfd);
                                break;
			*/
			case 9:
				return;
			default:
				write(user_info->sockfd, INVALID_CMD, 8);
				break;
		}
	}
}

/*Following are run operation functions.*/

/*This is used to run read data operation.*/
void 
run_read_data_operation(INDEX_NODE *root, USER_INFO *user_info)
{
	PID user = user_info->user_id;
	int sockfd = user_info->sockfd;

	write(sockfd, BEGIN_CMD, 6);
	
	char *signal = create_n_byte_mem(6);
	signal[5] = '\0';
	read(sockfd, signal, 6);
 
	if(strcmp(signal, RIGHT_SIGNAL))
	{
		free(signal);
		signal = NULL;
		return;
	}
	else
	{
		free(signal);
		signal = NULL;
		write(sockfd, RECV_CMD, 8);
	}	

	char key_buf[11];
	key_buf[10] = '\0';
	read(sockfd, key_buf, 10);

        int input_key;
        sscanf(key_buf, "%10d", &input_key);

	LEAF_NODE *res;
        res = exec_read_value(root, input_key, user);

        if(!res)
	{
		write(sockfd, NO_VALUE, 8);
	}
	else
	{
		write(sockfd, HAS_VALUE, 8);
		char len_buf[4];
		len_buf[3] = '\0';
		sprintf(len_buf, "%3d", res->data_record->len);
		write(sockfd, len_buf, 3);
		write(sockfd, res->data_record->value, res->data_record->len);
	}
	
	return;
}

/*This is used to run insert data operation.*/
void 
run_insert_data_operation(INDEX_NODE *root, USER_INFO *user_info)
{
	PID user = user_info->user_id;
        int sockfd = user_info->sockfd;

	write(sockfd, BEGIN_CMD, 6);

        char *signal = create_n_byte_mem(6);
        signal[5] = '\0';
        read(sockfd, signal, 6);

        if(strcmp(signal, RIGHT_SIGNAL))
        {
                free(signal);
                signal = NULL;
                return;
        }
        else
        {
                free(signal);
                signal = NULL;
                write(sockfd, RECV_CMD, 8);
        }
		
	char len_buf[4];
	len_buf[3] = '\0';
	int len;
	char key_buf[11];
	key_buf[10] = '\0';	
	int input_key;

	read(sockfd, key_buf, 10); 
	sscanf(key_buf, "%10d", &input_key);
	
	read(sockfd, len_buf, 3);
	sscanf(len_buf, "%3d", &len);

        DATA_RECORD *data;
        data = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
        data->key = input_key;
	data->value = create_n_byte_mem(len+1);
	(data->value)[len] = '\0';

	read(sockfd, data->value, len);
	data->len = len;

	if(!insert_node(root, data, user))
	{
		write(sockfd, OP_FAILED, 11); 
		free(data->value);
		free(data);
		data = NULL;
	}
	else
	{
		write(sockfd, OP_SUCCESS, 11);
                free(data->value);
                free(data);
                data = NULL;
	}

	return;
}

/*This is used to run delete data operation.*/
void 
run_delete_data_operation(INDEX_NODE *root, USER_INFO *user_info)
{
	PID user = user_info->user_id;
        int sockfd = user_info->sockfd;

	write(sockfd, BEGIN_CMD, 6);

        char *signal = create_n_byte_mem(6);
        signal[5] = '\0';
        read(sockfd, signal, 6);

        if(strcmp(signal, RIGHT_SIGNAL))
        {
                free(signal);
                signal = NULL;
                return;
        }
        else
        {
                free(signal);
                signal = NULL;
                write(sockfd, RECV_CMD, 8);
        }

	char key_buf[11];
        key_buf[10] = '\0';
        read(sockfd, key_buf, 10);

        int input_key;
        sscanf(key_buf, "%10d", &input_key);

        if(!delete_node(root, input_key, user))
        {
		write(sockfd, OP_FAILED, 11);
        }
        else
        {
		write(sockfd, OP_SUCCESS, 11);
        }
	return;
}

/*This is used to run range search operation.*/
void 
run_range_search_operation(INDEX_NODE *root, USER_INFO *user_info)
{
        PID user = user_info->user_id;
        int sockfd = user_info->sockfd;

	write(sockfd, BEGIN_CMD, 6);

        char *signal = create_n_byte_mem(6);
        signal[5] = '\0';
        read(sockfd, signal, 6);

        if(strcmp(signal, RIGHT_SIGNAL))
        {
                free(signal);
                signal = NULL;
                return;
        }
        else
        {
                free(signal);
                signal = NULL;
                write(sockfd, RECV_CMD, 8);
        }

        char low_buf[11], high_buf[11];
        low_buf[10] = high_buf[10] = '\0';
        read(sockfd, low_buf, 10);
	read(sockfd, high_buf, 10);

        int low, high;
        sscanf(low_buf, "%10d", &low);
	sscanf(high_buf, "%10d", &high);

	char order_buf[4];
	read(sockfd, order_buf, 3);

	ASC_DSC order;

	if(!strcmp(order_buf, ASC_CMD))
	{
		order = ASC;
	}
	else
	{
		order = DSC;
	}

	DOUBLE_LEAF_LINK *result_link;

        result_link = exec_range_search(root, low, high, user);

	int num = 0;
	char num_buf[4];
	num_buf[3] = '\0';	

        if(!result_link)
        {
		sprintf(num_buf, "%3d", num);
		write(sockfd, num_buf, 3);
		return;
        }

        DOUBLE_LEAF_LINK *cur_link = result_link;
	num = 1;
	while(cur_link->next_link)
	{
		cur_link = cur_link->next_link;
		num ++;
	}

	sprintf(num_buf, "%3d", num);
	write(sockfd, num_buf, 3);

	char len_buf[4], key_buf[11];
	len_buf[3] = key_buf[10] = '\0';
        if(order == ASC)
	{
		cur_link = result_link;
	}

	while(cur_link)
	{
		sprintf(key_buf, "%10d", cur_link->leaf_node->data_record->key);
		write(sockfd, key_buf, 10);
		sprintf(len_buf, "%3d", cur_link->leaf_node->data_record->len);
		write(sockfd, len_buf, 3);
		write(sockfd, cur_link->leaf_node->data_record->value, cur_link->leaf_node->data_record->len);
		
		if(order == ASC)
		{
			cur_link = cur_link->next_link;
		}
		else
		{
			cur_link = cur_link->front_link;
		}
	}
	
	free_double_leaf_link_mem(result_link);
	return;
}

/*This is used to run update data operation.*/
void 
run_update_data_operation(INDEX_NODE *root, USER_INFO *user_info)
{
        PID user = user_info->user_id;
        int sockfd = user_info->sockfd;

        write(sockfd, BEGIN_CMD, 6);

        char *signal = create_n_byte_mem(6);
        signal[5] = '\0';
        read(sockfd, signal, 6);

        if(strcmp(signal, RIGHT_SIGNAL))
        {
                free(signal);
                signal = NULL;
                return;
        }
        else
        {
                free(signal);
                signal = NULL;
                write(sockfd, RECV_CMD, 8);
        }

        char key_buf[11];
        key_buf[10] = '\0';
        read(sockfd, key_buf, 10);

        int input_key, len;
        sscanf(key_buf, "%10d", &input_key);

	char len_buf[4];
	len_buf[3] = '\0';

	read(sockfd, len_buf, 3);
	sscanf(len_buf, "%3d", &len);

        DATA_RECORD *data;
        data = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
        data->key = input_key;
        data->value = create_n_byte_mem(len+1);
        (data->value)[len] = '\0';
        read(sockfd, data->value, len);
        data->len = len;
	
	if(!update_value(root, data, user))
	{
                write(sockfd, OP_FAILED, 11);
        }
        else
        {
                write(sockfd, OP_SUCCESS, 11);
        }

	free(data->value);
	free(data);
	data = NULL;
	return;
}

/*This is used to run top or bottom search operation.*/
void 
run_top_bottom_search_operation(INDEX_NODE *root, USER_INFO *user_info, int is_top)
{
        PID user = user_info->user_id;
        int sockfd = user_info->sockfd;

        write(sockfd, BEGIN_CMD, 6);

        char *signal = create_n_byte_mem(6);
        signal[5] = '\0';
        read(sockfd, signal, 6);

        if(strcmp(signal, RIGHT_SIGNAL))
        {
                free(signal);
                signal = NULL;
                return;
        }
        else
        {
                free(signal);
                signal = NULL;
                write(sockfd, RECV_CMD, 8);
        }

	char num_buf[6], order_buf[4];
	num_buf[5] = order_buf[3] = '\0';
	int num;

	read(sockfd, num_buf, 5);
	read(sockfd, order_buf, 3);
	sscanf(num_buf, "%5d", &num);
	
        ASC_DSC order;

        if(!strcmp(order_buf, ASC_CMD))
        {
                order = ASC;
        }
        else
        {
                order = DSC;
        }

	SINGLE_LEAF_LINK *res = NULL;

	int res_num = 0;
	char res_num_buf[6], len_buf[4], key_buf[11];
	res_num_buf[5] = len_buf[3] = key_buf[10] = '\0';

	if(is_top)
	{	
        	res = exec_top_search(root, num, order, user);
	}
	else
	{
		res = exec_bottom_search(root, num, order, user);
	}

        if(!res)
        {
		sprintf(res_num_buf, "%5d", res_num);
		write(sockfd, res_num_buf, 5);
		return;
        }

        SINGLE_LEAF_LINK *cur;
        cur = res;

	res_num = 1;
        while(cur->next_link)
	{
		cur = cur->next_link;
		res_num ++;
	}

	sprintf(res_num_buf, "%5d", res_num);
        write(sockfd, res_num_buf, 5);

	cur = res;
	while(cur)
	{
		sprintf(key_buf, "%10d", cur->leaf_node->data_record->key);
		write(sockfd, key_buf, 10);
		sprintf(len_buf, "%3d", cur->leaf_node->data_record->len);
		write(sockfd, len_buf, 3);
		write(sockfd, cur->leaf_node->data_record->value, cur->leaf_node->data_record->len);
		cur = cur->next_link;
	}

	free_single_leaf_link_mem(res);
	res = NULL;
	
	return;
}

/*This is used to run batch insert operation.*/
void 
run_batch_insert_operation(INDEX_NODE *root, USER_INFO *user_info)
{
        PID user = user_info->user_id;
        int sockfd = user_info->sockfd;

        write(sockfd, BEGIN_CMD, 6);

        char *signal = create_n_byte_mem(6);
        signal[5] = '\0';
        read(sockfd, signal, 6);

        if(strcmp(signal, RIGHT_SIGNAL))
        {
                free(signal);
                signal = NULL;
                return;
        }
        else
        {
                free(signal);
                signal = NULL;
                write(sockfd, RECV_CMD, 8);
        }

        char num_buf[6];
        num_buf[5] = '\0';
        int num;

        read(sockfd, num_buf, 5);
        sscanf(num_buf, "%5d", &num);

	int i = 0, key, len;
	DATA_RECORD_LIST *data_list, *cur_data, *new_data;
	DATA_RECORD *data;
	data_list = NULL;
	char key_buf[11], len_buf[4];
	key_buf[10] = len_buf[3] = '\0';	

	while(i != num)
	{
	        read(sockfd, key_buf, 10);
        	sscanf(key_buf, "%10d", &key);

        	read(sockfd, len_buf, 3);
        	sscanf(len_buf, "%3d", &len);

        	data = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
	        data->key = key;
        	data->value = create_n_byte_mem(len+1);
	        (data->value)[len] = '\0';

        	read(sockfd, data->value, len);
	        data->len = len;
		
		if(i == 0)
		{
			data_list = create_data_record_list_mem();
			data_list->data_record = data;
			cur_data = data_list;
			data_list->next_data = NULL;
		}
		else
		{
                        new_data = create_data_record_list_mem();
                        new_data->data_record = data;
                        new_data->next_data = NULL;
			cur_data->next_data = new_data;
			cur_data = cur_data->next_data;
                }

		i++;
	}

	if(!batch_insert(root, data_list, user))
	{
                write(sockfd, OP_FAILED, 11);
        }
        else
        {
                write(sockfd, OP_SUCCESS, 11);
        }

	return;
}

/*This is used to run batch delete operation.*/
void 
run_batch_delete_operation(INDEX_NODE *root, USER_INFO *user_info)
{
	PID user = user_info->user_id;
        int sockfd = user_info->sockfd;

        write(sockfd, BEGIN_CMD, 6);

        char *signal = create_n_byte_mem(6);
        signal[5] = '\0';
        read(sockfd, signal, 6);

        if(strcmp(signal, RIGHT_SIGNAL))
        {
                free(signal);
                signal = NULL;
                return;
        }
        else
        {
                free(signal);
                signal = NULL;
                write(sockfd, RECV_CMD, 8);
        }

        char low_buf[11], high_buf[11];
        low_buf[10] = high_buf[10] = '\0';
        int low, high;

        read(sockfd, low_buf, 10);
        sscanf(low_buf, "%10d", &low);

	read(sockfd, high_buf, 10);
        sscanf(high_buf, "%10d", &high);

	if(!batch_delete(root, low, high, user))
        {
                write(sockfd, OP_FAILED, 11);
        }
        else
        {
                write(sockfd, OP_SUCCESS, 11);
        }

	return;
}

/*This is used to run show data and tree operation.*/
void 
run_show_data_draw_tree_operation(INDEX_NODE *root)
{
	draw_a_tree(root);
	return;
}

/*This is used to run show some logs.*/
void
run_show_logs(INDEX_NODE *root)
{
	int max_log_num = 99999;
	cout<<"How many logs do you want to read? Input a positive integer not larger than "
		<<max_log_num<<" or 0 to indicate case all!\n"<<endl;
	string num_buf;
	cin>>num_buf;

	if(!is_pure_digit(num_buf))
	{
		cout<<"Invalid number!\n"<<endl;
		return;
	}

	if(num_buf.length() > 5)
	{
		cout<<"Too large number!\n"<<endl;
		return;
	}
	
	int num;
	sscanf(num_buf.c_str(), "%5d", &num);
	
	if(num == 0)
	{
		LOG_INFO *all_log;
	        all_log = exec_read_log(0);

        	if(all_log->log_err)
	        {
        	        cout<<"Error in log!\n"<<endl;
                	free_log_info_mem(all_log);
	                all_log = NULL;
        	        return;
	        }

        	cout<<"Total log number: "<<all_log->total_num<<" Begin Time: "
                	<<all_log->begin_time<<endl;

	        print_log_details(all_log);
        	free_log_info_mem(all_log);
	        all_log = NULL;
	}
	else if(num > 0)
	{
	        LOG_INFO *res1;

	        cout<<"Read lastest "<<num<<" logs: "<<endl;
        	res1 = read_recent_logs(num);

	        if(!res1)
        	{
        	        cout<<"Open log file failed!"<<endl;
                	return;
	        }
	
        	if(res1->log_err)
	        {
        	        cout<<"Not enough log found!\n"<<endl;
	        }

		if(!res1->total_num)
		{
			cout<<"No avalibale log found!\n"<<endl;
			free(res1);
			res1 = NULL;
			return;
		}

        	cout<<"Following are the log details: "<<endl;

        	cout<<"Total log number: "<<res1->total_num<<" Begin Time: "
                	<<res1->begin_time<<endl;

	        print_log_details(res1);

        	free_log_info_mem(res1);
	        res1 = NULL;
	}
	else
	{
		cout<<"Wrong input!\n"<<endl;
	}

	return;
}

/*This is used to do diagnose and repair.*/
INDEX_NODE *
run_diagnose_and_repair(INDEX_NODE *root)
{
	string input;
	cout<<"What do you want to do?\n"<<"1, repair the leaf link;\n"
		<<"2, repair the leaf link and rebuild the whole index tree;\n"
		<<"3, recovery all data by using data backup fils and log files;\n"
		<<"4, exit.\n"<<endl;
	cin>>input;

	if(input.length() != 1)
	{
		cout<<"Invalid input"<<endl;
		return(root);
	}

	int choice;
	sscanf(input.c_str(), "%1d", &choice);	
	switch(choice)
	{
		case 1:
			if(check_and_repair_leaf_link(root))
			{
				cout<<"Repair success!\n"<<endl;
			}
			else
			{
				cout<<"Repair failed!\n"<<endl;
			}
			break;
		case 2:
			INDEX_NODE *new_root;
			new_root = repair_and_rebuild_index_tree(root);
			if(!new_root)
			{
				cout<<"Re-build index tree failed.\n"<<endl;
			}
			else
			{
				cout<<"Re-build index tree successed.\n"<<endl;
				root = new_root;
			}
                        break;
		case 3:
			cout<<"Not finished yet!\n"<<endl;
			break;
		case 4:
			cout<<"Exit!\n"<<endl;
			break;
		default:
			cout<<"Invalid input!\n"<<endl;
			break;
	}

	return(root);
}

/*This is used to setup auto backup.*/
void *
setup_auto_backup(void *arg)
{
	int backup_times = 0;
        while(1)
        {
                sleep(5);
                if(backup_times == 10)
                {
			INDEX_NODE *root = (INDEX_NODE *)arg;
                        BACK_INFO *back_info;
                        back_info = search_backup_info();
                        if(!strcmp(back_info->filename, BACK_FILE1))
                        {
                                exec_write_all_data(root, BACK_FILE2);
				free(back_info->filename);
				free(back_info);
				back_info = NULL;
                        }
                        else
                        {
                                exec_write_all_data(root, BACK_FILE1);
				free(back_info->filename);
                                free(back_info);
                                back_info = NULL;
                        }
                        backup_times = 0;
                }
                else
                {
                        auto_backup();
                        backup_times ++;
                }
        }
}
