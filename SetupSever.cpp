#include<iostream>
#include"./BIndex.h"
#include<math.h>
#include<stdlib.h>
#include<string>
#include<string.h>


using namespace std;


void setup_consol(void);
void initial_display(void);
void operate_consol_header(void);
void operate_consol_display(void);
void operate_exist(INDEX_NODE *);

/*Declare run operation functions.*/
void run_read_data_operation(INDEX_NODE *);
void run_insert_data_operation(INDEX_NODE *);
void run_delete_data_operation(INDEX_NODE *);
void run_range_search_operation(INDEX_NODE *);
void run_update_data_operation(INDEX_NODE *);
void run_top_search_operation(INDEX_NODE *);
void run_bottom_search_operation(INDEX_NODE *);
void run_batch_insert_operation(INDEX_NODE *);
void run_batch_delete_operation(INDEX_NODE *);
void run_show_data_draw_tree_operation(INDEX_NODE *);
void run_show_logs(INDEX_NODE *);
INDEX_NODE *run_diagnose_and_repair(INDEX_NODE *);


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

	setup_consol();
	return 0;
}

/*This is used to setup a consol.*/
void 
setup_consol(void)
{
	INDEX_NODE *test_root;
	int input, db_set = 0, in_test = 0;
	string choose;
	initial_display();
	while(1)
	{
		cin>>choose;
		if(choose.length()>1)
		{
			input = -1;
		}
		else
		{
			sscanf(choose.c_str(), "%1d", &input);
		}
		switch(input)
		{
			case 1:
				if(!db_set)
				{
					test_root = test_init();
					db_set = 1;
					in_test = 1;
					run_exist(test_root);
				}
				else
				{
					run_exist(test_root);
				}
				initial_display();
				break;
			case 2:
				if(db_set)
				{
					operate_exist(test_root);
				}
				else
				{
					test_root = test_init();
					db_set = 1;
					in_test = 1;
					operate_exist(test_root);
				}
				initial_display();
				break;
			case 3:
				if(in_test)
				{
					end_test(test_root);
					cout<<"Quit!\n"<<endl;
				}
				return;
			default:
				cout<<"Invalid Input!\n"<<endl;
				initial_display();
				break;
		}
	}
	return;
}

/*This is used for initial display*/
void 
initial_display(void)
{
	cout<<"\
		Welcom to PangDB!\n\
		\n\
		ԅ( ˘ω˘ ԅ) \n\
		\n\
		Please input your choice!\n\
		1, Setup a test database and run tests;\n\
		2, Initial a test database and have a try;\n\
		3, exit.\n"<<endl;
	return;
}

/*This is used to operations on exist db.*/
void
operate_exist(INDEX_NODE *root)
{
	string choose;
	int input;
	operate_consol_header();
	operate_consol_display();
	while(1)
	{
		cin>>choose;
                if(choose.length()>2)
                {
                        input = -1;
                }
		else
		{
			sscanf(choose.c_str(), "%2d", &input);
		}

		switch(input)
		{
			case 0:
				run_read_data_operation(root);
				operate_consol_display();
				break;
			case 1:
				run_insert_data_operation(root);
				operate_consol_display();
				break;
			case 2:
				run_delete_data_operation(root);
				operate_consol_display();
				break;
			case 3:
                                run_range_search_operation(root);
                                operate_consol_display();
                                break;
                        case 4:
                                run_update_data_operation(root);
                                operate_consol_display();
                                break;
                        case 5:
                                run_top_search_operation(root);
                                operate_consol_display();
                                break;
			case 6:
                                run_bottom_search_operation(root);
                                operate_consol_display();
                                break;
                        case 7:
                                run_batch_insert_operation(root);
                                operate_consol_display();
                                break;
                        case 8:
                                run_batch_delete_operation(root);
                                operate_consol_display();
                                break;
			case 9:
				run_show_data_draw_tree_operation(root);
				operate_consol_display();
				break;
			case 10:
				run_show_logs(root);
				operate_consol_display();
                                break;
			case 11:
                                root = run_diagnose_and_repair(root);
                                operate_consol_display();
                                break;
			case 12:
				cout<<"Exit operation try!\n"<<endl;
				return;
			default:
				cout<<"Invalid input.\n"<<endl;
				operate_consol_display();	
				break;
		}
	}
}

/*This is used for show operation consel header.*/
void
operate_consol_header(void)
{
	cout<<"\
                A test database setup.\n\
                Now you can try some operations.\n\
		"<<endl;
	return;
}


/*This is used for operation consel display.*/
void
operate_consol_display(void)
{
	cout<<"\
		\n\
                ԅ( ˘ω˘ ԅ) \n\
                \n\
		What operations do you want to try:\n\
		0, read a data record.\n\
		1, insert a data record.\n\
		2, delete a data record.\n\
		3, range search.\n\
		4, update a data record.\n\
		5, top search.\n\
		6. bottom search.\n\
		7. batch insert.\n\
		8. batch delete.\n\
		9. show all data and draw the tree.\n\
		10. show some logs.\n\
		11. diagnose and repair index tree.\n\
		12. exit\n\
		\n"<<endl;
	return;
}

/*Following are run operation functions.*/

/*This is used to run read data operation.*/
void 
run_read_data_operation(INDEX_NODE *root)
{
	cout<<"Please input a key: "<<endl;
	string key_buf;
        cin>>key_buf;

        if(!is_pure_digit(key_buf))
        {
                cout<<"Invalid key!\n"<<endl;
                return;
        }

	if(key_buf.length()>10)
	{
		cout<<"Key is too long!\n"<<endl;
		return;
	}

        int input_key;
        sscanf(key_buf.c_str(), "%10d", &input_key);

        if(input_key > KEY_RANGE)
        {
                cout<<"Key is too large!\n"<<endl;
                return;
        }

	cout<<"Runnig read data operation.\n"<<endl;
	read_value(root,input_key);
	return;
}

/*This is used to run insert data operation.*/
void 
run_insert_data_operation(INDEX_NODE *root)
{
	cout<<"Please input a key: "<<endl;
	string key_buf;
	cin>>key_buf;

        if(!is_pure_digit(key_buf))
        {
                cout<<"Invalid key!\n"<<endl;
                return;
        }

        if(key_buf.length()>10)
        {
                cout<<"Key is too long!\n"<<endl;
                return;
        }

	int input_key;
	sscanf(key_buf.c_str(), "%10d", &input_key);

	if(input_key > KEY_RANGE)
	{
		cout<<"Key is too large!\n"<<endl;
		return;
	}
	
	string value_buf;
	
	cout<<"Please input value: "<<endl;
	cin>>value_buf;
	int len;
	len = value_buf.length();
	if(len>964)
	{
		cout<<"Value is too long!\n"<<endl;
	}

        DATA_RECORD *data;
        data = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
        data->key = input_key;
	data->value = create_n_byte_mem(len+1);
	(data->value)[len] = '\0';
	strncpy(data->value, value_buf.c_str(), len);
	data->len = len;

	if(!insert_node(root, data))
	{
		cout<<"Insert failed!\n"<<endl;
		free(data->value);
		free(data);
		data = NULL;
	}
	else
	{
		cout<<"Insert key: "<<data->key<<" value: "<<data->value<<endl;
		cout<<"Insert successed!\n"<<endl;
	}

	return;
}

/*This is used to run delete data operation.*/
void 
run_delete_data_operation(INDEX_NODE *root)
{
        cout<<"Please input a key: "<<endl;
	string key_buf;
        cin>>key_buf;

        if(!is_pure_digit(key_buf))
        {
                cout<<"Invalid key!\n"<<endl;
                return;
        }

        if(key_buf.length()>10)
        {
                cout<<"Key is too long!\n"<<endl;
                return;
        }

        int input_key;
        sscanf(key_buf.c_str(), "%10d", &input_key);

        if(input_key > KEY_RANGE)
        {
                cout<<"Key is too large!\n"<<endl;
                return;
        }

        if(!delete_node(root, input_key))
        {
                cout<<"Delete failed!\n"<<endl;
        }
        else
        {
                cout<<"Delete successed!\n"<<endl;
        }
	return;
}

/*This is used to run range search operation.*/
void 
run_range_search_operation(INDEX_NODE *root)
{
	int low, high;
	ASC_DSC order;
	
	string low_buf, high_buf, choose_buf;

	cout<<"Please input a integer to spicify sub bound: "<<endl;
        cin>>low_buf;

	if(!is_pure_digit(low_buf))
	{
		cout<<"Invalid sub bound key!\n"<<endl;
		return;
	}
	
	if(low_buf.length() > 10)
	{
		cout<<"Key is too long!\n"<<endl;
		return;
	}
	else
	{
		sscanf(low_buf.c_str(), "%10d", &low);
	}

	if(low>KEY_RANGE)
	{
		cout<<"Sub bound is larger than max key! No result will return!\n"<<endl;
		return;
	}
	
	if(low < 0)
	{
		cout<<"All keys are larger than 0. Sub bound will be replaced by 0.\n"<<endl;
		low = 0;
	}

	cout<<"Please input a integer to spicify up bound: "<<endl;
        cin>>high_buf;

	if(!is_pure_digit(high_buf))
        {
                cout<<"Invalid up bound key!\n"<<endl;
                return;
        }

        if(high_buf.length() > 10)
        {
                cout<<"Key is too long! Will be replaced by largest key allowed!\n"<<endl;
		high = KEY_RANGE;
        }
        else
        {
                sscanf(high_buf.c_str(), "%10d", &high);
        }

	if(high < 0)
	{
		cout<<"All keys are larger than 0. No result will return.\n"<<endl;
		return;
	}

	cout<<"Please chose display order:\n1, ASC; 2, DSC.\n"<<endl;
	cin>>choose_buf;
	if(choose_buf == "1")
	{
		order = ASC;
	}
	else if(choose_buf == "2")
	{
		order = DSC;
	}
	else
	{
		cout<<"Invilid input!\n"<<endl;
		return;
	}

	if(range_search(root, low, high, order))
	{
		cout<<"Range search successed!\n"<<endl;
		return;
	}
	else
	{
		cout<<"Range search failed!\n"<<endl;
	}
}

/*This is used to run update data operation.*/
void 
run_update_data_operation(INDEX_NODE *root)
{
        cout<<"Please input a key: "<<endl;
	string key_buf;
        cin>>key_buf;

        if(!is_pure_digit(key_buf))
        {
                cout<<"Invalid key!\n"<<endl;
                return;
        }

        if(key_buf.length()>10)
        {
                cout<<"Key is too long!\n"<<endl;
                return;
        }

        int input_key;
        sscanf(key_buf.c_str(), "%10d", &input_key);

        if(input_key > KEY_RANGE)
        {
                cout<<"Key is too large!\n"<<endl;
                return;
        }

        string value_buf;

        cout<<"Please input value: "<<endl;
        cin>>value_buf;
        int len;
        len = value_buf.length();
        if(len>974)
        {
                cout<<"Value is too long!\n"<<endl;
        }

        DATA_RECORD *data;
        data = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
        data->key = input_key;
        data->value = create_n_byte_mem(len+1);
        (data->value)[len] = '\0';
        strncpy(data->value, value_buf.c_str(), len);
        data->len = len;
	
	if(update_value(root, data))
	{
		cout<<"Update successed!"<<endl;
	}
	else
	{
		cout<<"Update failed!"<<endl;
	}

	free(data);
	data = NULL;
	return;
}

/*This is used to run top search operation.*/
void 
run_top_search_operation(INDEX_NODE *root)
{
	string num_buf, asc_dsc;
        cout<<"Please input a number to spicify how many records to fetch: "<<endl;
        cin>>num_buf;

	if(!is_pure_digit(num_buf))
	{
		cout<<"Invalid number!\n"<<endl;
		return;
	}

	int num;	
	sscanf(num_buf.c_str(), "%d", &num);
	
	ASC_DSC order;
	cout<<"Please chose display order:\n1, ASC; 2, DSC.\n"<<endl;
        cin>>asc_dsc;
        if(asc_dsc == "1")
        {
                order = ASC;
        }
        else if(asc_dsc == "2")
        {
                order = DSC;
        }
        else
        {
                cout<<"Invilid input!\n"<<endl;
                return;
        }

	if(top_search(root, num, order))
	{
		cout<<"Search successed!\n"<<endl;
	}
	else
	{
		cout<<"Search failed!\n"<<endl;
	}

	return;
}

/*This is used to run bottom search operation.*/
void 
run_bottom_search_operation(INDEX_NODE *root)
{
        string num_buf, asc_dsc;
        cout<<"Please input a number to spicify how many records to fetch: "<<endl;
        cin>>num_buf;

        if(!is_pure_digit(num_buf))
        {
                cout<<"Invalid number!\n"<<endl;
                return;
        }

        int num;
        sscanf(num_buf.c_str(), "%d", &num);

        ASC_DSC order;
        cout<<"Please chose display order:\n1, ASC; 2, DSC.\n"<<endl;
        cin>>asc_dsc;
        if(asc_dsc == "1")
        {
                order = ASC;
        }
        else if(asc_dsc == "2")
        {
                order = DSC;
        }
        else
        {
                cout<<"Invilid input!\n"<<endl;
                return;
        }

        if(bottom_search(root, num, order))
        {
                cout<<"Search successed!\n"<<endl;
        }
        else
        {
                cout<<"Search failed!\n"<<endl;
        }

	return;
}

/*This is used to run batch insert operation.*/
void 
run_batch_insert_operation(INDEX_NODE *root)
{
	string num_buf;
	cout<<"Please input how many data to insert:"<<endl;
	cin>>num_buf;
	
	if(!is_pure_digit(num_buf))
	{
		cout<<"Invalid input!"<<endl;
		return;
	}
	
	if(num_buf.length()>4)
	{
		cout<<"Too many data!"<<endl;
		return;
	}
	int num;
	sscanf(num_buf.c_str(), "%4d", &num);
	if(num <= 0)
	{
		cout<<"Invalid input!"<<endl;
		return;
	}

	int i = 0, key, len;
	DATA_RECORD_LIST *data_list, *cur_data, *new_data;
	data_list = NULL;
	string key_buf, value_buf;	

	while(i != num)
	{
		cout<<"Please input a key:"<<endl;
		cin>>key_buf;
		if(!is_pure_digit(key_buf))
	        {
        	        cout<<"Invalid key!\n"<<endl;
                	continue;
        	}

        	if(key_buf.length()>10)
        	{
                	cout<<"Key is too long!\n"<<endl;
                	continue;
        	}

	        sscanf(key_buf.c_str(), "%10d", &key);

        	if(key<0)
        	{
                	cout<<"Negtive key is not supported."<<endl;
                	continue;
        	}
        	else if(key > KEY_RANGE)
        	{
                	cout<<"Key is larger than key range."<<endl;
                	continue;
        	}
		
		cout<<"Please input a value:"<<endl;
                cin>>value_buf;
		if(i == 0)
		{
			data_list = create_data_record_list_mem();
			data_list->data_record = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
			data_list->data_record->key = key;
			len = value_buf.length();
			data_list->data_record->len = len;
			data_list->data_record->value = create_n_byte_mem(len+1);
			data_list->data_record->value[len] = '\0';
			strncpy(data_list->data_record->value, value_buf.c_str(), len);
			cur_data = data_list;
			data_list->next_data = NULL;
		}
		else
		{
                        new_data = create_data_record_list_mem();
                        new_data->data_record = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
                        new_data->data_record->key = key;
                        len = value_buf.length();
                        new_data->data_record->len = len;
                        new_data->data_record->value = create_n_byte_mem(len+1);
                        new_data->data_record->value[len] = '\0';
                        strncpy(new_data->data_record->value, value_buf.c_str(), len);
                        new_data->next_data = NULL;
			cur_data->next_data = new_data;
			cur_data = cur_data->next_data;
                }

		i++;
	}

	if(batch_insert(root, data_list))
	{
		cout<<"Batch insert successed!"<<endl;
	}
	else
	{
		cout<<"Batch isnert failed!"<<endl;
		free_data_record_list_mem(data_list);
	}	

	return;
}

/*This is used to run batch delete operation.*/
void 
run_batch_delete_operation(INDEX_NODE *root)
{
	cout<<"Please input the low bound of delete range:"<<endl;
	string low_buf;
	cin>>low_buf;

	if(!is_pure_digit(low_buf))
        {
                cout<<"Invalid key!\n"<<endl;
                return;
        }

        if(low_buf.length()>10)
        {
                cout<<"Key is too long!\n"<<endl;
                return;
        }	
	
	int low;
	sscanf(low_buf.c_str(), "%10d", &low);
	
	if(low<0)
	{
		cout<<"Negtive key is not supported. Replace it with 0.\n"<<endl;
		low = 0;
	}
	else if(low > KEY_RANGE)
	{
		cout<<"Low bound is larger than key range. No key to delete."<<endl;
		return;
	}

	cout<<"Please input the high bound of delete range:"<<endl;
        string high_buf;
        cin>>high_buf;

        if(!is_pure_digit(high_buf))
        {
                cout<<"Invalid key!\n"<<endl;
                return;
        }

        if(high_buf.length()>10)
        {
                cout<<"Key is too long!\n"<<endl;
                return;
        }

        int high;
        sscanf(high_buf.c_str(), "%10d", &high);

        if(high < 0)
        {
                cout<<"High bound is less than 0. No key to delete.\n"<<endl;
                return;
        }
        else if(high > KEY_RANGE)
        {
                cout<<"High bound is larger than key range. Replace it with largest key."<<endl;
                high = KEY_RANGE + 1;
        }
	
	cout<<"Do the batch delete!"<<endl;
	cout<<"Delete range: "<<low<<"~"<<high<<endl;
	if(batch_delete(root, low, high))
	{
		cout<<"Batch delete successed!"<<endl;
	}
	else
	{
		cout<<"Batch delete failed!"<<endl;
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
