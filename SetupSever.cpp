#include<iostream>
#include"./BIndex.h"
#include<math.h>
#include<stdlib.h>


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


int main(void)
{
	setup_consol();
	return 0;
}

/*This is used to setup a consol.*/
void 
setup_consol(void)
{
	INDEX_NODE *test_root;
	int input, db_set = 0, in_test = 0;
	initial_display();
	while(cin>>input)
	{
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
	int input;
	operate_consol_header();
	operate_consol_display();
	while(cin>>input)
	{
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
				cout<<"Exit operation try!\n"<<endl;
				return;
			default:
				cout<<"Invalid input.\n"<<endl;	
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
		10. exit\n\
		\n"<<endl;
	return;
}

/*Following are run operation functions.*/

/*This is used to run read data operation.*/
void 
run_read_data_operation(INDEX_NODE *root)
{
	int key;
	cout<<"Please input a key: "<<endl;
	cin>>key;
	cout<<"Runnig read data operation.\n"<<endl;
	read_value(root,key);
	return;
}

/*This is used to run insert data operation.*/
void 
run_insert_data_operation(INDEX_NODE *root)
{
	DATA_RECORD *data;
	data = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
	cout<<"Please input a key: "<<endl;
	cin>>data->key;
	cout<<"Please input value: "<<endl;
	cin>>data->value;

	if(!insert_node(root, data))
	{
		cout<<"Insert failed!\n"<<endl;
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
	int key;
	cout<<"Please input a key: "<<endl;
        cin>>key;

        if(!delete_node(root, key))
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
	int low, high, asc_dsc;
	ASC_DSC order;

	cout<<"Please input a integer to spicify sub bound: "<<endl;
        cin>>low;
	
	cout<<"Please input a integer to spicify up bound: "<<endl;
        cin>>high;

	cout<<"Please chose display order:\n1, ASC; 2, DSC.\n"<<endl;
	cin>>asc_dsc;
	if(asc_dsc == 1)
	{
		order = ASC;
	}
	else if(asc_dsc == 2)
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
	DATA_RECORD *data;
        data = (DATA_RECORD *)malloc(sizeof(DATA_RECORD));
        cout<<"Please input a key: "<<endl;
        cin>>data->key;
        cout<<"Please input value: "<<endl;
        cin>>data->value;
	
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
	int num, asc_dsc;
        cout<<"Please input a number to spicify how many records to fetch: "<<endl;
        cin>>num;
	
	ASC_DSC order;
	cout<<"Please chose display order:\n1, ASC; 2, DSC.\n"<<endl;
        cin>>asc_dsc;
        if(asc_dsc == 1)
        {
                order = ASC;
        }
        else if(asc_dsc == 2)
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
        int num, asc_dsc;
        cout<<"Please input a number to spicify how many records to fetch: "<<endl;
        cin>>num;

        ASC_DSC order;
        cout<<"Please chose display order:\n1, ASC; 2, DSC.\n"<<endl;
        cin>>asc_dsc;
        if(asc_dsc == 1)
        {
                order = ASC;
        }
        else if(asc_dsc == 2)
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
	cout<<"Batch operations has not tested!\n"<<endl;
	return;
}

/*This is used to run batch delete operation.*/
void 
run_batch_delete_operation(INDEX_NODE *root)
{
	cout<<"Batch operations has not tested!\n"<<endl;
	return;
}

/*This is used to run show data and tree operation.*/
void 
run_show_data_draw_tree_operation(INDEX_NODE *root)
{
	draw_a_tree(root);
	return;
}
