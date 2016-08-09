#include<iostream>
#include<stdlib.h>
#include<string>
#include<string.h>
#include<unistd.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<arpa/inet.h>
#include"./client.h"

using namespace std;


void initial_display(void);
IDX_BOOL is_pure_digit(string);
void recv_begin(int);
char *create_n_byte_mem(int);
void run_read_data_operation(int);
void run_operation(int);
void operate_consol_header(void);
void operate_consol_display(void);
void recv_begin(int);
void run_insert_data_operation(int);
void run_delete_data_operation(int);
void run_range_search_operation(int);
void run_update_data_operation(int);
void run_top_search_operation(int);
void run_bottom_search_operation(int);
void run_batch_insert_operation(int);
void run_batch_delete_operation(int);


int main(void)
{
	struct sockaddr_in server_addr;
	int serverfd;
	char *server_ip = "10.172.105.84";
	bzero((void *)&server_addr, sizeof(server_addr));
	server_addr.sin_family = AF_INET;

	inet_pton(AF_INET, server_ip, (void *)&(server_addr.sin_addr));
	server_addr.sin_port = htons(PORT);

	serverfd  = socket(AF_INET, SOCK_STREAM, 0);

	connect(serverfd, (struct sockaddr *)&(server_addr), sizeof(server_addr));


	char test_char = 'c';
	write(serverfd, &test_char, 1);

	char buf[11];
	buf[10] = '\0';
	read(serverfd, buf, 8);
	string input_buf;

	if(strncmp(buf, RECV_CMD, 7))
	{
		write(serverfd, ERR_CMD, 5); 
		cout<<"Connect error!"<<endl;
		return 0;
	}
	else
	{
		cout<<"Please input a user id with 5 digits:"<<endl;
		cin>>input_buf;

		if(!is_pure_digit(input_buf) || (input_buf.length() != 5))
		{
			write(serverfd, ERR_CMD, 5);
                	cout<<"Wrong id!"<<endl;
                	return 0;
		}
		
		write(serverfd, input_buf.c_str(), 5);
		
		read(serverfd, buf, 8);
		if(!strncmp(buf, ALREADY_LOG, 8))
		{
			cout<<"User alredy login!"<<endl;
			return 0;
		}
	
		initial_display();
		string choose;
		int input;

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
					write(serverfd, choose.c_str(), 1);
					read(serverfd, buf, 6);
					cout<<"Only for test!"<<endl;
					initial_display();
					break;
				case 2:
					write(serverfd, choose.c_str(), 1);
					run_operation(serverfd);
					initial_display();
					break;
				case 3:
					write(serverfd, choose.c_str(), 1);
					read(serverfd, buf, 5);
					cout<<"Exit!"<<endl;
					return 0;
				default:
					write(serverfd, choose.c_str(), 1);
					read(serverfd, buf, 8);
					cout<<"Invalid input!"<<endl;
					initial_display();
					break;
			}
	
		}
	}
	return 0;
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


/*This is used to check digital string.*/
IDX_BOOL
is_pure_digit(string input)
{
        IDX_BOOL ret = TRUE;
        for(string::iterator it = input.begin(); it != input.end(); ++it)
        {
                if(*it > '9' || *it <'0')
                {
                        ret = FALSE;
                        break;
                }
        }

        return(ret);
}

/*This is used to run operations.*/
void
run_operation(int serverfd)
{
	string choose;
	int input;
	operate_consol_header();
	operate_consol_display();
	while(1)
	{
		choose = "";
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
			case 0:
				write(serverfd, choose.c_str(), 1);
				run_read_data_operation(serverfd);
				operate_consol_display();
				break;
			case 1:
				write(serverfd, choose.c_str(), 1);
				run_insert_data_operation(serverfd);
				operate_consol_display();
				break;
			case 2:
				write(serverfd, choose.c_str(), 1);
				run_delete_data_operation(serverfd);
				operate_consol_display();
				break;
			case 3:
				write(serverfd, choose.c_str(), 1);
                                run_range_search_operation(serverfd);
                                operate_consol_display();
                                break;
                        case 4:
				write(serverfd, choose.c_str(), 1);
                                run_update_data_operation(serverfd);
                                operate_consol_display();
                                break;
	                case 5:
				write(serverfd, choose.c_str(), 1);
                                run_top_search_operation(serverfd);
                                operate_consol_display();
                                break;
			case 6:
				write(serverfd, choose.c_str(), 1);
                                run_bottom_search_operation(serverfd);
                                operate_consol_display();
                                break;
                        case 7:
				write(serverfd, choose.c_str(), 1);
                                run_batch_insert_operation(serverfd);
                                operate_consol_display();
                                break;
                        case 8:
				write(serverfd, choose.c_str(), 1);
                                run_batch_delete_operation(serverfd);
                                operate_consol_display();
                                break;
			case 9:
				write(serverfd, choose.c_str(), 1);
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
		9. exit\n\
		\n"<<endl;
	return;
}

/*This is used to receive begin cmd.*/
void
recv_begin(int serverfd)
{
	char *begin_buf = create_n_byte_mem(6);
	read(serverfd, begin_buf, 6);
	
	free(begin_buf);
	begin_buf = NULL;
	return;
}

/*This is used to run read data operations.*/
void
run_read_data_operation(int serverfd)
{
	recv_begin(serverfd);

	cout<<"Please input a key: "<<endl;
	string key_buf;
        cin>>key_buf;

        if(!is_pure_digit(key_buf))
        {
                cout<<"Invalid key!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
                return;
        }

	if(key_buf.length()>10)
	{
		cout<<"Key is too long!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
		return;
	}

        int input_key;
        sscanf(key_buf.c_str(), "%10d", &input_key);

        if(input_key > KEY_RANGE)
        {
                cout<<"Key is too large!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
                return;
        }

	cout<<"Runnig read data operation.\n"<<endl;

	write(serverfd, RIGHT_SIGNAL, 6);

	char *buf = create_n_byte_mem(11);
	buf[10] = '\0';
        read(serverfd, buf, 8);

	strncpy(buf, key_buf.c_str(), 10);
	write(serverfd, buf, 10);

	read(serverfd, buf, 8);
	buf[8] = '\0';
	if(!strncmp(buf, NO_VALUE, 8))
	{
		cout<<"No value with key: "<<input_key<<" found!\n"<<endl;
		buf[8] = '1';
		free(buf);
		buf = NULL;
		return;
	}
	buf[8] = '1';
        free(buf);
	buf = NULL;

	char len_buf[4];
	len_buf[3] = '\0';
	read(serverfd, len_buf, 3);
	int len;
	sscanf(len_buf, "%3d", &len);

	char *res_buf = create_n_byte_mem(len+1);
	res_buf[len] = '\0';
	
	read(serverfd, res_buf, len);
	cout<<"Key: "<<input_key<<" Value: "<<res_buf<<endl;
	free(res_buf);
	res_buf = NULL;
	return;
}

/*This is used to run insert data operation.*/
void
run_insert_data_operation(int serverfd)
{
        recv_begin(serverfd);

	cout<<"Please input a key: "<<endl;
	string key_buf;
	cin>>key_buf;

        if(!is_pure_digit(key_buf))
        {
                cout<<"Invalid key!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
                return;
        }

        if(key_buf.length()>10)
        {
                cout<<"Key is too long!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
                return;
        }

	int input_key;
	sscanf(key_buf.c_str(), "%10d", &input_key);

	if(input_key > KEY_RANGE)
	{
		cout<<"Key is too large!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
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
		write(serverfd, ERR_CMD, 6);
		return;
	}

	write(serverfd, RIGHT_SIGNAL, 6);

	char *buf = create_n_byte_mem(11);
        buf[10] = '\0';
        read(serverfd, buf, 8);

        strncpy(buf, key_buf.c_str(), 10);
        write(serverfd, buf, 10);

	char len_buf[4];
	len_buf[3] = '\0';
	sprintf(len_buf, "%3d", len);
	write(serverfd, len_buf, 3);

	write(serverfd, value_buf.c_str(), len);

	read(serverfd, buf, 11);
	if(strncmp(buf, OP_FAILED, 10))
	{
		cout<<"Insert key: "<<input_key<<" value: "<<value_buf<<endl;
		cout<<"Insert successed!\n"<<endl;
	}
	else
	{
		cout<<"Insert failed!\n"<<endl;
	}

	free(buf);
	buf = NULL;
	return;
}

/*This is used to run delete operation.*/
void
run_delete_data_operation(int serverfd)
{
        recv_begin(serverfd);

        cout<<"Please input a key: "<<endl;
        string key_buf;
        cin>>key_buf;

        if(!is_pure_digit(key_buf))
        {
                cout<<"Invalid key!\n"<<endl;
                write(serverfd, ERR_CMD, 6);
                return;
        }

        if(key_buf.length()>10)
        {
                cout<<"Key is too long!\n"<<endl;
                write(serverfd, ERR_CMD, 6);
                return;
        }

        int input_key;
        sscanf(key_buf.c_str(), "%10d", &input_key);

        if(input_key > KEY_RANGE)
        {
                cout<<"Key is too large!\n"<<endl;
                write(serverfd, ERR_CMD, 6);
                return;
        }

	write(serverfd, RIGHT_SIGNAL, 6);

        char *buf = create_n_byte_mem(11);
        buf[10] = '\0';
        read(serverfd, buf, 8);

        strncpy(buf, key_buf.c_str(), 10);
        write(serverfd, buf, 10);

	read(serverfd, buf, 11);
        if(strncmp(buf, OP_FAILED, 10))
        {
                cout<<"Delete successed!\n"<<endl;
        }
        else
        {
                cout<<"Delete failed!\n"<<endl;
        }

	free(buf);
	buf = NULL;
        return;
}

/*This is used to run range search.*/
void
run_range_search_operation(int serverfd)
{
        recv_begin(serverfd);

	int low, high;
	ASC_DSC order;
	
	string low_buf, high_buf, choose_buf;

	cout<<"Please input a integer to specify sub bound: "<<endl;
        cin>>low_buf;

	if(!is_pure_digit(low_buf))
	{
		cout<<"Invalid sub bound key!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
		return;
	}
	
	if(low_buf.length() > 10)
	{
		cout<<"Key is too long!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
		return;
	}
	else
	{
		sscanf(low_buf.c_str(), "%10d", &low);
	}

	if(low>KEY_RANGE)
	{
		cout<<"Sub bound is larger than max key! No result will return!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
		return;
	}
	
	if(low < 0)
	{
		cout<<"All keys are larger than 0. Sub bound will be replaced by 0.\n"<<endl;
		low = 0;
	}

	cout<<"Please input a integer to specify up bound: "<<endl;
        cin>>high_buf;

	if(!is_pure_digit(high_buf))
        {
                cout<<"Invalid up bound key!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
                return;
        }

        if(high_buf.length() > 10)
        {
                cout<<"Key is too long! Will be replaced by largest key allowed!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
		high = KEY_RANGE;
        }
        else
        {
                sscanf(high_buf.c_str(), "%10d", &high);
        }

	if(high < 0)
	{
		cout<<"All keys are larger than 0. No result will return.\n"<<endl;
		write(serverfd, ERR_CMD, 6);
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
		write(serverfd, ERR_CMD, 6);
		return;
	}

	write(serverfd, RIGHT_SIGNAL, 6);
	char *buf = create_n_byte_mem(11);
        buf[10] = '\0';
        read(serverfd, buf, 8);

	sprintf(buf, "%10d", low);
	write(serverfd, buf, 10);
	sprintf(buf, "%10d", high);
        write(serverfd, buf, 10);

	if(order == ASC)
	{
		write(serverfd, ASC_CMD, 3);
	}
	else
	{
		write(serverfd, DSC_CMD, 3);
	}

	int num;

	read(serverfd, buf, 5);
	buf[5] = '\0';
	sscanf(buf, "%5d", &num);

	if(!num)
	{
		free(buf);
		buf = NULL;
		cout<<"No value found!"<<endl;
		return;
	}
	else
	{
		int key, len, i = 0;
		char len_buf[4];
		char *value_buf;
		len_buf[3] = '\0';
		cout<<"Totally "<<num<<" data records found!"<<endl;
		while(i != num)
		{
			buf[10] = '\0';
			read(serverfd, buf, 10);
			sscanf(buf, "%10d", &key);
			read(serverfd, len_buf, 3);
			sscanf(len_buf, "%3d", &len);
			value_buf = create_n_byte_mem(len+1);
			value_buf[len] = '\0';
			read(serverfd, value_buf, len);
			cout<<"Key: "<<key<<" Value: "<<value_buf<<"  "<<endl;
			i++;
			free(value_buf);
			value_buf = NULL;
		}
		
		free(buf);
		buf = NULL;
		return;
	}
}

/*This is used to run update operations.*/
void
run_update_data_operation(int serverfd)
{
        recv_begin(serverfd);

	cout<<"Please input a key: "<<endl;
	string key_buf;
        cin>>key_buf;

        if(!is_pure_digit(key_buf))
        {
                cout<<"Invalid key!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
                return;
        }

        if(key_buf.length()>10)
        {
                cout<<"Key is too long!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
                return;
        }

        int input_key;
        sscanf(key_buf.c_str(), "%10d", &input_key);

        if(input_key > KEY_RANGE)
        {
                cout<<"Key is too large!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
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
		write(serverfd, ERR_CMD, 6);
		return;
        }

	write(serverfd, RIGHT_SIGNAL, 6);
	char *buf = create_n_byte_mem(11);
        buf[10] = '\0';
        read(serverfd, buf, 8);

        sprintf(buf, "%10d", input_key);
        write(serverfd, buf, 10);

	sprintf(buf, "%3d", len);
	buf[3] = '\0';
	write(serverfd, buf, 3);
	write(serverfd, value_buf.c_str(), len);
	
	read(serverfd, buf, 11);
	if(strncmp(buf, OP_FAILED, 10))
        {
                cout<<"Update successed!\n"<<endl;
        }
        else
        {
                cout<<"Update failed! Key may not exist or value is duplicated!\n"<<endl;
        }

	free(buf);
	buf = NULL;
        return;
}

/*This is used to run top search.*/
void
run_top_search_operation(int serverfd)
{
	recv_begin(serverfd);

	string num_buf, asc_dsc;
        cout<<"Please input a number to specify how many records to fetch: "<<endl;
        cin>>num_buf;

	if(!is_pure_digit(num_buf) || (num_buf.length() > 5))
	{
		cout<<"Invalid number!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
		return;
	}

	int num;	
	sscanf(num_buf.c_str(), "%d", &num);
	
	ASC_DSC order;
	cout<<"Please chose display order:\n1, ASC; 2, DSC.\n"<<endl;
        cin>>asc_dsc;
        if((asc_dsc != "1") && (asc_dsc != "2"))
        {
                cout<<"Invilid input!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
                return;
        }
	
	write(serverfd, RIGHT_SIGNAL, 6);
	
	char *buf = create_n_byte_mem(11);
	buf[10] = '\0';
	
	read(serverfd, buf, 8);
	buf[5] = '\0';
	sprintf(buf, "%5d", num);
	write(serverfd, buf, 5);

	if(asc_dsc == "1")
        {
                write(serverfd, ASC_CMD, 3);
        }
        else
        {
                write(serverfd, DSC_CMD, 3);
        }

	int res_num, i = 0, key, len;
	read(serverfd, buf, 5);
	buf[5] = '\0';
	sscanf(buf, "%5d", &res_num);
	char len_buf[4];
	char *value_buf;
	len_buf[3] = '\0';

	if(!res_num)
	{
		cout<<"Error! No result found!"<<endl;
		return;
	}
	
	if(res_num < num)
	{
		cout<<"Not enough data records found!"<<endl;
	}

	while(i != res_num)
	{
		read(serverfd, buf, 10);
		sscanf(buf, "%10d", &key);
		read(serverfd, len_buf, 3);
		sscanf(len_buf, "%3d", &len);
		value_buf = create_n_byte_mem(len+1);
		read(serverfd, value_buf, len);
		value_buf[len] = '\0';
		cout<<"Key: "<<key<<" Value: "<<value_buf<<"    "<<endl;
		i++;

		free(value_buf);
		value_buf = NULL;
	}

	free(buf);
	buf = NULL;
	return;
}

/*This is used to run bottom  search.*/
void
run_bottom_search_operation(int serverfd)
{
	run_top_search_operation(serverfd);
	return;
}

/*This is used to run batch insert operation.*/
void
run_batch_insert_operation(int serverfd)
{
        recv_begin(serverfd);

	string num_buf;
	cout<<"Please input how many data to insert:"<<endl;
	cin>>num_buf;
	
	if(!is_pure_digit(num_buf))
	{
		cout<<"Invalid input!"<<endl;
		write(serverfd, ERR_CMD, 6);
		return;
	}
	
	if(num_buf.length()>4)
	{
		cout<<"Too many data!"<<endl;
		write(serverfd, ERR_CMD, 6);
		return;
	}
	int num;
	sscanf(num_buf.c_str(), "%4d", &num);
	if(num <= 0)
	{
		cout<<"Invalid input!"<<endl;
		write(serverfd, ERR_CMD, 6);
		return;
	}

	if(num <= MAXKEYNUM + 1)
	{
		cout<<"Too few!"<<endl;
                write(serverfd, ERR_CMD, 6);
                return;
	}

	write(serverfd, RIGHT_SIGNAL, 6);

	int i = 0, key, len;
	string key_buf, value_buf;	
	char send_key[11], send_len[4];
	send_key[10] = send_len[3] = '\0';

	char buf[11];
	buf[10] = '\0';
	
	read(serverfd, buf, 8);

        sprintf(buf, "%5d", num);
	buf[5] = '\0';
        write(serverfd, buf, 5);

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
		len = value_buf.length();

		if(len>974)
		{
			cout<<"Value is too long!"<<endl;
			continue;
		}

		sprintf(send_key, "%10d", key);
		sprintf(send_len, "%3d", len);

		write(serverfd, send_key, 10);
		write(serverfd, send_len, 3);
		write(serverfd, value_buf.c_str(), len);
		
		i++;
	}

	read(serverfd, buf, 11);
        if(strncmp(buf, OP_FAILED, 10))
        {
                cout<<"Batch insert successed!\n"<<endl;
        }
        else
        {
                cout<<"Batch insert failed!\n"<<endl;
        }

	return;
}

/*This is used to run batch delete operation.*/
void
run_batch_delete_operation(int serverfd)
{
        recv_begin(serverfd);

	cout<<"Please input the low bound of delete range:"<<endl;
	string low_buf;
	cin>>low_buf;

	if(!is_pure_digit(low_buf))
        {
                cout<<"Invalid key!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
                return;
        }

        if(low_buf.length()>10)
        {
                cout<<"Key is too long!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
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
		write(serverfd, ERR_CMD, 6);
		return;
	}

	cout<<"Please input the high bound of delete range:"<<endl;
        string high_buf;
        cin>>high_buf;

        if(!is_pure_digit(high_buf))
        {
                cout<<"Invalid key!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
                return;
        }

        if(high_buf.length()>10)
        {
                cout<<"Key is too long!\n"<<endl;
		write(serverfd, ERR_CMD, 6);
                return;
        }

        int high;
        sscanf(high_buf.c_str(), "%10d", &high);

        if(high < 0)
        {
                cout<<"High bound is less than 0. No key to delete.\n"<<endl;
		write(serverfd, ERR_CMD, 6);
                return;
        }
        else if(high > KEY_RANGE)
        {
                cout<<"High bound is larger than key range. Replace it with largest key."<<endl;
                high = KEY_RANGE + 1;
        }

	if(low >= high)
	{
		cout<<"Low bound is larger than high bound!"<<endl;
		write(serverfd, ERR_CMD, 6);
		return;
	}

	write(serverfd, RIGHT_SIGNAL, 6);

        char *buf = create_n_byte_mem(11);
        buf[10] = '\0';

        read(serverfd, buf, 8);
	
	sprintf(buf, "%10d", low);
	write(serverfd, buf, 10);
	
	sprintf(buf, "%10d", high);
        write(serverfd, buf, 10);

	read(serverfd, buf, 11);
        if(strncmp(buf, OP_FAILED, 10))
        {
                cout<<"Batch delete successed!\n"<<endl;
        }
        else
        {
                cout<<"Batch delete failed!\n"<<endl;
        }

        return;
}

/*This is used to create n char memory.*/
char *
create_n_byte_mem(int num)
{
        char *ret;
        ret = (char *)malloc(num * sizeof(char));
        return(ret);
}
