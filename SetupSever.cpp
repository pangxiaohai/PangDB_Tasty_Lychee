#include<iostream>
#include"./BIndex.h"
#include<math.h>
#include<stdlib.h>

void setup_consol(void);
void initial_display(void);

int main(void)
{
	setup_consol();
	return 0;
}

void 
setup_consol(void)
{
	int input, db_set = 0, in_test = 0;
	initial_display();
	while(cin>>input)
	{
		switch(input)
		{
			case 1:
				if(!db_set)
				{
					run_test();
					db_set = 1;
					in_test = 1;
				}
				else
				{
					run_exist();
				}
				initial_display();
				break;
			case 3:
				if(in_test)
				{
					end_test();
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
