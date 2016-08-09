#include<iostream>
#include<stdlib.h>
#include<string>
#include<string.h>
#include<unistd.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<arpa/inet.h>

/*For C/S communication.*/
#define RECV_CMD "receive"
#define RIGHT_SIGNAL "right"
#define DISPLAY_CMD "display"
#define OP_CONSOL_HEADER "op_header"
#define OP_CONSOL "op_consol"
#define EXIT_CMD "exit"
#define ERR_CMD "error"
#define INVALID_CMD "invalid"
#define PACKAGE_CMD "package"
#define TYPE_CMD "type"
#define NO_VALUE "no_value"
#define DUP_VALUE "duplicate"
#define OP_FAILED "op_failed!"
#define OP_SUCCESS "op_success"
#define QUIT "quit"
#define BEGIN_CMD "begin"
#define ASC_CMD "ASC"
#define DSC_CMD "DSC"
#define ALREADY_LOG "already"

#define PORT 8088
#define KEY_RANGE 100000
#define MAXKEYNUM 4

enum IDX_BOOL {FALSE = 0, TRUE = 1};
enum ASC_DSC {ASC, DSC};
