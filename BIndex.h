#include <string>
#include <math.h>

using namespace std;

/*Definition marco.*/

#define MAXKEYNUM 4
#define LEAST_P_NUM (short)(ceil(MAXKEYNUM/2)+1)
#define ALL_REST_NODES -1
#define NO_LOCK 0
#define READ_LOCK 1
#define WRITE_LOCK 2
#define LEAF_NODE_TYPE (unsigned char)0
#define INDEX_NODE_TYPE (unsigned char)1
#define KEY_RANGE 100000
#define FAKE_PID 100


/*For file operations*/
#define LOG_FILE "./PangDB_Log"
#define LOG_END " LOG_END"
#define BACK_FILE1 "./back_data1"
#define BACK_FILE2 "./back_data2"
#define BACK_LOG "./back_log"
#define LAST_LOG "./last_log"
#define DATA_END "DATA_END"
#define BACK_END "BACK_END"
#define INVALID " INVALID"


/*Define data sturcts.*/


enum RUN_RESULT {RUN_FAILED = 0, RUN_SUCCESS = 1};

enum IDX_BOOL {FALSE = 0, TRUE = 1};

enum SEARCH_DIR {FORWARD, BACKWORD};

enum ASC_DSC {ASC, DSC};

enum HOLDER_STATUS {NO_RIGHTS, EMPTY, NOT_EMPTY, REMOVE_SUCCESS};

enum ON_OFF {OFF = 0, ON = 1};

enum FIRST_OR_LAST {FIRST_NODE = 0, LAST_NODE = 1};

enum SUB_UP {SUB_MARK = 0, UP_MARK = 1};

enum ACTION {READ = 0, INSERT = 1, UPDATE = 2 , DELETE = 3, UNDEFINED = -1};

enum SORT_STAT {UNSORT = 0, SORT = 1};

typedef int PID;

typedef struct data_record
{
	int key;
	int len;
	char *value;
} DATA_RECORD;

typedef struct data_record_list
{
	DATA_RECORD *data_record;
	struct data_record_list *next_data;
} DATA_RECORD_LIST;

typedef struct leaf_node
{
	int pri_key;
	unsigned char node_type;
	unsigned char node_lock;
	struct data_record *data_record;
	struct leaf_node *back_node;
	struct leaf_node *front_node;
} LEAF_NODE;

typedef struct index_node
{
	int pri_key;
	unsigned char node_type;
	short key_num;
	unsigned char node_lock;
	int key_list[MAXKEYNUM];
	struct index_node *p_node[MAXKEYNUM+1];
} INDEX_NODE;

typedef struct node_info
{
	INDEX_NODE *node_addr;
	unsigned char node_type;
	struct node_info *next_node_info;
} NODE_INFO;

typedef struct level_info
{
	NODE_INFO *node_info_addr;
	int node_num;
} LEVEL_INFO;
typedef struct data_link_list
{
	struct data_record cur_data;
	struct data_record *next_data;
} DATA_LINK_LIST;

typedef struct index_node_link
{
	INDEX_NODE *index_node;
	struct index_node_link *front_link;
	struct index_node_link *next_link;
} INDEX_NODE_LINK;

typedef struct double_leaf_link
{
	LEAF_NODE *leaf_node;
	struct double_leaf_link *front_link;
	struct double_leaf_link *next_link;
} DOUBLE_LEAF_LINK;

typedef struct single_leaf_link
{
        LEAF_NODE *leaf_node;
        struct single_leaf_link *next_link;
} SINGLE_LEAF_LINK;

typedef struct node_path_info
{
	RUN_RESULT status;
	IDX_BOOL is_include_leaf;
	LEAF_NODE *end_leaf;
	INDEX_NODE_LINK *index_node_link;
} NODE_PATH_INFO;

typedef struct node_analyze_res
{
	SINGLE_LEAF_LINK *leaf_link;
	INDEX_NODE_LINK *index_link;
} NODE_ANALYZE_RES;

typedef struct test_summary{
        int success_num;
        int fail_num;
        int total_num;
} TEST_SUMMARY;

typedef struct log_list{
	PID user;
	int time;
	ACTION act;
	DATA_RECORD *data_record;
	struct log_list *next_log;
} LOG_LIST;

typedef struct log_info{
	LOG_LIST *log_list;
	int total_num;
	int begin_time;
	IDX_BOOL log_err;
} LOG_INFO;

typedef struct back_info{
	int begin_time;
	char filename[12];
} BACK_INFO;

typedef struct data_info{
	DATA_RECORD_LIST *data_list;
	int num;
	int max_key;
	int min_key;
} DATA_INFO;

typedef struct sub_tree_info_link{
	INDEX_NODE *sub_root;
	INDEX_NODE *parent;
	LEAF_NODE *begin_leaf;
	LEAF_NODE *end_leaf;
	IDX_BOOL is_first;
	IDX_BOOL is_last;
	IDX_BOOL is_leaf;
	struct sub_tree_info_link *next_link;
} SUB_TREE_INFO_LINK;

typedef struct sub_tree_list_info{
	int leaf_num;
	SUB_TREE_INFO_LINK *sub_tree_info_link;
	DATA_RECORD_LIST *data_list;
} SUB_TREE_LIST_INFO;

/*Need to consider what info to store here*/
/*
typedef struct depth_info{
	int largest;
	int smallest;
} DEPTH_INFO;
*/

/*Declare Function.*/

/*Following for initialization.*/
INDEX_NODE *mk_index(DATA_RECORD_LIST *, int);
RUN_RESULT create_sys_files(void);

/*Following for generating index support.*/
LEAF_NODE *generate_leaf_node(DATA_RECORD_LIST *, int);
LEAF_NODE *link_list(DATA_RECORD_LIST *, int);
INDEX_NODE *generate_non_leaf(LEAF_NODE *, int);
LEVEL_INFO *generate_level(LEVEL_INFO*);
LEVEL_INFO *generate_rest_level(LEVEL_INFO*);
DATA_RECORD_LIST *fetch_last_data_record_list(DATA_RECORD_LIST *, int);
DATA_RECORD_LIST *merge_sort(DATA_RECORD_LIST*, DATA_RECORD_LIST*);
DATA_RECORD_LIST *quick_sort(DATA_RECORD_LIST*, int);

/*Following for function support.*/
IDX_BOOL is_leaf_node(INDEX_NODE *);
DATA_INFO *get_data_info(DATA_RECORD_LIST *);
IDX_BOOL check_duplicate_data(DATA_RECORD_LIST *, SORT_STAT);
ACTION get_action(int);
IDX_BOOL is_pure_digit(string);

/*Following for memory allocate.*/
LEAF_NODE *create_leaf_mem(void);
INDEX_NODE *create_index_mem(void);
NODE_INFO *create_node_info_mem(void);
LEVEL_INFO *create_level_info_mem(void);
INDEX_NODE_LINK *create_index_node_link_mem(void);
NODE_PATH_INFO *create_node_path_info_mem(void);
SINGLE_LEAF_LINK *create_single_leaf_link_mem(void);
DOUBLE_LEAF_LINK *create_double_leaf_link_mem(void);
NODE_ANALYZE_RES *create_node_analyze_res_mem(void);
DATA_RECORD_LIST *create_data_record_list_mem(void);
DATA_RECORD *create_data_record_mem(void);
LOG_LIST *create_log_list_mem(void);
LOG_INFO *create_log_info_mem(void);
DATA_INFO *create_data_info_mem(void);
SUB_TREE_LIST_INFO *create_sub_tree_list_info_mem(void);
SUB_TREE_INFO_LINK *create_sub_tree_info_link_mem(void);
//DEPTH_INFO *create_depth_info_mem(void);
char *create_n_byte_mem(int);

/*Following for memory free.*/
void free_index_node_link_mem(INDEX_NODE_LINK *);
void free_node_path_info_mem(NODE_PATH_INFO *);
void free_node_analyze_res_mem(NODE_ANALYZE_RES *);
void free_single_leaf_link_mem(SINGLE_LEAF_LINK *);
void free_double_leaf_link_mem(DOUBLE_LEAF_LINK *);
void free_level_info_mem(LEVEL_INFO *);
void free_data_record_list_mem(DATA_RECORD_LIST *);
void free_a_tree_mem(INDEX_NODE *);
void free_log_list_mem(LOG_LIST *);
void free_log_info_mem(LOG_INFO *);
void free_data_info_mem(DATA_INFO *);
void free_sub_tree_list_info_mem(SUB_TREE_LIST_INFO *);
void free_sub_tree_info_link_mem(SUB_TREE_INFO_LINK *);
//void free_depth_info_mem(DEPTH_INFO *);

/*Following for test support!*/
RUN_RESULT run_mk_BIndex_test(INDEX_NODE *);
void for_test(int);
void test_leaf_link(LEAF_NODE *, ON_OFF);
void test_list(DATA_RECORD_LIST *, int, ON_OFF);
void test_node_info(NODE_INFO *, ON_OFF);
void test_level_info(LEVEL_INFO *, ON_OFF);
void draw_all_rest_leaves(LEAF_NODE *, SEARCH_DIR);
RUN_RESULT run_range_search_test(INDEX_NODE *);
void draw_a_leaf(LEAF_NODE *, int);
void draw_a_tree(INDEX_NODE *);
DATA_RECORD_LIST *generate_random_data(int);
void run_exist(INDEX_NODE *);
RUN_RESULT run_top_bottom_test(INDEX_NODE *);
void end_test(INDEX_NODE *);
RUN_RESULT run_read_test(INDEX_NODE *);
int produce_actual_key(INDEX_NODE *, int);
RUN_RESULT run_update_test(INDEX_NODE *);
RUN_RESULT run_delete_test(INDEX_NODE *);
RUN_RESULT run_insert_test(INDEX_NODE *);
INDEX_NODE *test_init(void);

/*Following for supported operations.*/
NODE_PATH_INFO *scan_node(INDEX_NODE *, int);
LEAF_NODE *exec_read_value(INDEX_NODE *, int);
RUN_RESULT update_value(INDEX_NODE *, DATA_RECORD *);
NODE_PATH_INFO *search_special_leaf(INDEX_NODE *, FIRST_OR_LAST);
SINGLE_LEAF_LINK *exec_top_search(INDEX_NODE *, int, ASC_DSC);
SINGLE_LEAF_LINK *exec_bottom_search(INDEX_NODE *,int, ASC_DSC);
LEAF_NODE *search_near_leaf(INDEX_NODE *, int, SUB_UP);
DOUBLE_LEAF_LINK *generate_double_leaf_link(LEAF_NODE *, LEAF_NODE *);
DOUBLE_LEAF_LINK *exec_range_search(INDEX_NODE *, int, int);
int fetch_pos(INDEX_NODE *, int);
INDEX_NODE *split_node(INDEX_NODE *, int, int *, INDEX_NODE **);
NODE_ANALYZE_RES *analyze_influnce_nodes(NODE_PATH_INFO *, ACTION);
RUN_RESULT delete_node(INDEX_NODE *, int);
RUN_RESULT insert_node(INDEX_NODE *, DATA_RECORD *);
RUN_RESULT range_search(INDEX_NODE *, int, int, ASC_DSC);
RUN_RESULT top_search(INDEX_NODE *, int, ASC_DSC);
RUN_RESULT bottom_search(INDEX_NODE *, int, ASC_DSC);
RUN_RESULT delete_whole_tree(INDEX_NODE *);
RUN_RESULT read_value(INDEX_NODE *, int);
NODE_ANALYZE_RES *analyze_insert_influnce_nodes(NODE_PATH_INFO *, int);
IDX_BOOL need_modify_pri(INDEX_NODE *, int);
RUN_RESULT insert_to_file(char *, DATA_RECORD *);
RUN_RESULT delete_from_file(char *, DATA_RECORD *);
RUN_RESULT exec_delete_from_file(fstream, DATA_RECORD *);
RUN_RESULT update_to_file(char *, DATA_RECORD *);
void exec_insert_index_node(INDEX_NODE *, INDEX_NODE_LINK *, INDEX_NODE *);
RUN_RESULT insert_sub_tree(INDEX_NODE *, INDEX_NODE *, INDEX_NODE *);
void exec_delete_a_sub_tree(INDEX_NODE *, INDEX_NODE_LINK *, IDX_BOOL,
                INDEX_NODE *, LEAF_NODE *, LEAF_NODE *, LEAF_NODE *);
INDEX_NODE *batch_insert(INDEX_NODE *, DATA_RECORD_LIST *);
INDEX_NODE *bactch_delete(INDEX_NODE *, int, int);
SUB_TREE_LIST_INFO *get_sub_tree_info(INDEX_NODE *, LEAF_NODE *, LEAF_NODE *);
SUB_TREE_INFO_LINK *analyze_influence_sub_tree(INDEX_NODE *, INDEX_NODE *, int, int);
LEAF_NODE *quick_search_special(INDEX_NODE *, FIRST_OR_LAST);
void exec_delete_sub_trees(INDEX_NODE *, SUB_TREE_LIST_INFO *);
NODE_PATH_INFO *fetch_index_path(INDEX_NODE *, INDEX_NODE *);
DATA_INFO *fetch_leaf_list_data_info(LEAF_NODE *, LEAF_NODE *);

/*Following for log operations.*/
RUN_RESULT exec_write_log(PID, ACTION, DATA_RECORD *);
LOG_INFO *exec_read_log(int);
INDEX_NODE *redo_according_log(INDEX_NODE *, LOG_INFO *);
LOG_INFO *read_recent_logs(int);

/*Following for backup and recorvery.*/
RUN_RESULT exec_write_all_data(INDEX_NODE *, char *);
INDEX_NODE *data_recovery(void);
BACK_INFO *search_backup_info(void);
DATA_INFO *exec_read_data(char *);
BACK_INFO *search_backup_file(void);
int write_file_according_log(char *, LOG_INFO *);
RUN_RESULT auto_backup(void);

/*Following for log and backup test.*/
RUN_RESULT run_backup_write_read_test(INDEX_NODE *);
RUN_RESULT run_log_write_read_test(INDEX_NODE *);
RUN_RESULT run_read_recent_log_test(INDEX_NODE *);
void print_log_details(LOG_INFO *);

/*Global Varibles*/
extern INDEX_NODE *test_root;
