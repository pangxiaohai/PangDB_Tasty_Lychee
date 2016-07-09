#include <string>
#include <math.h>

using namespace std;

/*Definition marco.*/

#define MAXKEYNUM 4
#define LEAST_P_NUM (short)(ceil(MAXKEYNUM/2)+1)
#define MAXLENGTH 255
#define ALL_REST_NODES -1
#define NO_LOCK 0
#define READ_LOCK 1
#define WRITE_LOCK 2
#define LEAF_NODE_TYPE (unsigned char)0
#define INDEX_NODE_TYPE (unsigned char)1

/*Define data sturcts.*/


enum RUN_RESULT {RUN_FAILED = 0, RUN_SUCCESS = 1};

enum IDX_BOOL {FALSE = 0, TRUE = 1};

enum SEARCH_DIR {FORWARD, BACKWORD};

enum ASC_DSC {ASC, DSC};

enum HOLDER_STATUS {NO_RIGHTS, EMPTY, NOT_EMPTY, REMOVE_SUCCESS};

enum ON_OFF {OFF = 0, ON = 1};

enum FIRST_OR_LAST {FIRST_NODE = 0, LAST_NODE = 1};

enum SUB_UP {SUB_MARK = 0, UP_MARK = 1};

enum ACTION {READ, INSERT, UPDATE, DELETE};

typedef int PID;

typedef struct data_record
{
	int key;
	char value[MAXLENGTH];
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


/*Declare Function.*/

/*Following for initialization.*/
INDEX_NODE *mk_index(DATA_RECORD_LIST *, int);

/*Following for function support.*/
IDX_BOOL is_leaf_node(INDEX_NODE *);

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

/*Following for memory free.*/
void free_index_node_link_mem(INDEX_NODE_LINK *);
void free_node_path_info_mem(NODE_PATH_INFO *);
void free_node_analyze_res_mem(NODE_ANALYZE_RES *);
void free_single_leaf_link_mem(SINGLE_LEAF_LINK *);
void free_double_leaf_link_mem(DOUBLE_LEAF_LINK *);
void free_level_info_mem(LEVEL_INFO *);
void free_data_record_list_mem(DATA_RECORD_LIST *);
void free_a_tree_mem(INDEX_NODE *);

/*Following for test support!*/
RUN_RESULT run_mk_BIndex_test(INDEX_NODE *);
void run_test(void);
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
void run_exist(void);
RUN_RESULT run_top_bottom_test(INDEX_NODE *);
void end_test(void);
RUN_RESULT run_read_test(INDEX_NODE *);
int produce_actual_key(INDEX_NODE *, int);
RUN_RESULT run_update_test(INDEX_NODE *);
RUN_RESULT run_delete_test(INDEX_NODE *);
RUN_RESULT run_insert_test(INDEX_NODE *);

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
RUN_RESULT delete_a_tree(INDEX_NODE *);
RUN_RESULT read_value(INDEX_NODE *, int);
NODE_ANALYZE_RES *analyze_insert_influnce_nodes(NODE_PATH_INFO *, int);
IDX_BOOL need_modify_pri(INDEX_NODE *, int);
