/*
 * Copyright (c) 2005 Topspin Communications.  All rights reserved.
 * Copyright (c) 2009-2010 Mellanox Technologies.  All rights reserved.
 */

#if HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */

#define CORE_DIRECT_DEBUG

#ifdef CORE_DIRECT_DEBUG
#define messaged(args...) printf("(%s: %d) in function %s: ",__FILE__,__LINE__,__func__); printf(args)
#else
#define messaged(args...) printf("")
#endif //CORE_DIRECT_DEBUG



#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netdb.h>
#include <malloc.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <time.h>
#include <math.h>
#include <infiniband/arch.h> 
#include <rdma/rdma_cma.h>

////////////////////////////////////////////////////////////////////////////
// Legacy CD API (mverbs)
// export CD_PATH=/hpc/local/src/cd/install
// gcc -o mverbs_pp mverbs_pingpong.c -I$CD_PATH/include -L$CD_PATH/lib64 -Wl,--rpath,$CD_PATH/lib64 -libverbs -lrdmacm -lmqe -lmverbs
//
// MOFED2.0+ CD API (verbs)
// gcc -o mverbs_pp mverbs_pingpong.c -libverbs -lrdmacm
//
// CALC_SUPPORT - enable/disable CALC operations code (off by default)
// USE_RDMA - enable/disable new Core-Direct API code (off by default)
////////////////////////////////////////////////////////////////////////////


#if defined(CALC_SUPPORT)
#	include "cc2_pingpong.h"
#else

static inline enum ibv_mtu pp_mtu_to_enum(int mtu)
{
	switch (mtu) {
	case 256:  return IBV_MTU_256;
	case 512:  return IBV_MTU_512;
	case 1024: return IBV_MTU_1024;
	case 2048: return IBV_MTU_2048;
	case 4096: return IBV_MTU_4096;
	default:   return -1;
	}
}

static inline uint16_t pp_get_local_lid(struct ibv_context *context, int port)
{
	struct ibv_port_attr attr;

	if (ibv_query_port(context, port, &attr))
		return 0;

	return attr.lid;
}
#endif /* CALC_SUPPORT */


#define CYCLE_BUFFER        (4096)
#define CACHE_LINE_SIZE     (64)
#define NUM_OF_RETRIES		(10)
#define KEY_MSG_SIZE 	 (50)   // Message size without gid.

#define BUFF_SIZE(size) ((size < CYCLE_BUFFER) ? (CYCLE_BUFFER) : (size))

typedef unsigned long int	uint64_t;

#define ALLOCATE(var,type,size)                                     \
    { if((var = (type*)malloc(sizeof(type)*(size))) == NULL)        \
        { fprintf(stderr," Cannot Allocate\n"); exit(1);}}


// Connection types availible.
#define RC  (0)
#define UC  (1)
#define UD  (2)
#define RawEth  (3)
// #define XRC 3 (TBD)

// Genral control definitions
#define OFF	     			(0)
#define ON 	     			(1)
#define SUCCESS	     		(0)
#define FAILURE	     		(1)
#define MTU_FIX	     		(7)
#define MAX_SIZE     		(8388608)
#define LINK_FAILURE 		(4)
#define MAX_OUT_READ_HERMON (16)
#define MAX_OUT_READ        (4)
#define UD_ADDITION         (40)
#define RAWETH_ADDITTION    (18)
#define HW_CRC_ADDITION    (4)
#define MAX_NODE_NUM		(8)

// Default Values of perftest parameters
#define DEF_PORT      (18515)
#define DEF_IB_PORT   (1)
#define DEF_SIZE_BW   (65536)
#define DEF_SIZE_LAT  (2)
#define DEF_ITERS     (1000)
#define DEF_ITERS_WB  (5000)
#define DEF_TX_BW     (300)
#define DEF_TX_LAT    (2000)
#define DEF_QP_TIME   (14)
#define DEF_SL	      (0)
#define DEF_GID_INDEX (-1)
#define DEF_NUM_QPS   (1)
#define DEF_INLINE_BW (0)
#define DEF_INLINE_LT (400)
#define DEF_RX_RDMA   (1)
#define DEF_RX_SEND   (600)
#define DEF_CQ_MOD    (50)
#define DEF_TOS		  (-1)
#define DEF_DURATION  (10)
#define DEF_MARGIN    (2)

// Max and Min allowed values for perftest parameters.
#define MIN_IB_PORT   (1)
#define MAX_IB_PORT   (2)
#define MIN_ITER      (5)
#define MAX_ITER      (100000000)
#define MIN_TX 	      (50)
#define MAX_TX	      (15000)
#define MIN_SL	      (0)
#define MAX_SL	      (15)
#define MIN_GID_IX    (0)
#define MAX_GID_IX    (64)
#define MIN_QP_NUM    (1)
#define MAX_QP_NUM    (8)
#define MIN_INLINE    (0)
#define MAX_INLINE    (400)
#define MIN_QP_MCAST  (1)
#define MAX_QP_MCAST  (56)
#define MIN_RX	      (1)
#define MAX_RX	      (15000)
#define MIN_CQ_MOD    (1)
#define MAX_CQ_MOD    (1000)
#define MIN_TOS 	  (0)
#define MAX_TOS		  (256)
#define RAWETH_MIN_MSG_SIZE    (64)

// The Verb of the benchmark.
typedef enum {
	SEND, WRITE, READ
} VerbType;

// The type of the machine ( server or client actually).
typedef enum {
	LAT, BW
} TestType;

// The type of the machine ( server or client actually).
typedef enum {
	SERVER, CLIENT
} MachineType;

// The type of the machine ( server or client actually).
typedef enum {
	LOCAL, REMOTE
} PrintDataSide;

// The type of the device (Hermon B0/A0 or no)
typedef enum {
	ERROR = -1, NOT_HERMON = 0, HERMON = 1
} Device;


// Type of test method.
typedef enum {
	ITERATIONS, DURATION
} TestMethod;

#define MAX(x, y) 		(((x) > (y)) ? (x) : (y))
#define MIN(x, y) 		(((x) < (y)) ? (x) : (y))
#define LAMBDA			0.00001

// The Format of the message we pass through sockets , without passing Gid.
#define KEY_PRINT_FMT "%04x:%04x:%06x:%06x:%08x:%016Lx"

//#   define errno (*__errno_location ())

#if defined(CALC_SUPPORT)
#define EXEC_INT(calc_op, op1, op2)					\
	((calc_op) == IBV_M_CALC_OP_LXOR	? (((op1) != (op2)))	\
	: (calc_op) == IBV_M_CALC_OP_BXOR	? (((op1) ^  (op2)))	\
	: (calc_op) == IBV_M_CALC_OP_LOR 	? (((op1) || (op2)))	\
	: (calc_op) == IBV_M_CALC_OP_BOR	? (((op1) |  (op2)))	\
	: (calc_op) == IBV_M_CALC_OP_LAND	? (((op1) && (op2)))	\
	: (calc_op) == IBV_M_CALC_OP_BAND	? (((op1) &  (op2)))	\
	: EXEC_FLOAT(calc_op, op1, op2))

#define EXEC_FLOAT(calc_op, op1, op2)					\
	((calc_op) == IBV_M_CALC_OP_ADD		? (((op1) +  (op2)))	\
	: (calc_op) == IBV_M_CALC_OP_MAX	? (MAX((op1), (op2)))	\
	: (calc_op) == IBV_M_CALC_OP_MIN	? (MIN((op1), (op2)))	\
	: (calc_op) == IBV_M_CALC_OP_MAXLOC	? (MAX((op1), (op2)))	\
	: (calc_op) == IBV_M_CALC_OP_MINLOC	? (MIN((op1), (op2)))	\
	: 0)

#define VERIFY_FLOAT(calc_op, op1, op2, res)       			\
	((calc_op) == IBV_M_CALC_OP_ADD ?				\
		((fabs(EXEC_FLOAT(calc_op, op1, op2) - (res))) < LAMBDA)\
	: ((EXEC_FLOAT(calc_op, op1, op2)) == (res)))			\


#define VERIFY_INT(calc_op, op1, op2, res)				\
	EXEC_INT(calc_op, op1, op2) == (res)

#define EXEC_VER_FLOAT(verify, calc_op, data_type, op1, op2, res) 	\
	((verify) ?                                                     \
		(VERIFY_FLOAT(calc_op, ((data_type)*op1),     		\
			((data_type)*op2), ((data_type)*res)))          \
	: EXEC_FLOAT(calc_op, ((data_type)*op1), ((data_type)*op2)))

#define EXEC_VER_INT(verify, calc_op, data_type, op1, op2, res) 	\
	((verify) ?                                                     \
		(VERIFY_INT(calc_op, ((data_type)*op1),       		\
			((data_type)*op2), ((data_type)*res)))          \
	: EXEC_INT(calc_op, ((data_type)*op1), ((data_type)*op2)))

#define EXEC_VERIFY(calc_data_type, calc_op, verify, op1, op2, res)   	\
	((calc_data_type) == IBV_M_DATA_TYPE_INT8 ? 			\
		EXEC_VER_INT(verify, calc_op, int8_t, op1, op2, res)	\
	: (calc_data_type) == IBV_M_DATA_TYPE_INT16 ?			\
		EXEC_VER_INT(verify, calc_op, int16_t, op1, op2, res)	\
	: (calc_data_type) == IBV_M_DATA_TYPE_INT32 ?                    \
		EXEC_VER_INT(verify, calc_op, int32_t, op1, op2, res)	\
	: (calc_data_type) == IBV_M_DATA_TYPE_INT64 ?                    \
		EXEC_VER_INT(verify, calc_op, int64_t, op1, op2, res)	\
	: (calc_data_type) == IBV_M_DATA_TYPE_FLOAT32 ?			\
		EXEC_VER_FLOAT(verify, calc_op, float, op1, op2, res)	\
	: (calc_data_type) == IBV_M_DATA_TYPE_FLOAT64 ?                  \
		EXEC_VER_FLOAT(verify, calc_op, double, op1, op2, res)	\
	: 0)

// EXEC_VER_FLOAT(verify, calc_op, FLOAT64, op1, op2, res)

struct pingpong_calc_ctx {
	enum ibv_m_wr_calc_op opcode;
	enum ibv_m_wr_data_type data_type;
	void *gather_buff;
	int gather_list_size;
	struct ibv_sge *gather_list;
};
#endif /* CALC_SUPPORT */

enum {
	PP_RECV_WRID = 1, PP_SEND_WRID = 2, PP_CQE_WAIT = 3,
};

char *wr_id_str[] = { [PP_RECV_WRID] = "RECV", [PP_SEND_WRID] = "SEND",
		[PP_CQE_WAIT] = "CQE_WAIT", };

static long page_size;


#if 0
static const char *eventArray[] = { "RDMA_CM_EVENT_ADDR_RESOLVED",
		"RDMA_CM_EVENT_ADDR_ERROR", "RDMA_CM_EVENT_ROUTE_RESOLVED",
		"RDMA_CM_EVENT_ROUTE_ERROR", "RDMA_CM_EVENT_CONNECT_REQUEST",
		"RDMA_CM_EVENT_CONNECT_RESPONSE", "RDMA_CM_EVENT_CONNECT_ERROR",
		"RDMA_CM_EVENT_UNREACHABLE", "RDMA_CM_EVENT_REJECTED",
		"RDMA_CM_EVENT_ESTABLISHED", "RDMA_CM_EVENT_DISCONNECTED",
		"RDMA_CM_EVENT_DEVICE_REMOVAL", "RDMA_CM_EVENT_MULTICAST_JOIN",
		"RDMA_CM_EVENT_MULTICAST_ERROR", "RDMA_CM_EVENT_ADDR_CHANGE",
		"RDMA_CM_EVENT_TIMEWAIT_EXIT" };
#endif

struct perftest_comm {
	struct pingpong_context *rdma_ctx;
	struct perftest_parameters *rdma_params;
	int sockfd_sd;
};

struct report_options {
	int unsorted;
	int histogram;
	int cycles; /* report delta's in cycles, not microsec's */
};

struct perftest_parameters {

	int port;
	int num_of_nodes;
	char *ib_devname;
	char *servername;
	int ib_port;
	int mtu;
	enum ibv_mtu curr_mtu;
	uint64_t size;
	int iters;
	int tx_depth;
	int qp_timeout;
	int sl;
	int gid_index;
	int all;
	int cpu_freq_f;
	int connection_type;
	int num_of_qps;
	int use_event;
	int inline_size;
	int out_reads;
	int use_mcg;
	int use_rdma_cm;
	int work_rdma_cm;
	char *user_mgid;
	int rx_depth;
	int duplex;
	int noPeak;
	int cq_mod;
	int spec;
	int tos;
	uint8_t link_type;
	MachineType machine;
	PrintDataSide side;
	VerbType verb;
	TestType tst;
	int sockfd;
	int sockfd_sd;
	int cq_size;
	float version;
	struct report_options *r_flag;
	volatile int state;
	int duration;
	int margin;
	TestMethod test_type;
	int calc_first_byte_latency;
#if defined(CALC_SUPPORT)
	/*
	 * core direct test additions:
	 */
	int verbose;
	int verify;
	enum ibv_m_wr_data_type calc_data_type;
	char *calc_operands_str;
	enum ibv_m_wr_calc_op calc_opcode;
	int mqe_poll;
#endif /* CALC_SUPPORT */

};

struct perftest_parameters user_param;


struct pingpong_context {
	struct ibv_context *context;
	struct ibv_comp_channel *channel;
	struct ibv_pd *pd[MAX_NODE_NUM];
	struct ibv_mr *mr[MAX_NODE_NUM];
	struct ibv_cq *cq;
	struct ibv_qp *qp[MAX_NODE_NUM]; //in the write_lat.c code define **qp!

	struct ibv_qp *mqp;
	struct ibv_cq *mcq;

	void *buf_for_calc_operands;
	void *net_buf[MAX_NODE_NUM];
	int size;
	int rx_depth;
	int pending;
	uint64_t last_result;

#if defined(CALC_SUPPORT)
	struct pingpong_calc_ctx calc_op;
#endif /* CALC_SUPPORT */
	struct ibv_ah *ah; //  add this
	int tx_depth;
	int *scnt;
	int *ccnt;
	struct rdma_event_channel *cm_channel;
	struct rdma_cm_id *cm_id_control;
	struct rdma_cm_id *cm_id;
	uint64_t *my_addr;
};

struct pingpong_dest {
	int 			   lid;
	int 			   out_reads;
	int 			   qpn;
	int 			   psn;
	unsigned           rkey;
	unsigned long long vaddr;
	union ibv_gid      gid;
	uint8_t mac[6];
};

static int pp_connect_ctx(struct pingpong_context *ctx, struct ibv_qp *qp,
		int port, int my_psn, enum ibv_mtu mtu, int sl,
		struct pingpong_dest *dest) {
	struct ibv_qp_attr attr = {
		.qp_state 		= IBV_QPS_RTR,
		.path_mtu 		= mtu,
		.dest_qp_num 		= dest->qpn,
		.rq_psn 		= dest->psn,
		.max_dest_rd_atomic = 1,
		.min_rnr_timer 	= 12,
		.ah_attr.is_global  = 0,
		.ah_attr.dlid       = dest->lid,
		.ah_attr.sl         = sl,
		.ah_attr.src_path_bits = 0,
		.ah_attr.port_num   = port
	};

	if (ibv_modify_qp(qp, &attr,
			  IBV_QP_STATE              |
			  IBV_QP_AV                 |
			  IBV_QP_PATH_MTU           |
			  IBV_QP_DEST_QPN           |
			  IBV_QP_RQ_PSN             |
			  IBV_QP_MAX_DEST_RD_ATOMIC |
			  IBV_QP_MIN_RNR_TIMER)) {
		fprintf(stderr, "%s: Failed to modify QP to RTR\n", __func__);
		return 1;
	}

	attr.qp_state 	    = IBV_QPS_RTS;
	attr.timeout 	    = 14;
	attr.retry_cnt 	    = 7;
	attr.rnr_retry 	    = 7;
	attr.sq_psn 	    = my_psn;
	attr.max_rd_atomic  = 1;
	if (ibv_modify_qp(qp, &attr,
			  IBV_QP_STATE              |
			  IBV_QP_TIMEOUT            |
			  IBV_QP_RETRY_CNT          |
			  IBV_QP_RNR_RETRY          |
			  IBV_QP_SQ_PSN             |
			  IBV_QP_MAX_QP_RD_ATOMIC)) {
		fprintf(stderr, "%s: Failed to modify QP to RTS\n", __func__);
		return 1;
	}

	return 0;
}

static struct pingpong_dest **pp_client_exch_dest(const char *servername,
		int port, const struct pingpong_dest *my_dest, int *sockfd_ret) {
	struct addrinfo *res, *t;
	struct addrinfo hints =
			{ .ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM };
	char *service;
	char msg[KEY_MSG_SIZE];
	int n;
	int sockfd = -1;
	struct pingpong_dest **rem_dest = NULL;


	rem_dest = malloc(sizeof(struct pingpong_dest*)*1);

	if (asprintf(&service, "%d", port) < 0)
		return NULL;

	n = getaddrinfo(servername, service, &hints, &res);
	if (n < 0) {
		fprintf(stderr, "%s for %s:%d\n", gai_strerror(n), servername, port);
		free(service);
		return NULL;
	}

	for (t = res; t; t = t->ai_next) {
		sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
		if (sockfd >= 0) {
			if (!connect(sockfd, t->ai_addr, t->ai_addrlen))
				break;
			close(sockfd);
			sockfd = -1;
		}
	}

	freeaddrinfo(res);
	free(service);

	if (sockfd < 0) {
		fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
		return NULL;
	}

	sprintf(msg,KEY_PRINT_FMT,my_dest->lid,my_dest->out_reads,my_dest->qpn,my_dest->psn, my_dest->rkey, my_dest->vaddr);
	if (write(sockfd, msg, sizeof msg) != sizeof msg) {
		fprintf(stderr, "Couldn't send local address\n");
		goto out;
	}

	if (read(sockfd, msg, sizeof msg) != sizeof msg) {
		perror("client read");
		fprintf(stderr, "Couldn't read remote address\n");
		goto out;
	}

	write(sockfd, "done", sizeof "done");

	rem_dest[0] = malloc(sizeof(struct pingpong_dest));
		if (!rem_dest[0])
			goto out;

	sscanf(msg,KEY_PRINT_FMT,&rem_dest[0]->lid,&rem_dest[0]->out_reads,&rem_dest[0]->qpn,&rem_dest[0]->psn, &rem_dest[0]->rkey,&rem_dest[0]->vaddr);

	out:
//	close(sockfd);
	*sockfd_ret=sockfd;

	return rem_dest;
}

static struct pingpong_dest **pp_server_exch_dest(struct pingpong_context *ctx,
		int ib_port, enum ibv_mtu mtu, int port, int sl,
		const struct pingpong_dest my_dest[MAX_NODE_NUM], int num_of_nodes, int *connfd_ret) {
	struct addrinfo *res, *t;
	struct addrinfo hints = { .ai_flags = AI_PASSIVE, .ai_family = AF_UNSPEC,
			.ai_socktype = SOCK_STREAM };
	char *service;
	char msg[KEY_MSG_SIZE];
	int n,nodeind;
	int sockfd = -1;  //, connfd;
	struct pingpong_dest **rem_dest = NULL;
	int connfd[MAX_NODE_NUM];

	rem_dest = malloc(sizeof(struct pingpong_dest*)*num_of_nodes);



		if (asprintf(&service, "%d", port) < 0)
			return NULL;

		/*
		 *
		 */
		n = getaddrinfo(NULL, service, &hints, &res);

		if (n < 0) {
			fprintf(stderr, "%s for port %d\n", gai_strerror(n), port);
			free(service);
			return NULL;
		}

		for (t = res; t; t = t->ai_next) {
			sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
			if (sockfd >= 0) {
				n = 1;
				setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);
				if (!bind(sockfd, t->ai_addr, t->ai_addrlen))
					break;
				close(sockfd);
				sockfd = -1;
			}
		}

		freeaddrinfo(res);
		free(service);

		if (sockfd < 0) {
			fprintf(stderr, "Couldn't listen to port %d\n", port);
			return NULL;
		}



	for (nodeind = 0; nodeind < num_of_nodes; nodeind++) {   /////////////work?  how??
		messaged("server number  %d\n",nodeind);
		listen(sockfd, 1);
		connfd[nodeind] = accept(sockfd, NULL, 0);
	}
	close(sockfd);

	for (nodeind = 0; nodeind < num_of_nodes; nodeind++)
		if (connfd[nodeind] < 0) {
			fprintf(stderr, "accept() failed\n");
			return NULL;
		}


		for (nodeind = 0; nodeind < num_of_nodes; nodeind++) {
		n = read(connfd[nodeind], msg, sizeof msg);
		if (n != sizeof msg) {
			perror("server read");
			fprintf(stderr, "%d/%d: Couldn't read remote address\n", n,
					(int) sizeof msg);
			goto out;
		}

		rem_dest[nodeind] = malloc(sizeof(struct pingpong_dest));
		if (!rem_dest[nodeind])
			goto out;


//	sscanf(msg, "%x:%x:%x", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn);
		sscanf(msg, KEY_PRINT_FMT, &rem_dest[nodeind]->lid, &rem_dest[nodeind]->out_reads,
				&rem_dest[nodeind]->qpn,&rem_dest[nodeind]->psn, &rem_dest[nodeind]->rkey,
				&rem_dest[nodeind]->vaddr);

		if (pp_connect_ctx(ctx, ctx->qp[nodeind], ib_port, my_dest[nodeind].psn, mtu, sl,
				rem_dest[nodeind])) {
			fprintf(stderr, "Couldn't connect to remote QP\n");
			free(rem_dest);		///todo memory_leak
			rem_dest = NULL;
			goto out;
		}



//	sprintf(msg, "%04x:%06x:%06x", my_dest->lid, my_dest->qpn, my_dest->psn);
		sprintf(msg, KEY_PRINT_FMT, my_dest[nodeind].lid, my_dest[nodeind].out_reads,
				my_dest[nodeind].qpn, my_dest[nodeind].psn, my_dest[nodeind].rkey, my_dest[nodeind].vaddr);

		if (write(connfd[nodeind], msg, sizeof msg) != sizeof msg) {
			fprintf(stderr, "Couldn't send local address\n");
			free(rem_dest);  ///todo memory_leak
			rem_dest = NULL;
			goto out;
		}


		read(connfd[nodeind], msg, sizeof msg);
		}

	out:

	for (nodeind = 0; nodeind < num_of_nodes; nodeind++)
//				close(connfd[nodeind]);
		connfd_ret[nodeind]=connfd[nodeind];



	return rem_dest;
}

#if defined(CALC_SUPPORT)

int __parse_calc_to_gather(char *ops_str, enum ibv_m_wr_calc_op calc_op,
		enum ibv_m_wr_data_type data_type, struct pingpong_calc_ctx *calc_ctx,
		struct ibv_context *ibv_ctx, void *buff, void *net_buff) {
	int i, num_operands;
	int sz;
	char *__gather_token, *__err_ptr = NULL;

	if (!ops_str) {
		fprintf(stderr, "You must choose an operation to perform.\n");
		return -1;
	}
	sz = ibv_m_data_type_to_size(data_type);

	for (i = 0, num_operands = 1; i < strlen(ops_str); i++) {
		if (ops_str[i] == ',')
			num_operands++;
	}

	calc_ctx->gather_list_size = num_operands;

	__gather_token = strtok(ops_str, ",");
	if (!__gather_token)
		return -1;
	/* Build the gather list, assume one operand per sge. todo: improve for any nr of operands */
	for (i = 0; i < num_operands; i++) {
		/* copy the operands to the buffer */
		switch (data_type) {
		case IBV_M_DATA_TYPE_INT8:
			return -1;

		case IBV_M_DATA_TYPE_INT16:
			return -1;

		case IBV_M_DATA_TYPE_INT32:
			*((int32_t *) buff + i * 4) = strtol(__gather_token, &__err_ptr, 0);
			break;

		case IBV_M_DATA_TYPE_INT64:
			*((int64_t *) buff + i * 2) = strtoll(__gather_token, &__err_ptr,
					0);
			break;

		case IBV_M_DATA_TYPE_INT128:
			return -1;

		case IBV_M_DATA_TYPE_FLOAT32:
			*((float *) buff + i * 4) = strtof(__gather_token, &__err_ptr);
			break;

		case IBV_M_DATA_TYPE_FLOAT64:
			// *((FLOAT64 *) buff + i * 2) = strtof(__gather_token, &__err_ptr);
			*((double*) buff + i * 2) = strtof(__gather_token, &__err_ptr);
			break;

		case IBV_M_DATA_TYPE_FLOAT96:
			return -1;

		case IBV_M_DATA_TYPE_FLOAT128:
			return -1;

		case IBV_M_DATA_TYPE_COMPLEX:
			return -1;

		default:
			return -1;
		}

		if (pack_data_for_calc(ibv_ctx, calc_op, data_type, 0,
				(int64_t *) buff + i * 2, 0, &calc_ctx->opcode,
				&calc_ctx->data_type, (uint64_t *) net_buff + i * 2)) {
			fprintf(stderr, "Error in pack \n");
			return -1;
		}
		__gather_token = strtok(NULL, ",");
		if (!__gather_token)
			break;

	}

	calc_ctx->gather_buff = net_buff;

	return num_operands;
}

int prepare_sg_list(int op_per_gather, int num_operands, uint32_t lkey,
		struct pingpong_calc_ctx *calc_ctx, void *buff) {
	int num_sge, sz;
	int i, gather_ix;
	struct ibv_sge *gather_list = NULL;

	sz = ibv_m_data_type_to_size(calc_ctx->data_type);
	num_sge = (num_operands / op_per_gather)
			+ ((num_operands % op_per_gather) ? 1 : 0); /* todo - change to ceil. requires -lm */

	gather_list = calloc(num_sge, sizeof(*gather_list));
	if (!gather_list) {
		fprintf(stderr, "Failed to allocate %Zu bytes for gather_list\n",
				(num_sge * sizeof(*gather_list)));
		return -1;
	}

	/* Build the gather list */
	for (i = 0, gather_ix = 0; i < num_operands; i++) {
		if (!(i % op_per_gather)) {
			gather_list[gather_ix].addr = (uint64_t) buff + ((sz + 8) * i);
			gather_list[gather_ix].length = (sz + 8) * op_per_gather;
			gather_list[gather_ix].lkey = lkey;

			gather_ix++;
		}
	}

	calc_ctx->gather_list = gather_list;

	return 0;
}

#endif /* CALC_SUPPORT */


#if defined(CALC_SUPPORT)
int pp_init_ctx(struct pingpong_context *ctx, struct ibv_device *ib_dev, int size,
		int tx_depth, int rx_depth, int port, int use_event,
		enum ibv_m_wr_calc_op calc_op, enum ibv_m_wr_data_type calc_data_type,
		char *calc_operands_str, VerbType verb)
#else
int pp_init_ctx(struct pingpong_context *ctx, struct ibv_device *ib_dev, int size,
		int tx_depth, int rx_depth, int port, int use_event,
		VerbType verb)
#endif /* CALC_SUPPORT */
{

	//int rc = 0;
	int flags;
	uint64_t buff_size;

	ctx->size = size;
	ctx->rx_depth = rx_depth;

#if defined(CALC_SUPPORT)
	ctx->calc_op.opcode = calc_op;
	ctx->calc_op.data_type = calc_data_type;
#endif /* CALC_SUPPORT */

	buff_size = BUFF_SIZE(ctx->size) * 2 * user_param.num_of_qps;
	ctx->buf_for_calc_operands = memalign(page_size, buff_size);
	if (!ctx->buf_for_calc_operands) {
		fprintf(stderr, "Couldn't allocate work buf.\n");
		goto clean_ctx;
	}
	memset(ctx->buf_for_calc_operands, 0, size);

	ctx->net_buf[0] =memalign(page_size, buff_size);
	if (!ctx->net_buf[0]) {
		fprintf(stderr, "Couldn't allocate work buf.\n");
		goto clean_buffer;
	}
	memset(ctx->net_buf[0], 0, buff_size);

	flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;



	ctx->context = ibv_open_device(ib_dev);
	if (!ctx->context) {
		fprintf(stderr, "Couldn't get context for %s\n",
				ibv_get_device_name(ib_dev));
		goto clean_net_buf;
	}

	ctx->channel = NULL;
	if (use_event) {
		ctx->channel = ibv_create_comp_channel(ctx->context);
		if (!ctx->channel) {
			fprintf(stderr, "Couldn't create completion channel\n");
			goto clean_device;
		}
	} else
		ctx->channel = NULL;

	ctx->pd[0] = ibv_alloc_pd(ctx->context);
	if (!ctx->pd[0]) {
		fprintf(stderr, "Couldn't allocate PD\n");
		goto clean_comp_channel;
	}

	if (verb == READ){
		flags |= IBV_ACCESS_REMOTE_READ;
	}


	ctx->mr[0] = ibv_reg_mr(ctx->pd[0], ctx->net_buf[0], buff_size, flags);  ///sasha,  was  size
	if (!ctx->mr[0]) {
		fprintf(stderr, "Couldn't register MR\n");
		goto clean_pd;
	}

	///////////////////////////////////////////////////////////////////////////////////////
	ctx->cq = ibv_create_cq(ctx->context, rx_depth + 1, NULL, ctx->channel, 0);  //sasha  was rx_depth
	if (!ctx->cq) {
		fprintf(stderr, "Couldn't create CQ (%s:%d)\n", __FUNCTION__, __LINE__);
		goto clean_mr;
	}
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	{
		struct ibv_exp_qp_init_attr attr = {
			.send_cq = ctx->cq,
			.recv_cq = ctx->cq,
			.cap     = {
				.max_send_wr  = tx_depth,
				.max_recv_wr  = rx_depth,
				.max_send_sge = 1,
				.max_recv_sge = 1,
				.max_inline_data = 8,
			},
			.qp_type = IBV_QPT_RC
		};
		attr.comp_mask |= IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS | IBV_EXP_QP_INIT_ATTR_PD;
		attr.exp_create_flags = IBV_EXP_QP_CREATE_CROSS_CHANNEL;

		attr.pd = ctx->pd[0];
		ctx->qp[0] = ibv_exp_create_qp(ctx->context, &attr);
		if (!ctx->qp[0]) {
			fprintf(stderr, "Couldn't create QP\n");
			goto clean_cq;
		}
	}


	{
		struct ibv_qp_attr attr = {
				.qp_state = IBV_QPS_INIT,
				.pkey_index = 0,
				.port_num = port,
				.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE
		};

		if (ibv_modify_qp(ctx->qp[0], &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
			fprintf(stderr, "Failed to modify QP to INIT\n");
			goto clean_qp;
		}
	}

	return 0;

	/* clean_mqp: */ ibv_destroy_qp(ctx->mqp);

	/* clean_mcq: */ ibv_destroy_cq(ctx->mcq);

	clean_qp: ibv_destroy_qp(ctx->qp[0]);

	clean_cq: ibv_destroy_cq(ctx->cq);

	clean_mr: ibv_dereg_mr(ctx->mr[0]);

	clean_pd: ibv_dealloc_pd(ctx->pd[0]);

	clean_comp_channel:
		if (ctx->channel) {
			ibv_destroy_comp_channel(ctx->channel);
		}

	clean_device: ibv_close_device(ctx->context);

	clean_net_buf: free(ctx->net_buf[0]);

	clean_buffer: free(ctx->buf_for_calc_operands);

	clean_ctx: free(ctx);

	return 0;
}
//////////////////////////////////////////////////

#if defined(CALC_SUPPORT)
int pp_init_ctx_server(struct pingpong_context *ctx, struct ibv_device *ib_dev, int size,
		int tx_depth, int rx_depth, int port, int use_event,
		enum ibv_m_wr_calc_op calc_op, enum ibv_m_wr_data_type calc_data_type,
		char *calc_operands_str, VerbType verb)
#else
int pp_init_ctx_server(struct pingpong_context *ctx, struct ibv_device *ib_dev, int size,
		int tx_depth, int rx_depth, int port, int use_event,
		VerbType verb)
#endif /* CALC_SUPPORT */
{

	int i;
	int flags;
	uint64_t buff_size;



	ctx->size = size;
	ctx->rx_depth = rx_depth;

#if defined(CALC_SUPPORT)
	ctx->calc_op.opcode = calc_op;
	ctx->calc_op.data_type = calc_data_type;
#endif /* CALC_SUPPORT */

	buff_size = BUFF_SIZE(ctx->size) * 2 * user_param.num_of_qps;
	ctx->buf_for_calc_operands = memalign(page_size, buff_size);
	if (!ctx->buf_for_calc_operands) {
		fprintf(stderr, "Couldn't allocate work buf.\n");
		goto clean_ctx;
	}
	memset(ctx->buf_for_calc_operands, 0, size);


	for(i=0; i<user_param.num_of_nodes; i++)
	{

	ctx->net_buf[i] =memalign(page_size, buff_size);
	if (!ctx->net_buf[i]) {
		fprintf(stderr, "Couldn't allocate work buf.\n");
		goto clean_buffer;
	}
	memset(ctx->net_buf[i], 0, buff_size);

	}


	flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;



	ctx->context = ibv_open_device(ib_dev);
	if (!ctx->context) {
		fprintf(stderr, "Couldn't get context for %s\n",
				ibv_get_device_name(ib_dev));
		goto clean_net_buf;
	}

	if (use_event) {
		ctx->channel = ibv_create_comp_channel(ctx->context);
		if (!ctx->channel) {
			fprintf(stderr, "Couldn't create completion channel\n");
			goto clean_device;
		}
	} else
		ctx->channel = NULL;

	for(i=0; i<user_param.num_of_nodes; i++)
	{
		ctx->pd[i] = ibv_alloc_pd(ctx->context);
		if (!ctx->pd[i]) {
			fprintf(stderr, "Couldn't allocate PD\n");
			goto clean_comp_channel;
		}
	}

	if (verb == READ){
		flags |= IBV_ACCESS_REMOTE_READ;
	}


	for(i=0; i<user_param.num_of_nodes; i++)
	{
		ctx->mr[i] = ibv_reg_mr(ctx->pd[i], ctx->net_buf[i], buff_size, flags);
		if (!ctx->mr[i]) {
			fprintf(stderr, "Couldn't register MR\n");
			goto clean_pd;
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////

	printf("rx_depth=%d\n",rx_depth);  //sasha debug, the number of cq's we get on this cq is depend on the rx_depth (~ *2)
	ctx->cq = ibv_create_cq(ctx->context, rx_depth + 1, NULL, ctx->channel, 0);  // Question? the size of rx_depth
	if (!ctx->cq) {
		fprintf(stderr, "Couldn't create CQ\n");
		goto clean_mr;
	}
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	{
		struct ibv_exp_qp_init_attr attr = {
			.send_cq = ctx->cq,
			.recv_cq = ctx->cq,
			.cap     = {
				.max_send_wr  = tx_depth,
				.max_recv_wr  = rx_depth,
				.max_send_sge = 1,
				.max_recv_sge = 1,
				.max_inline_data = 8,
			},
			.qp_type = IBV_QPT_RC
		};
		attr.comp_mask |= IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS | IBV_EXP_QP_INIT_ATTR_PD;
		attr.exp_create_flags = IBV_EXP_QP_CREATE_CROSS_CHANNEL;

		for(i=0; i<user_param.num_of_nodes; i++)
		{
			attr.pd = ctx->pd[i];
			ctx->qp[i] = ibv_exp_create_qp(ctx->context, &attr);
			if (!ctx->qp[i]) {
				fprintf(stderr, "Couldn't create QP number %d\n",i);
				goto clean_cq;
			}
		}
	}

	{
		struct ibv_qp_attr attr = { .qp_state = IBV_QPS_INIT, .pkey_index = 0,
				.port_num = port, .qp_access_flags = IBV_ACCESS_REMOTE_WRITE
						| IBV_ACCESS_LOCAL_WRITE }; /*sasha  was 0 */

		for(i=0; i<user_param.num_of_nodes; i++) {
			if (ibv_modify_qp(ctx->qp[i], &attr,
					IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT
						| IBV_QP_ACCESS_FLAGS)) {

				fprintf(stderr, "Failed to modify QP number %d to INIT\n",i);
				goto clean_qp;
		}
		}
	}

	return 0;

#if defined(CALC_SUPPORT)
	clean_mqp: ibv_destroy_qp(ctx->mqp);

	clean_mcq: ibv_destroy_cq(ctx->mcq);

	clean_qp: ibv_destroy_qp(ctx->qp);

	clean_cq: ibv_destroy_cq(ctx->cq);

	clean_mr: ibv_dereg_mr(ctx->mr);

	clean_pd: ibv_dealloc_pd(ctx->pd);

	clean_comp_channel: if (ctx->channel)
		ibv_destroy_comp_channel(ctx->channel);

	clean_device: ibv_close_device(ctx->context);

	clean_net_buf: free(ctx->net_buf);

	clean_buffer: free(ctx->buf_for_calc_operands);

	clean_ctx: free(ctx);
#else
	//clean_mqp:
	//clean_mcq:
	clean_qp:

	clean_cq:

	clean_mr:

	clean_pd:

	clean_comp_channel: if (ctx->channel)
		ibv_destroy_comp_channel(ctx->channel);

	clean_device: ibv_close_device(ctx->context);

	clean_net_buf: free(ctx->net_buf);

	clean_buffer: free(ctx->buf_for_calc_operands);

	clean_ctx: free(ctx);
#endif /* CALC_SUPPORT */

	return 0;
}




int pp_close_ctx(struct pingpong_context *ctx) {
	int i;


	for(i=0; i<user_param.num_of_nodes; i++)
		if (ctx->qp[i] && ibv_destroy_qp(ctx->qp[i])) {
			fprintf(stderr, "Couldn't destroy QP\n");
			return 1;
		}

//	if (ibv_destroy_qp(ctx->mqp)) {
//		fprintf(stderr, "Couldn't destroy MQP\n");
//		return 1;
//	}

	if (ibv_destroy_cq(ctx->cq)) {
		fprintf(stderr, "Couldn't destroy CQ (%s:%d)\n", __FUNCTION__, __LINE__);
		return 1;
	}

//	if (ibv_destroy_cq(ctx->mcq)) {
//		fprintf(stderr, "Couldn't destroy MCQ\n");
//		return 1;
//	}

	for(i=0; i<user_param.num_of_nodes; i++)
		if (ctx->mr[i] && ibv_dereg_mr(ctx->mr[i])) {
			fprintf(stderr, "Couldn't deregister MR\n");
			return 1;
		}

	for(i=0; i<user_param.num_of_nodes; i++)
		if (ctx->pd[i] && ibv_dealloc_pd(ctx->pd[i])) {
			fprintf(stderr, "Couldn't deallocate PD\n");
			return 1;
		}

	if (ctx->channel) {
		if (ibv_destroy_comp_channel(ctx->channel)) {
			fprintf(stderr, "Couldn't destroy completion channel\n");
			return 1;
		}
	}

	if (ibv_close_device(ctx->context)) {
		fprintf(stderr, "Couldn't release context\n");
		return 1;
	}

	free(ctx->buf_for_calc_operands);
	free(ctx->net_buf[0]);
	free(ctx);

	return 0;
}

static int pp_post_recv(struct pingpong_context *ctx, int n) {

	int sum=0;

	if (user_param.servername)		//client
	{

	struct ibv_sge list = { .addr = (uintptr_t) ctx->net_buf[0], .length =
			ctx->size, .lkey = ctx->mr[0]->lkey };
	struct ibv_recv_wr wr = { .wr_id = PP_RECV_WRID, .sg_list = &list,
			.num_sge = 1, .next = NULL, };
	struct ibv_recv_wr *bad_wr;
	int i;

	for (i = 0; i < n; ++i)
		if (ibv_post_recv(ctx->qp[0], &wr, &bad_wr))
			break;

	return i;
	}

	else		//server
	{
		int j;
		for (j=0; j<user_param.num_of_nodes; j++)
		{
			struct ibv_sge list = { .addr = (uintptr_t) ctx->net_buf[j], .length =
						ctx->size, .lkey = ctx->mr[j]->lkey };
				struct ibv_recv_wr wr = { .wr_id = PP_RECV_WRID, .sg_list = &list,
						.num_sge = 1, .next = NULL, };
				struct ibv_recv_wr *bad_wr;
				int i;

				for (i = 0; i < n; ++i)
					if (ibv_post_recv(ctx->qp[j], &wr, &bad_wr))
						break;

				sum=sum+i;
		}
		return sum;
	}

}

/*
 * pp_post_send:
 * post SEND request on the QP
 *
 */
static int pp_post_send(struct pingpong_context *ctx, struct pingpong_dest *rem_dest) {
	int ret;

	struct ibv_sge list = { .addr = (uintptr_t) ctx->net_buf[0], .length =
			ctx->size, .lkey = ctx->mr[0]->lkey };

	struct ibv_exp_send_wr *bad_wr;
	struct ibv_exp_send_wr wr =

			{ .wr_id = PP_SEND_WRID, .sg_list = &list,
			.num_sge = 1, 
#if defined(USE_RDMA)
			.exp_opcode = IBV_EXP_WR_RDMA_WRITE_WITH_IMM
#else
			.exp_opcode = IBV_EXP_WR_SEND
#endif  // USE_RDMA
			, .wr.rdma.remote_addr = rem_dest->vaddr, .wr.rdma.rkey = rem_dest->rkey //.send_flags = IBV_SEND_SIGNALED, //requst fo cqe
	};

	wr.exp_send_flags |= IBV_SEND_INLINE; //sasha    ////check with ofer??
#if defined(CALC_SUPPORT)
	// If this is a calc operation - set the required params in the wr
	if (ctx->calc_op.opcode != IBV_M_CALC_OP_INVALID) {
//		wr.opcode = IBV_WR_SEND;
		//wr.wr.calc_send.calc_op = ctx->calc_op.opcode;   // wr.wr.calc_send.calc_op = ctx->calc_op.opcode;
		//wr.wr.op.calc_op = ctx->calc_op.opcode;   // wr.wr.calc_send.calc_op = ctx->calc_op.opcode;
		wr.op.calc.calc_op = ctx->calc_op.opcode;   // wr.wr.calc_send.calc_op = ctx->calc_op.opcode;
		wr.op.calc.data_type = ctx->calc_op.data_type;
		wr.sg_list = ctx->calc_op.gather_list;
		wr.num_sge = ctx->calc_op.gather_list_size;
	}
#endif // CALC_SUPPORT


	ret = ibv_exp_post_send(ctx->qp[0], &wr, &bad_wr);
	if (ret) {
		messaged("error in ibv_post_send\n");
	}

	return ret;

}

#if defined(CALC_SUPPORT)
int pp_post_ext_wqe(struct pingpong_context *ctx, enum ibv_exp_wr_opcode op) {		//not in use
	int ret;
	struct ibv_exp_send_wr *bad_wr;
	struct ibv_exp_send_wr wr = {
			.wr_id = PP_CQE_WAIT, //todo - check
			.sg_list = NULL,
			.num_sge = 0,
			.exp_opcode = op,
			.exp_send_flags = IBV_EXP_SEND_SIGNALED,
	};

	switch (op) {
	case IBV_EXP_WR_RECV_ENABLE:
	case IBV_EXP_WR_SEND_ENABLE:
		wr.task.wqe_enable.qp = ctx->qp;
		wr.task.wqe_enable.wqe_count = 0;
		break;

	case IBV_EXP_WR_CQE_WAIT:
		wr.task.cqe_wait.cq = ctx->cq;
		wr.task.cqe_wait.cq_count = 1;
		break;

	default:
		fprintf(stderr, "-E- unsupported m_wqe opcode %d\n", op);
		return -1;
	}

	ret = ibv_exp_post_send(ctx->mqp, &wr, &bad_wr);
	if (!ret) {
		wmb();
	}
	return ret;
}
#endif /* CALC_SUPPORT */



int server_pre_post_wqes(struct pingpong_context *ctx, int iters,struct pingpong_dest **rem_dest, int num_of_nodes) {
	int ret = 0;
	int i;
	struct ibv_exp_send_wr *bad_wr;
	struct ibv_exp_send_wr wr;
	struct ibv_sge     		list;
	int j;



	for (i = 0; i < iters; i++) {

		for (j=0; j<num_of_nodes; j++){

			memset(&wr, 0, sizeof(wr));

			list.addr   = (uintptr_t)ctx->net_buf[j];
			list.length = user_param.size;
			list.lkey   = ctx->mr[j]->lkey;


			wr.sg_list = NULL;
			wr.num_sge = 0;
			wr.exp_send_flags = 0;
			wr.wr_id = PP_CQE_WAIT; //todo - check
			wr.exp_opcode = IBV_EXP_WR_CQE_WAIT;
			wr.next = NULL;
			wr.task.cqe_wait.cq = ctx->cq;
			wr.task.cqe_wait.cq_count = num_of_nodes;
			if (i == (iters - 1))
				wr.exp_send_flags |= IBV_EXP_SEND_SIGNALED;

			if (j == (num_of_nodes - 1))
				wr.exp_send_flags |= IBV_EXP_SEND_WAIT_EN_LAST;


			ret = ibv_exp_post_send(ctx->qp[j], &wr, &bad_wr);
				if (ret) {
					fprintf(stderr, "-E- ibv_post_send failed \n");
					return -1;
				}
		}

		for (j=0; j<num_of_nodes; j++){

			memset(&wr, 0, sizeof(wr));

			list.addr   = (uintptr_t)ctx->net_buf[j];
			list.length = user_param.size;
			list.lkey   = ctx->mr[j]->lkey;

			wr.exp_send_flags &= ~IBV_SEND_SIGNALED;
			wr.sg_list = &list;
			wr.num_sge = 1;
			wr.wr_id = PP_SEND_WRID; //todo - check
#if defined(USE_RDMA)
			wr.exp_opcode = IBV_EXP_WR_RDMA_WRITE_WITH_IMM;
			wr.wr.rdma.remote_addr = rem_dest[j]->vaddr;
			wr.wr.rdma.rkey = rem_dest[j]->rkey;
#else
			wr.exp_opcode = IBV_EXP_WR_SEND;
#endif  /* USE_RDMA */

			wr.exp_send_flags |= IBV_EXP_SEND_INLINE;
			wr.next = NULL;

			if (i == (iters - 1))
						wr.exp_send_flags |= IBV_EXP_SEND_SIGNALED;

			ret = ibv_exp_post_send(ctx->qp[j], &wr, &bad_wr);
				if (ret) {
					fprintf(stderr, "-E- ibv_post_send failed \n");
					return -1;
				}

		}
	}

	return ret;
}


#if defined(CALC_SUPPORT)


int pp_poll_mcq(struct ibv_cq *cq, int num_cqe) {
	int ne;
	int i;
	struct ibv_wc wc[2];

	if (num_cqe > 2) {
		fprintf(stderr, "-E- max num cqe exceeded\n");
		return -1;
	}

	do {
		printf("i polling on mcq \n"); /*check*/
		ne = ibv_poll_cq(cq, num_cqe, wc);
		if (ne < 0) {
			fprintf(stderr, "poll CQ failed %d\n", ne);
			return 1;
		}
	} while (ne < 1);

	for (i = 0; i < ne; ++i) {
		if (wc[i].status != IBV_WC_SUCCESS) {
			fprintf(stderr, "Failed %s status %s (%d)\n",
					wr_id_str[(int) wc[i].wr_id],
					ibv_wc_status_str(wc[i].status), wc[i].status);
			return 1;
		}

		if ((int) wc[i].wr_id != PP_CQE_WAIT) {
			fprintf(stderr, "invalid wr_id %lx\n", wc[i].wr_id);
			return -1;
		}
	}

	return 0;
}

static int calc_verify(struct pingpong_context *ctx,
		enum ibv_m_wr_data_type calc_data_type,
		enum ibv_m_wr_calc_op calc_opcode) {
	uint64_t *op1 = &(ctx->last_result);
	uint64_t *op2 = (uint64_t *) ctx->buf_for_calc_operands + 2;
	uint64_t *res = (uint64_t *) ctx->buf_for_calc_operands;

	return !EXEC_VERIFY(calc_data_type, calc_opcode, 1, op1, op2, res);
}

static int update_last_result(struct pingpong_context *ctx,
		enum ibv_m_wr_data_type calc_data_type,
		enum ibv_m_wr_calc_op calc_opcode) {
	/* EXEC_VERIFY derefence result parameter */
	uint64_t dummy;

	uint64_t *op1 = (uint64_t *) ctx->buf_for_calc_operands;
	uint64_t *op2 = (uint64_t *) ctx->buf_for_calc_operands + 2;
	uint64_t res =
			(uint64_t)EXEC_VERIFY(calc_data_type, calc_opcode, 0, op1, op2, &dummy);

	ctx->last_result = res;
	return 0;
}
#endif /* CALC_SUPPORT */

static void usage(const char *argv0) {
	printf("Usage:\n");
	printf("  %s				start a server and wait for connection\n", argv0);
	printf("  %s <host>	 		connect to server at <host>\n", argv0);
	printf("\n");
	printf("Options:\n");
	printf(
			"  -p, --port=<port>		listen on/connect to port <port> (default 18515)\n");
	printf(
			"  -d, --ib-dev=<dev>		use IB device <dev> (default first device found)\n");
	printf(
			"  -i, --ib-port=<port>		use port <port> of IB device (default 1)\n");
	printf("  -s, --size=<size>		size of message to exchange (default 4096)\n");
	printf("  -m, --mtu=<size>		path MTU (default 1024)\n");
	printf(
			"  -r, --rx-depth=<dep>		number of receives to post at a time (default 500)\n");
	printf("  -n, --iters=<iters>		number of exchanges (default 1000)\n");
	printf("  -l, --sl=<sl>			service level value\n");
	printf("  -e, --events			sleep on CQ events (default poll)\n");
#if defined(CALC_SUPPORT)
	printf("  -c, --calc=<operation>	calc operation\n");
	printf("  -t, --op_type=<type>		calc operands type\n");
	printf("  -o, --operands=<o1,o2,...>  	comma separeted list of operands\n");
	printf("  -w, --wait_cq=cqn		wait for entries on cq\n");
	printf("  -v, --verbose			print verbose information\n");
	printf("  -V, --verify			verify calc operations\n");
#endif /* CALC_SUPPORT */
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
///----------------------------------sassha additions--------------------------------------------////

static void init_perftest_params(struct perftest_parameters *user_param) { ///todo set client and server!! sashas

	user_param->port = DEF_PORT;
	user_param->ib_port = DEF_IB_PORT;
	user_param->size = user_param->tst == BW ? DEF_SIZE_BW : DEF_SIZE_LAT;
	user_param->tx_depth = user_param->tst == BW ? DEF_TX_BW : DEF_TX_LAT;
	user_param->qp_timeout = DEF_QP_TIME;
	user_param->all = OFF;
	user_param->cpu_freq_f = OFF;
	user_param->connection_type = RC;
	user_param->use_event = OFF;
	user_param->num_of_qps = DEF_NUM_QPS;
	user_param->gid_index = DEF_GID_INDEX;
	user_param->inline_size =
			user_param->tst == BW ? DEF_INLINE_BW : DEF_INLINE_LT;
	user_param->use_mcg = OFF;
	user_param->use_rdma_cm = ON;  ////sasha was OFF
	user_param->work_rdma_cm = OFF;
	user_param->rx_depth = user_param->verb == SEND ? DEF_RX_SEND : DEF_RX_RDMA;
	user_param->duplex = OFF;
	user_param->noPeak = OFF;
	user_param->cq_mod = DEF_CQ_MOD;
	user_param->tos = DEF_TOS;

	user_param->test_type = ITERATIONS;
	user_param->duration = DEF_DURATION;
	user_param->margin = DEF_MARGIN;

	user_param->iters =
			(user_param->tst == BW && user_param->verb == WRITE) ?
					DEF_ITERS_WB : DEF_ITERS;
	user_param->calc_first_byte_latency = OFF;
#if defined(CALC_SUPPORT)

	/*
	 * core direct additions:
	 */
	user_param->verbose = 0;
	user_param->verify = 0;
	user_param->calc_data_type = IBV_M_DATA_TYPE_INVALID;
	user_param->calc_operands_str = NULL;
	user_param->calc_opcode = IBV_M_CALC_OP_INVALID;
	user_param->mqe_poll = 0;
#endif /* CALC_SUPPORT */

	if (user_param->tst == LAT) {
		user_param->r_flag->unsorted = OFF;
		user_param->r_flag->histogram = OFF;
		user_param->r_flag->cycles = OFF;
	}

}


int create_rdma_resources(struct pingpong_context *ctx,
		struct perftest_parameters *user_param) {

	enum rdma_port_space port_space;

	ctx->cm_channel = rdma_create_event_channel();
	if (ctx->cm_channel == NULL) {
		fprintf(stderr, " rdma_create_event_channel failed\n");
		return FAILURE;
	}

	switch (user_param->connection_type) {

	case RC:
		port_space = RDMA_PS_TCP;
		break;
	case UD:
		port_space = RDMA_PS_UDP;
		break;
	default:
		port_space = RDMA_PS_TCP;
	}

	if (user_param->machine == CLIENT) {

		if (rdma_create_id(ctx->cm_channel, &ctx->cm_id, NULL, RDMA_PS_TCP)) {
			fprintf(stderr, "rdma_create_id failed\n");
			return FAILURE;
		}

	} else {

		if (rdma_create_id(ctx->cm_channel, &ctx->cm_id_control, NULL,
				RDMA_PS_TCP)) {
			fprintf(stderr, "rdma_create_id failed\n");
			return FAILURE;
		}

	}

	return SUCCESS;
}


/******************************************************************************
 *
 ******************************************************************************/
int create_comm_struct(struct perftest_comm *comm,
		struct perftest_parameters *user_param) {

	ALLOCATE(comm->rdma_params, struct perftest_parameters, 1);
	memset(comm->rdma_params, 0, sizeof(struct perftest_parameters));

	comm->rdma_params->port = user_param->port;
	comm->rdma_params->sockfd = -1;
	comm->rdma_params->gid_index = user_param->gid_index;
	comm->rdma_params->use_rdma_cm = user_param->use_rdma_cm;
	comm->rdma_params->servername = user_param->servername;
	comm->rdma_params->machine = user_param->machine;
	comm->rdma_params->side = LOCAL;
	comm->rdma_params->verb = user_param->verb;
	comm->rdma_params->use_mcg = user_param->use_mcg;
	comm->rdma_params->duplex = user_param->duplex;
	comm->rdma_params->tos = DEF_TOS;

	if (user_param->use_rdma_cm) {

		ALLOCATE(comm->rdma_ctx, struct pingpong_context, 1);
		memset(comm->rdma_ctx, 0, sizeof(struct pingpong_context));

		comm->rdma_params->tx_depth = 1;
		comm->rdma_params->rx_depth = 1;
		comm->rdma_params->cq_size = 1;
		comm->rdma_params->connection_type = RC;
		comm->rdma_params->num_of_qps = 1;
		comm->rdma_params->size = sizeof(struct pingpong_dest);
		comm->rdma_ctx->context = NULL;


		if (create_rdma_resources(comm->rdma_ctx, comm->rdma_params)) { ////	unmark this!!
			fprintf(stderr,
					" Unable to create the resources needed by comm struct\n");
			return FAILURE;
		}
	}

	return SUCCESS;
}


/******************************************************************************
 *
 ******************************************************************************/



int check_add_port(char **service, int port, const char *servername,
		struct addrinfo *hints, struct addrinfo **res) {

	int number;
	if (asprintf(service, "%d", port) < 0)
		return FAILURE;
	number = getaddrinfo(servername, *service, hints, res);

	if (number < 0) {
		fprintf(stderr, "%s for %s:%d\n", gai_strerror(number), servername,
				port);
		return FAILURE;
	}
	return SUCCESS;
}

/*
 * parser: get all user parameters and assign to the user_param global struct
 */
int parser(struct perftest_parameters *user_param, int argc, char *argv[]) {

	int port = 18515;
	int ib_port = 1;
	int size = 4096;

	enum ibv_mtu mtu = IBV_MTU_1024;
	int rx_depth = 8000; /*oferh: was 500*/
	//int tx_depth = 16000;
	int iters = 1000;
	int use_event = 0;
	//int rcnt, scnt;
	int sl = 0;
	//int mqe_poll = 0, mqe_qp_enable = 0;
	int verbose = 0;
	int verify = 0;

	char *ib_devname = NULL;

#if defined(CALC_SUPPORT)
	enum ibv_m_wr_data_type calc_data_type = IBV_M_DATA_TYPE_INVALID;
	enum ibv_m_wr_calc_op calc_opcode      = IBV_M_CALC_OP_INVALID;
	char *calc_operands_str = NULL;
#endif /* CALC_SUPPORT */


	srand48(getpid() * time(NULL));

	page_size = sysconf(_SC_PAGESIZE);

	init_perftest_params(user_param);

	while (1) {
		int c;

		static struct option long_options[] = {
				{ .name = "port",     .has_arg = 1, .val = 'p' },
				{ .name = "ib-dev",   .has_arg = 1, .val = 'd' },
				{ .name = "ib-port",  .has_arg = 1, .val = 'i' },
				{ .name = "size",     .has_arg = 1, .val = 's' },
				{ .name = "mtu",      .has_arg = 1, .val = 'm' },
				{ .name = "rx-depth", .has_arg = 1, .val = 'r' },
				{ .name = "iters",    .has_arg = 1, .val = 'n' },
				{ .name = "sl",       .has_arg = 1, .val = 'l' },
				{ .name = "events",   .has_arg = 0, .val = 'e' },
				{ .name = "calc",     .has_arg = 1, .val = 'c' },
				{ .name = "op_type",  .has_arg = 1, .val = 't' },
				{ .name = "operands", .has_arg = 1, .val = 'o' },
				{ .name = "poll_mqe", .has_arg = 0, .val = 'w' },
				{ .name = "verbose",  .has_arg = 0, .val = 'v' },
				{ .name = "verify",   .has_arg = 0, .val = 'V' },
				{ 0 } };

		c = getopt_long(argc, argv, "p:d:i:s:m:r:n:l:et:c:o:wfvV", long_options, NULL);
		if (c == -1)
			break;

		switch (c) {
		case 'p':
			port = strtol(optarg, NULL, 0);
			if (port < 0 || port > 65535) {
				usage(argv[0]);
				return 1;
			}
			break;

		case 'd':
			ib_devname = (char *) malloc(
					sizeof(char) * strlen(strdupa(optarg)) + 1);
			strcpy(ib_devname, strdupa(optarg));
			break;

		case 'i':
			ib_port = strtol(optarg, NULL, 0);
			if (ib_port < 0) {
				usage(argv[0]);
				return 1;
			}
			break;

		case 's':
			size = strtol(optarg, NULL, 0);
			break;

		case 'm':
			mtu = pp_mtu_to_enum(strtol(optarg, NULL, 0));
			if (mtu < 0) {
				usage(argv[0]);
				return 1;
			}
			break;

		case 'r':
			rx_depth = strtol(optarg, NULL, 0);
			break;

		case 'n':
			iters = strtol(optarg, NULL, 0);

			if (iters * 2 > rx_depth) {
				fprintf(stderr, "iters*2 > rx_depth");
				return -1;
			}
			break;

		case 'l':
			sl = strtol(optarg, NULL, 0);
			break;

		case 'v':
			verbose = 1;
			break;

		case 'V':
			verify = 1;
			break;

		case 'e':
			++use_event;
			break;
#if defined(CALC_SUPPORT)

		case 't':
			calc_data_type = ibv_m_str_to_data_type(optarg);
			if (calc_data_type == IBV_M_DATA_TYPE_INVALID) {
				printf("-E- invalid data types. Valid values are:\n");
				ibv_m_print_data_type();
				return 1;
			}
			break;

		case 'o':
			calc_operands_str = (char *) malloc(sizeof(char) * strlen(strdupa(optarg)) + 1);
						strcpy(calc_operands_str, strdupa(optarg));
			break;

		case 'c':
			calc_opcode = ibv_m_str_to_calc_op(optarg);
			if (calc_opcode == IBV_M_CALC_OP_INVALID) {
				printf("-E- invalid data types. Valid values are:\n");
				ibv_m_print_calc_op();
				return 1;
			}
			break;

		case 'w':
			mqe_poll = 1;
			break;
#endif /* CALC_SUPPORT */

		default:
			usage(argv[0]);
			return 1;
		}
	}
#if defined(CALC_SUPPORT)

	/* calc and data type are mandatory */
	if (calc_opcode == IBV_M_CALC_OP_INVALID
			|| calc_data_type == IBV_M_DATA_TYPE_INVALID) { /*sasha*/ //we are not useing this
			/*sasha*/
		calc_opcode = IBV_M_CALC_OP_ADD;
	}
#endif /* CALC_SUPPORT */


	/*
	 * assign to user_param:
	 */
	user_param->port = port;
	user_param->ib_devname = ib_devname;
	user_param->ib_port = ib_port;
	user_param->size = size;
	user_param->mtu = mtu;
	user_param->rx_depth = rx_depth;
	user_param->iters = iters;
	user_param->tx_depth=2*iters;
	user_param->sl = sl;
	user_param->use_event = use_event;
#if defined(CALC_SUPPORT)
	user_param->verbose = verbose;
	user_param->verify = verbose;
	user_param->calc_data_type = calc_data_type;
	user_param->calc_operands_str = calc_operands_str;
	user_param->calc_opcode = calc_opcode;
	user_param->mqe_poll = mqe_poll;
#endif /* CALC_SUPPORT */

	return SUCCESS;
}



/******************************************************************************
 *
 ******************************************************************************/

int main(int argc, char *argv[]) {
	struct ibv_device **dev_list;
	struct ibv_device *ib_dev = NULL;
	struct pingpong_context *ctx;
	struct pingpong_dest my_dest[MAX_NODE_NUM];
	struct pingpong_dest **rem_dest;
	struct timeval start, end;

	int routs;
	int num_cq_events = 0;
	int comp_count;

	char msg[KEY_MSG_SIZE];
	int connfd[MAX_NODE_NUM];
	int sockfd = 0;

	struct ibv_wc wc[2];
	int ne, i;

	int ret;
	union ibv_gid temp_gid;
	int j;

	struct report_options report = { };
	struct perftest_comm user_comm; ///todo
	char *servername = NULL;

	ctx = malloc(sizeof *ctx);		///sasha  was in pp_init_ctx!!
	if (!ctx)
		return 0;
	memset(ctx, 0, sizeof *ctx);

	/* init default values to user's parameters */
	memset(&user_param, 0, sizeof(struct perftest_parameters)); //sasha, do I need this?? define global in write.lat
	memset(&user_comm, 0, sizeof(struct perftest_comm));
	memset(&my_dest, 0, sizeof(struct pingpong_dest));

	user_param.verb = WRITE;
	user_param.tst = LAT;
	user_param.r_flag = &report;
	user_param.version = 0; // VERSION;
	user_param.num_of_nodes=4; //todo

	/*
	 * parse input arguments:
	 */
	if (parser(&user_param, argc, argv)) {
		fprintf(stderr, " Parser function exited with Error\n");
		return FAILURE;
	}

	if (optind == argc - 1) //sasha
		user_param.machine = CLIENT;
	else
		user_param.machine = SERVER;

	if (optind == argc - 1) {
		servername = strdupa(argv[optind]);
	} else if (optind < argc) {
		usage(argv[0]);
		return 1;
	}
	user_param.servername = servername;

	/*
	 * connect to the IB device:
	 */
	dev_list = ibv_get_device_list(NULL);
	if (!dev_list) {
		fprintf(stderr, "No IB devices found\n");
		return 1;
	}

	if (user_param.ib_devname) {
		int i;

		for (i = 0; dev_list[i]; ++i) {
			if (!strcmp(ibv_get_device_name(dev_list[i]),
					user_param.ib_devname)) {
				ib_dev = dev_list[i];
				break;
			}
		}
		if (!ib_dev) {
			fprintf(stderr, "IB device %s not found\n", user_param.ib_devname);
			return 1;
		}
	} else
		ib_dev = *dev_list;

	// copy the rellevant user parameters to the comm struct + creating rdma_cm resources.
	if (create_comm_struct(&user_comm, &user_param)) {
		fprintf(stderr, " Unable to create RDMA_CM resources\n");
		return 1;
	}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////



#if defined(CALC_SUPPORT)

		user_param.calc_operands_str="1,2";  // TODO  delite this.   /

		if (user_param.servername)		/*client*/
		{
			ret = pp_init_ctx(ctx,ib_dev, user_param.size, user_param.tx_depth,
					user_param.rx_depth, user_param.ib_port, user_param.use_event,
					user_param.calc_opcode, user_param.calc_data_type,
					user_param.calc_operands_str,user_param.verb);
			if (ret)
				return 1;
		}
		else
		{
			ret = pp_init_ctx_server(ctx,ib_dev, user_param.size, user_param.tx_depth,
								user_param.rx_depth, user_param.ib_port, user_param.use_event,
								user_param.calc_opcode, user_param.calc_data_type,
								user_param.calc_operands_str,user_param.verb);
						if (ret)
							return 1;

		}



	if (user_param.servername) /*client*/
		update_last_result(ctx, user_param.calc_data_type,
				user_param.calc_opcode);
	else
		ctx->last_result = *(uint64_t *) ctx->buf_for_calc_operands;
#else
		if (user_param.servername)		/*client*/
		{
			ret = pp_init_ctx(ctx,ib_dev, user_param.size, user_param.tx_depth,
					user_param.rx_depth, user_param.ib_port, user_param.use_event,
					user_param.verb);
			if (ret)
				return 1;
		}
		else
		{
			ret = pp_init_ctx_server(ctx,ib_dev, user_param.size, user_param.tx_depth,
								user_param.rx_depth, user_param.ib_port, user_param.use_event,
								user_param.verb);
						if (ret)
							return 1;

		}
#endif /* CALC_SUPPORT */



	///////////////server and vlient post recieve/////////////////////////////////

	routs = pp_post_recv(ctx, ctx->rx_depth);
	if (user_param.servername)		//client
	{
		if (routs < ctx->rx_depth) {
			fprintf(stderr, "Couldn't post receive (%d)\n", routs);
			return 1;
		}
	}
	else
	{
		if (routs < (ctx->rx_depth)*user_param.num_of_nodes) {
			fprintf(stderr, "Couldn't post receive (%d) for every node\n", routs);
			return 1;
		}
	}


	if (user_param.use_event)
		if (ibv_req_notify_cq(ctx->cq, 0)) {
			fprintf(stderr, "Couldn't request CQ notification\n");
			return 1;
		}

	if (user_param.gid_index != -1) {
		if (ibv_query_gid(ctx->context,user_param.ib_port,user_param.gid_index,&temp_gid)) {
			return -1;
		}
	}


	my_dest[0].lid = pp_get_local_lid(ctx->context, user_param.ib_port);

	my_dest[0].qpn = ctx->qp[0]->qp_num;
	my_dest[0].psn = lrand48() & 0xffffff;

	// Each qp gives his receive buffer address .


	my_dest[0].vaddr = (uintptr_t)ctx->net_buf[0];// + (user_param.num_of_qps + 0)*BUFF_SIZE(ctx->size);
	my_dest[0].rkey= ctx->mr[0]->rkey;
	my_dest[0].out_reads = user_param.out_reads;
	memcpy(my_dest[0].gid.raw,temp_gid.raw ,16);

	if (!my_dest[0].lid) {
		fprintf(stderr, "Couldn't get local LID\n");
		return 1;
	}


	/*
	 * server need more dests.
	 */

	if (!user_param.servername) //server
	{
		for(j=1; j<user_param.num_of_nodes; j++)
		{
			my_dest[j].lid = pp_get_local_lid(ctx->context, user_param.ib_port);
			my_dest[j].qpn = ctx->qp[j]->qp_num;
			my_dest[j].psn = lrand48() & 0xffffff;

			// Each qp gives his receive buffer address .

			my_dest[j].vaddr = (uintptr_t)ctx->net_buf[j];// + (user_param.num_of_qps + i)*BUFF_SIZE(ctx->size); ///  i --> ?
			my_dest[j].rkey= ctx->mr[j]->rkey;
			my_dest[j].out_reads = user_param.out_reads;
			memcpy(my_dest[j].gid.raw,temp_gid.raw ,16);

			if (!my_dest[j].lid) {
				fprintf(stderr, "Couldn't get local LID\n");
				return 1;
			}
		}
	}




	if (!user_param.servername)  //server
		for(j=0; j<user_param.num_of_nodes; j++)
			printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, Rkey 0x%08x, Vadrr 0x%016llx\n",
					my_dest[j].lid, my_dest[j].qpn, my_dest[j].psn, my_dest[j].rkey, my_dest[j].vaddr);
	else
			printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, Rkey 0x%08x,Vadrr 0x%016llx\n",
							my_dest[0].lid, my_dest[0].qpn, my_dest[0].psn,my_dest[0].rkey, my_dest[0].vaddr);



	//////////////exvhange information between client and server//////////
	if (user_param.servername) // client
		rem_dest = pp_client_exch_dest(user_param.servername, user_param.port,
				&my_dest[0], &sockfd);
	else
		rem_dest = pp_server_exch_dest(ctx, user_param.ib_port, user_param.mtu,	user_param.port, user_param.sl, my_dest, user_param.num_of_nodes, connfd);

	if (!rem_dest)
		return 1;


	if (!user_param.servername)	//server
		for(j=0; j<user_param.num_of_nodes; j++)
			printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, Rkey 0x%08x,Vadrr 0x%016llx\n",
					rem_dest[j]->lid, rem_dest[j]->qpn, rem_dest[j]->psn,rem_dest[j]->rkey, rem_dest[j]->vaddr);
	else
		printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, Rkey 0x%08x,Vadrr 0x%016llx\n",
							rem_dest[0]->lid, rem_dest[0]->qpn, rem_dest[0]->psn,rem_dest[0]->rkey, rem_dest[0]->vaddr);




	if (user_param.servername) 		//client
		if (pp_connect_ctx(ctx, ctx->qp[0], user_param.ib_port, my_dest[0].psn,
				user_param.mtu, user_param.sl, rem_dest[0]))
			return 1;

	ctx->pending = PP_RECV_WRID;

	/************************** DONE INIT, START THE TEST **************************************/
	// server: produce list of WR: WAIT-SEND-WAIT-SEND on each qp-...
	if (!user_param.servername) /*server*/{
		int nodeind;

		if (server_pre_post_wqes(ctx, user_param.iters, rem_dest, user_param.num_of_nodes)) {
			fprintf(stderr, "Failed pre posting WQEs\n");
			return -1;
		}
		messaged("server: done pre-posting\n");


		 //send message to all the client that the prepost is done
		for (nodeind=0 ; nodeind< user_param.num_of_nodes; nodeind++) {
				write(connfd[nodeind], "ready", sizeof "ready");
				close(connfd[nodeind]);
		}
		messaged("server: done client notification\n");

	} else {
		//client waits until server is done preposting!
		read(sockfd, msg, sizeof msg);
		close(sockfd);
		messaged("client: got notification from server\n");
	}


	if (gettimeofday(&start, NULL)) {
		perror("gettimeofday");
		return 1;
	}


	// main loop client: post write request, and poll on completion:
	if (!user_param.servername) {
		int ne_count = 0;

		//server poll until it gets all messages are received
		do {
			fflush(stdout);
			ne = ibv_poll_cq(ctx->cq, 1, wc);
			if (ne < 0) {
				fprintf(stderr, "poll CQ failed %d\n", ne);
				return 1;
			}
			if (ne > 0 ) { 
				if  (wc->status != IBV_WC_SUCCESS) {
					fprintf(stderr, "poll CQ (%d) with wr_id=%lu returned with an error 0x%x : 0x%x - %s\n",
						ne_count, wc->wr_id, wc->status, wc->vendor_err, ibv_wc_status_str(wc->status));
					return 1;
				}
			}			
			ne_count = ne_count + ne;
		} while (ne_count < (user_param.num_of_nodes*user_param.iters + user_param.num_of_nodes)); // TODO DEBUG 2 --> 1

		messaged("server: got %d completions\n", ne_count);
		fflush(stdout);
	} else {
		int ne_count = 0;

		for (i = 0; i < user_param.iters; i++) {

			if (pp_post_send(ctx,rem_dest[0])) {
				fprintf(stderr, "Couldn't post send\n");
				return 1;
			}

			do {
				comp_count = ibv_poll_cq(ctx->cq, 1, wc);

				if (comp_count < 0) {
					fprintf(stderr, "poll CQ failed %d\n", comp_count);
					return 1;
				}
				if (comp_count > 0 ) { 
					if  (wc->status != IBV_WC_SUCCESS) {
						fprintf(stderr, "poll CQ (%d) with wr_id=%lu returned with an error 0x%x : 0x%x - %s\n",
							ne_count, wc->wr_id, wc->status, wc->vendor_err, ibv_wc_status_str(wc->status));
						return 1;
					}
				}			
			} while (comp_count < 1);
			ne_count = ne_count + comp_count;
		}

	}



	if (gettimeofday(&end, NULL)) {
		perror("gettimeofday");
		return 1;
	}

	//print the result
	{
		float usec = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
		long long bytes = (long long) user_param.size * user_param.iters * 2;

		printf("%lld bytes in %.2f seconds = %.2f Mbit/sec\n", bytes, usec / 1000000., bytes * 8. / usec);
		printf("%d iters in %.2f seconds = %.2f usec/iter\n", user_param.iters, usec / 1000000., usec / user_param.iters);

		printf("\033[0;3%sm" "%s" "\033[m\n\n", "4", ">>>>LAUNCHED ON CORE-DIRECT API");

	}

	ibv_ack_cq_events(ctx->cq, num_cq_events);

	fflush(stdout);


	if (pp_close_ctx(ctx))
		return 1;

	ibv_free_device_list(dev_list);

	return 0;
}




