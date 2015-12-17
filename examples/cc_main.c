/*
 * Copyright (c) 2013 Mellanox Technologies.  All rights reserved.
 */

#define _GNU_SOURCE

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
#include <sys/resource.h>

#include <infiniband/arch.h>
#include <infiniband/verbs.h>
#include <infiniband/verbs_exp.h>

#include <infiniband/umad.h>
#include "config.h"

/* MPI should be used for this version */
#ifdef HAVE_MPICC
#define USE_MPI		1
#endif

#if defined(USE_MPI)
#include <mpi.h>
#endif


#define MODULE_VERSION 		0.1
#define MODULE_NAME 		"ibv_cc_demo"
#define MODULE_COPYRIGHT	\
		"Copyright (C) 2013 Mellanox Technologies Ltd." \
		"\nThis example is based on Infiniband VERBS API," \
		"\nand demonstrates usage of Cross-Channel operations" \
		"\non different collective algorithms" \
		"\nsee http://www.mellanox.com and http://www.openfabrics.org"

#define QUOTE(name) #name
#define STR(macro) QUOTE(macro)


struct cc_alg_info {
	const char             *name;	/* Algorithm name */
	const char             *short_name;	/* for matching with command line '--test' */
	const char             *note;	/* Algorithm note */
	int (*setup)(void *);
	int (*close)(void *);
	int (*proc)(void *);
	int (*check)(void *);
};

struct cc_conf {
	int                     my_proc;
	int                     num_proc;
	int                     iters;
	int                     check;
	int                     warmup;
	int                     cq_tx_depth;
	int                     cq_rx_depth;
	int                     qp_tx_depth;
        int                     qp_rx_depth;
        int                     use_mq;
        int                     oob_barrier;
	int                     size;
	struct cc_alg_info     *algorithm;
};

#pragma pack( push, 1 )
struct cc_proc_info {
	int                     lid;	/* LID of the IB port */
	int                     qpn;	/* QP number */
	int                     psn;
	uint32_t                rkey;	/* Remote key */
	uintptr_t               vaddr;	/* Buffer address */
	union ibv_gid           gid;	/* GID of the IB port */
};
#pragma pack( pop )

struct cc_proc {
	struct ibv_qp          *qp;
	struct ibv_cq          *scq;
	struct ibv_cq          *rcq;
        struct cc_proc_info     info;
    int wait_count;
    int credits;
};

struct cc_context {
	struct ibv_context     *ib_ctx;
	int                     ib_port;

	struct ibv_pd          *pd;
	struct ibv_mr          *mr;

	struct ibv_qp          *mqp;
	struct ibv_cq          *mcq;

	void                   *buf;

	struct cc_conf          conf;

	struct cc_proc         *proc_array;
};

enum {
	CC_LOG_FATAL		= 0,
	CC_LOG_INFO,
	CC_LOG_TRACE
};

static uint32_t __debug_mask = 1;
static int  __my_proc = 0;

#define log_fatal(fmt, ...)  \
	do {                                                           \
		if (__my_proc == 0 && __debug_mask >= 0) {                 \
			fprintf(stderr, "\033[0;3%sm" "[    FATAL ] #%d: " fmt "\033[m", "1", __my_proc, ##__VA_ARGS__);    \
			exit(errno);    \
		}    \
	} while(0)

#define log_info(fmt, ...)  \
	do {                                                           \
		if (__my_proc == 0 && __debug_mask >= CC_LOG_INFO)                 \
			fprintf(stderr, "\033[0;3%sm" "[     INFO ] #%d: " fmt "\033[m", "4", __my_proc, ##__VA_ARGS__);    \
	} while(0)

#define log_trace(fmt, ...) \
	do {                                                           \
		if (__my_proc == 0 && __debug_mask >= CC_LOG_TRACE)                 \
			fprintf(stderr, "\033[0;3%sm" "[    TRACE ] #%d: " fmt "\033[m", "7", __my_proc, ##__VA_ARGS__);    \
	} while(0)


struct cc_timer{
	double wall;
	double cpus;
};

static inline void __sleep(unsigned secs)
{
	struct timespec req, rem;
	int err;

	req.tv_sec  = secs;
	req.tv_nsec = 0;

	do {
		err = nanosleep(&req, &rem);
		if (err == 0 || errno != EINTR)
		break;
		memcpy(&req, &rem, sizeof(req));
	} while(1);
}

static inline void __timer(struct cc_timer *t)
{
	struct timeval tp;
	struct rusage ru;

	gettimeofday(&tp, NULL);
	t->wall = tp.tv_sec * (double)1.0e6 + tp.tv_usec;

	getrusage(RUSAGE_SELF, &ru);
	t->cpus = (ru.ru_utime.tv_sec  + ru.ru_stime.tv_sec) * (double)1.0e6 +
			(ru.ru_utime.tv_usec + ru.ru_stime.tv_usec);
}

static inline int __log2_cc(long val)
{
	int count = 0;

	while(val > 0) {
		val = val >> 1;
		count++;
	}

	return (count > 0 ? (count - 1) : 0);
}

static uint16_t __get_local_lid(struct ibv_context *context, int port)
{
	struct ibv_port_attr attr;

	if (ibv_query_port(context, port, &attr))
		return 0;

	return attr.lid;
}


static int __post_read(struct cc_context *ctx, struct ibv_qp *qp, int count)
{
	int rc = 0;
	struct ibv_sge list;
	struct ibv_recv_wr wr;
	struct ibv_recv_wr *bad_wr;
	int i = 0;
        /* prepare the scatter/gather entry */
	memset(&list, 0, sizeof(list));
	list.addr = (uintptr_t)ctx->buf;
        list.length = ctx->conf.size;
        list.lkey = ctx->mr->lkey;

	/* prepare the send work request */
	memset(&wr, 0, sizeof(wr));
	wr.wr_id = 0;
	wr.next = NULL;
	wr.sg_list = &list;
	wr.num_sge = 1;

	for (i = 0; i < count; i++)
		if ((rc = ibv_post_recv(qp, &wr, &bad_wr)) != 0) {
			break;
		}
        return i;
}

static int __repost(struct cc_context *ctx, struct ibv_qp *qp, int count, int peer) {
    int posted = __post_read(ctx,qp,count);
    ctx->proc_array[peer].credits += posted;
    return posted;
}

#if defined(USE_MPI)
static int __exchange_info_by_mpi(struct cc_context *ctx,
				  struct cc_proc_info *my_info,
				  struct cc_proc_info *peer_info,
				  int peer_proc)
{
	MPI_Status *status = NULL;

	MPI_Sendrecv(my_info, sizeof(*my_info), MPI_CHAR,
		     peer_proc, 0,
		     peer_info, sizeof(*peer_info), MPI_CHAR,
		     peer_proc, 0,
		     MPI_COMM_WORLD, status);

	return 0;
}
#endif

static int __init_ctx( struct cc_context *ctx )
{
	int rc = 0;
	int i = 0;

	/*
	 * 1. Setup buffer
	 * =======================================
	 */
	log_trace("setup buffer ...\n");
	/*
	 * A Protection Domain (PD) allows the user to restrict which components can interact
	 * with only each other. These components can be AH, QP, MR, and SRQ
	 */
	ctx->pd = ibv_alloc_pd(ctx->ib_ctx);
	if (!ctx->pd)
		log_fatal("ibv_alloc_pd failed\n");

	/*
	 * VPI only works with registered memory. Any memory buffer which is valid in
	 * the process's virtual space can be registered. During the registration process
	 * the user sets memory permissions and receives a local and remote key
	 * (lkey/rkey) which will later be used to refer to this memory buffer
	 */
	ctx->buf = memalign(sysconf(_SC_PAGESIZE), ctx->conf.size * (ctx->conf.num_proc + 1));
	if (!ctx->buf)
		log_fatal("memalign failed\n");
	memset(ctx->buf, 0, ctx->conf.size * (ctx->conf.num_proc + 1));

        ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, ctx->conf.size,
                             IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
	if (!ctx->mr)
		log_fatal("ibv_reg_mr failed (buf=%p size=%d)\n", ctx->buf, ctx->conf.size);

        if (ctx->conf.use_mq) {
            /*
             * 2. Create Manage QP
             * =======================================
             */
            log_trace("create manage QP ...\n");
            // Note: mcq was previously created with 0x10 entries. It must be larger
            ctx->mcq = ibv_create_cq(ctx->ib_ctx, 0x1000, NULL, NULL, 0);
            if (!ctx->mcq)
                log_fatal("ibv_create_cq failed\n");

            {
                struct ibv_exp_qp_init_attr init_attr;
                struct ibv_qp_attr attr;

                memset(&init_attr, 0, sizeof(init_attr));

                init_attr.qp_context = NULL;
                init_attr.send_cq = ctx->mcq;
                init_attr.recv_cq = ctx->mcq;
                init_attr.srq = NULL;
                init_attr.cap.max_send_wr  = 0x0400; // Default 0x40 was insufficient
                init_attr.cap.max_recv_wr  = 0;
                init_attr.cap.max_send_sge = 16;
                init_attr.cap.max_recv_sge = 16;
                init_attr.cap.max_inline_data = 0;
                init_attr.qp_type = IBV_QPT_RC;
                init_attr.sq_sig_all = 0;  // so that we can poll for a single completion for the entire list of WQEs posted to the regular QP,
                init_attr.pd = ctx->pd;

                {
                    init_attr.comp_mask |= IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS | IBV_EXP_QP_INIT_ATTR_PD;
                    init_attr.exp_create_flags = IBV_EXP_QP_CREATE_CROSS_CHANNEL | IBV_EXP_QP_CREATE_IGNORE_SQ_OVERFLOW;
                    ctx->mqp = ibv_exp_create_qp(ctx->ib_ctx, &init_attr);
                }
                if (!ctx->mqp)
                    log_fatal("ibv_create_qp_ex failed\n");

                memset(&attr, 0, sizeof(attr));

                attr.qp_state        = IBV_QPS_INIT;
                attr.pkey_index      = 0;
                attr.port_num        = ctx->ib_port;
                attr.qp_access_flags = 0;

                rc = ibv_modify_qp(ctx->mqp, &attr,
                                   IBV_QP_STATE              |
                                   IBV_QP_PKEY_INDEX         |
                                   IBV_QP_PORT               |
                                   IBV_QP_ACCESS_FLAGS);
                if (rc)
                    log_fatal("ibv_modify_qp failed\n");

                memset(&attr, 0, sizeof(attr));

                attr.qp_state              = IBV_QPS_RTR;
                attr.path_mtu              = IBV_MTU_1024;
                attr.dest_qp_num	   = ctx->mqp->qp_num;
                attr.rq_psn                = 0;
                attr.max_dest_rd_atomic    = 1;
                attr.min_rnr_timer         = 12;
                attr.ah_attr.is_global     = 0;
                attr.ah_attr.dlid          = 0;
                attr.ah_attr.sl            = 0;
                attr.ah_attr.src_path_bits = 0;
                attr.ah_attr.port_num      = 0;

                rc = ibv_modify_qp(ctx->mqp, &attr,
                                   IBV_QP_STATE              |
                                   IBV_QP_AV                 |
                                   IBV_QP_PATH_MTU           |
                                   IBV_QP_DEST_QPN           |
                                   IBV_QP_RQ_PSN             |
                                   IBV_QP_MAX_DEST_RD_ATOMIC |
                                   IBV_QP_MIN_RNR_TIMER);
                if (rc)
                    log_fatal("ibv_modify_qp failed\n");

                memset(&attr, 0, sizeof(attr));

                attr.qp_state      = IBV_QPS_RTS;
                attr.timeout       = 14;
                attr.retry_cnt     = 7;
                attr.rnr_retry     = 7;
                attr.sq_psn        = 0;
                attr.max_rd_atomic = 1;
                rc = ibv_modify_qp(ctx->mqp, &attr,
                                   IBV_QP_STATE              |
                                   IBV_QP_TIMEOUT            |
                                   IBV_QP_RETRY_CNT          |
                                   IBV_QP_RNR_RETRY          |
                                   IBV_QP_SQ_PSN             |
                                   IBV_QP_MAX_QP_RD_ATOMIC);
                if (rc)
                    log_fatal("ibv_modify_qp failed\n");
            }
        }
	ctx->proc_array = (struct cc_proc *)malloc(
			ctx->conf.num_proc * sizeof(*ctx->proc_array));
	if (!ctx->proc_array)
		log_fatal("malloc failed\n");
	memset(ctx->proc_array, 0, ctx->conf.num_proc * sizeof(*ctx->proc_array));

	/*
	 * 3. Create single SCQ for all Peers
	 * =======================================
	 */
	log_trace("create SCQ ...\n");
	ctx->proc_array[ctx->conf.my_proc].scq = ibv_create_cq(ctx->ib_ctx, ctx->conf.cq_tx_depth, NULL, NULL, 0);
	if (!ctx->proc_array[ctx->conf.my_proc].scq)
		log_fatal("ibv_create_cq failed\n");

	log_trace("create SCQ = %p\n", ctx->proc_array[ctx->conf.my_proc].scq);
	/*
	 * 4. Setup connections with Peers
	 * =======================================
	 */
	for (i = 0; i < ctx->conf.num_proc; i++) {

                // if (i == ctx->conf.my_proc)
                        // continue;

		/*
		 * 4.1 Create QPs for Peers
		 * =======================================
		 */
		log_trace("create QPs for peers ...\n");
		{
			struct ibv_exp_cq_attr attr;
			struct ibv_exp_qp_init_attr init_attr;


			ctx->proc_array[i].scq = ctx->proc_array[ctx->conf.my_proc].scq;
			ctx->proc_array[i].rcq = ibv_create_cq(ctx->ib_ctx, ctx->conf.cq_rx_depth, NULL, NULL, 0);
			if (!ctx->proc_array[i].rcq) {
				log_fatal("ibv_create_cq failed\n");
			}

			log_trace("create QPs for peers ...scq=%p  rcq=%p\n", ctx->proc_array[i].scq, ctx->proc_array[i].rcq);


                        attr.comp_mask            = IBV_EXP_CQ_ATTR_CQ_CAP_FLAGS;
                        attr.moderation.cq_count  = 0;
			attr.moderation.cq_period = 0;
			attr.cq_cap_flags         = IBV_EXP_CQ_IGNORE_OVERRUN;

			rc = ibv_exp_modify_cq(ctx->proc_array[i].rcq, &attr, IBV_EXP_CQ_CAP_FLAGS);
			if (rc) {
				log_fatal("ibv_modify_cq failed\n");
			}

			memset(&init_attr, 0, sizeof(init_attr));

			init_attr.qp_context = NULL;
			init_attr.send_cq = ctx->proc_array[i].scq;
			init_attr.recv_cq = ctx->proc_array[i].rcq;
			init_attr.srq = NULL;
			init_attr.cap.max_send_wr  = ctx->conf.qp_tx_depth;
			init_attr.cap.max_recv_wr  = ctx->conf.qp_rx_depth;
			init_attr.cap.max_send_sge = 16;
			init_attr.cap.max_recv_sge = 16;
                        init_attr.cap.max_inline_data = 128;
			init_attr.qp_type = IBV_QPT_RC;
                        init_attr.sq_sig_all = 0;
			init_attr.pd = ctx->pd;

			{
                                init_attr.comp_mask |= IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS | IBV_EXP_QP_INIT_ATTR_PD;
                                if (ctx->conf.use_mq) {
                                    init_attr.exp_create_flags =
                                        IBV_EXP_QP_CREATE_CROSS_CHANNEL      |
                                        IBV_EXP_QP_CREATE_MANAGED_SEND       |
                                        IBV_EXP_QP_CREATE_IGNORE_SQ_OVERFLOW |
                                        IBV_EXP_QP_CREATE_IGNORE_RQ_OVERFLOW;
                                } else {
                                    init_attr.exp_create_flags =
                                        IBV_EXP_QP_CREATE_CROSS_CHANNEL      |
                                        IBV_EXP_QP_CREATE_IGNORE_SQ_OVERFLOW |
                                        IBV_EXP_QP_CREATE_IGNORE_RQ_OVERFLOW;
;
                                }
				ctx->proc_array[i].qp = ibv_exp_create_qp(ctx->ib_ctx, &init_attr);
			}
			if (!ctx->proc_array[i].qp)
				log_fatal("ibv_create_qp_ex failed\n");

		}
	}
		for (i = 0; i < ctx->conf.num_proc; i++) {
			struct cc_proc_info local_info;
			struct cc_proc_info remote_info;

                        // if (i == ctx->conf.my_proc)
                                // continue;
		/*
		 * 4.2 Exchange information with Peers
		 * =======================================
		 */
		log_trace("exchange info with peers ...\n");
		{
			local_info.lid = htons(__get_local_lid(ctx->ib_ctx, ctx->ib_port));
			local_info.qpn = htonl(ctx->proc_array[i].qp->qp_num);
			local_info.psn = htonl(lrand48() & 0xffffff);
			local_info.vaddr = htonll((uintptr_t)ctx->buf + ctx->conf.size * (i + 1));
			local_info.rkey = htonl(ctx->mr->rkey);

#if defined(USE_MPI)
			rc = __exchange_info_by_mpi(ctx, &local_info, &remote_info, i);
#endif
			ctx->proc_array[i].info.lid = ntohs(remote_info.lid);
			ctx->proc_array[i].info.qpn = ntohl(remote_info.qpn);
			ctx->proc_array[i].info.psn = ntohl(remote_info.psn);
			ctx->proc_array[i].info.vaddr = ntohll(remote_info.vaddr);
			ctx->proc_array[i].info.rkey = ntohl(remote_info.rkey);

			log_trace("local     info: lid = %d qpn = %d psn = %d vaddr = 0x%lx rkey = %d\n",
					ntohs(local_info.lid),
					ntohl(local_info.qpn),
					ntohl(local_info.psn),
					ntohll(local_info.vaddr),
					ntohl(local_info.rkey));
			log_trace("peer(#%d) info: lid = %d qpn = %d psn = %d vaddr = 0x%lx rkey = %d\n",
					i,
					ctx->proc_array[i].info.lid,
					ctx->proc_array[i].info.qpn,
					ctx->proc_array[i].info.psn,
					ctx->proc_array[i].info.vaddr,
					ctx->proc_array[i].info.rkey);
		}

		/*
		 * 4.3 Complete QPs for Peers
		 * =======================================
		 */
		log_trace("connect to peers ...\n");
		{
			struct ibv_qp_attr attr;

                        if (__repost(ctx, ctx->proc_array[i].qp, ctx->conf.qp_rx_depth,i) != ctx->conf.qp_rx_depth)
				log_fatal("__post_read failed\n");

			memset(&attr, 0, sizeof(attr));

			attr.qp_state        = IBV_QPS_INIT;
			attr.pkey_index      = 0;
                        attr.port_num        = ctx->ib_port;
                        attr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;

			rc = ibv_modify_qp(ctx->proc_array[i].qp, &attr,
					IBV_QP_STATE              |
					IBV_QP_PKEY_INDEX         |
					IBV_QP_PORT               |
					IBV_QP_ACCESS_FLAGS);
			if (rc)
				log_fatal("ibv_modify_qp failed\n");

			memset(&attr, 0, sizeof(attr));

			attr.qp_state              = IBV_QPS_RTR;
			attr.path_mtu              = IBV_MTU_1024;
			attr.dest_qp_num	   = ctx->proc_array[i].info.qpn;
			attr.rq_psn                = ctx->proc_array[i].info.psn;
			attr.max_dest_rd_atomic    = 4;
			attr.min_rnr_timer         = 12;
			attr.ah_attr.is_global     = 0;
			attr.ah_attr.dlid          = ctx->proc_array[i].info.lid;
			attr.ah_attr.sl            = 0;
			attr.ah_attr.src_path_bits = 0;
			attr.ah_attr.port_num      = ctx->ib_port;

			rc = ibv_modify_qp(ctx->proc_array[i].qp, &attr,
					IBV_QP_STATE              |
					IBV_QP_AV                 |
					IBV_QP_PATH_MTU           |
					IBV_QP_DEST_QPN           |
					IBV_QP_RQ_PSN             |
					IBV_QP_MAX_DEST_RD_ATOMIC |
					IBV_QP_MIN_RNR_TIMER);
			if (rc)
				log_fatal("ibv_modify_qp failed\n");

                        memset(&attr, 0, sizeof(attr));

			attr.qp_state      = IBV_QPS_RTS;
			attr.timeout       = 14;
			attr.retry_cnt     = 7;
			attr.rnr_retry     = 7;
			attr.sq_psn        = ntohl(local_info.psn);
			attr.max_rd_atomic = 4;
			rc = ibv_modify_qp(ctx->proc_array[i].qp, &attr,
					IBV_QP_STATE              |
					IBV_QP_TIMEOUT            |
					IBV_QP_RETRY_CNT          |
					IBV_QP_RNR_RETRY          |
					IBV_QP_SQ_PSN             |
					IBV_QP_MAX_QP_RD_ATOMIC);
			if (rc)
				log_fatal("ibv_modify_qp failed\n");
		}
	}

	return rc;
}

/*
 * This is a place to include new algorithms
 */

#include "cc_latency_test.h"
#include "cc_barrier.h"
#include "cc_rk_barrier.h"
#include "cc_rk_allreduce.h"
#include "cc_ff_barrier.h"
#include "cc_fanin.h"
static void __usage(const char *argv)
{
	if (__my_proc == 0) {
		fprintf(stderr, MODULE_NAME " is an example to demonstrate Cross-Channel operations.\n");
		fprintf(stderr, MODULE_NAME ", version %s\n", STR(VERSION));
		fprintf(stderr, "   compiled %s, %s\n", __DATE__, __TIME__);
		fprintf(stderr, "\n%s\n", MODULE_COPYRIGHT);
		fprintf(stderr, "\n");
		fprintf(stderr, "Options:\n");
		fprintf(stderr, "  %-2s,  %-15s  \t%s\n", "-t", "--test", "test to run (provide unique subset of the test name, see --list option)");
		fprintf(stderr, "  %-2s,  %-15s  \t%s\n", "-l", "--list", "list available tests");
		fprintf(stderr, "  %-2s,  %-15s  \t%s\n", "-n", "--np", "number of processes");
		fprintf(stderr, "  %-2s,  %-15s  \t%s\n", "-r", "--rank", "current process rank");
		fprintf(stderr, "  %-2s,  %-15s  \t%s\n", "-i", "--iters",  "count of launchers (default: 100)");
		fprintf(stderr, "  %-2s,  %-15s  \t%s\n", "-c", "--check",  "check before run (default: 0)");
		fprintf(stderr, "  %-2s,  %-15s  \t%s\n", "-w", "--warmup",  "warmup iterations (default: 10)");
		fprintf(stderr, "  %-2s,  %-15s  \t%s\n", "-d", "--debug=<0,1,2>",  "debug mask (default: 1)");
		fprintf(stderr, "  %-2s,  %-15s  \t%s\n", "-s", "--size=<bytes>",  "size of message to exchange (default: 1)");
		fprintf(stderr, "  %-2s,  %-15s  \t%s\n", "-?", "--help",  "display this help and exit");
		fprintf(stderr, "\n");
	}
	exit(errno);
}

static void show_test_algorithms()
{
	printf("Available test algorithms\n");
	printf("-------------------------------------------------------\n");
	printf("%12s : %s \n", __barrier_algorithm_recursive_doubling_info.short_name, __barrier_algorithm_recursive_doubling_info.name);
	printf("%12s : %s \n", __latency_test_info.short_name,                         __latency_test_info.name);
	printf("\n");
}

static struct cc_alg_info     * get_test_algorithm(char *name)
{
	// For now we look for a substring of the 'short_name' for each test.
	// TBD: add 'registration' mechanism to register a test so that this code can simply iteratee over available tests...

	if (strstr(__barrier_algorithm_recursive_doubling_info.short_name, name) != NULL) {
                log_info("Chose barrier based on recursive doubling \n");
		return &__barrier_algorithm_recursive_doubling_info;  // reference it otherwise we get 'defined but not used' error in compilation
	}

        if (strstr(__rk_barrier_info.short_name, name) != NULL) {
                log_info("Chose barrier based on recursive King \n");
                return &__rk_barrier_info;  // reference it otherwise we get 'defined but not used' error in compilation
	}

        if (strstr(__rk_allreduce_info.short_name, name) != NULL) {
                log_info("Chose allreduce based on recursive King \n");
                return &__rk_allreduce_info;  // reference it otherwise we get 'defined but not used' error in compilation
	}

        if (strstr(__ff_barrier_info.short_name, name) != NULL) {
                log_info("Chose barrier based on Knomial Fanin/Fanout \n");
                return &__ff_barrier_info;  // reference it otherwise we get 'defined but not used' error in compilation
	}

        if (strstr(__knomial_fanin_info.short_name, name) != NULL) {
                log_info("Chose fanin based on knomial tree \n");
                return &__knomial_fanin_info;  // reference it otherwise we get 'defined but not used' error in compilation
        }

        if (strstr(__latency_test_info.short_name, name) != NULL) {
                log_info("Chose latency test\n");
		return &__latency_test_info;
	}


	return NULL;
}

static int __parse_cmd_line( struct cc_context *ctx, int argc, char * const argv[] )
{
	int rc = 0;

	/* Set default parameters */
	ctx->conf.iters = 100;
	ctx->conf.check = 0;
	ctx->conf.warmup = 10;
	ctx->conf.size = 1;
	ctx->conf.cq_tx_depth = 0x0200;
        ctx->conf.cq_rx_depth = 0x0200;
        ctx->conf.qp_tx_depth = 0x0200;
        ctx->conf.qp_rx_depth = 0x0200;
	ctx->conf.algorithm = &__barrier_algorithm_recursive_doubling_info;


#if defined(USE_MPI)
	MPI_Comm_rank(MPI_COMM_WORLD, &ctx->conf.my_proc);
	MPI_Comm_size(MPI_COMM_WORLD, &ctx->conf.num_proc);
	__my_proc = ctx->conf.my_proc;

#else
	logtrace("Compiled without mpi");
#endif

	while (1) {
		int c;

		static struct option long_options[] = {
			{ .name = "help",	.has_arg = 0, .val = '?' },
			{ .name = "test",	.has_arg = 1, .val = 't' },
			{ .name = "list",	.has_arg = 0, .val = 'l' },
			{ .name = "np",		.has_arg = 1, .val = 'n' },
			{ .name = "rank",	.has_arg = 1, .val = 'r' },
			{ .name = "iters",	.has_arg = 1, .val = 'i' },
			{ .name = "check",	.has_arg = 1, .val = 'c' },
			{ .name = "warmup",	.has_arg = 1, .val = 'w' },
			{ .name = "size",	.has_arg = 1, .val = 's' },
			{ .name = "debug",	.has_arg = 1, .val = 'd' },
			{ 0 }
		};

		c = getopt_long(argc, argv, "?t:ln:r:i:s:d", long_options, NULL);  // added a:l
		if (c == -1)
			break;

		switch (c) {
		case '?':
			__usage(argv[0]);
			break;

		case 't':
			ctx->conf.algorithm = get_test_algorithm(optarg);
			break;

		case 'l':
			show_test_algorithms();
			return -1;
			break;

		case 'n':
			ctx->conf.num_proc = strtol(optarg, NULL, 0);
			break;

		case 'r':
			ctx->conf.my_proc = strtol(optarg, NULL, 0);
			__my_proc = ctx->conf.my_proc;
			break;

		case 'i':
			ctx->conf.iters = strtol(optarg, NULL, 0);
			break;

		case 'c':
			ctx->conf.check = strtol(optarg, NULL, 0);
			break;

		case 'w':
			ctx->conf.warmup = strtol(optarg, NULL, 0);
			break;

		case 's':
			ctx->conf.size = strtol(optarg, NULL, 0);
			break;

		case 'd':
			__debug_mask = strtol(optarg, NULL, 0);
			break;

		default:
			__usage(argv[0]);
			return -1;
		}
	}

	if (optind < argc) {
		__usage(argv[0]);
		return -1;
	}

	return rc;
}

int main(int argc, char *argv[])
{
	int rc = 0;
	struct cc_context cc_obj = {0};
	struct cc_context *ctx = &cc_obj;
	struct ibv_device **dev_list = NULL;
	struct ibv_device *ib_dev = NULL;
	const char *ib_devname = NULL;
        char *env = NULL;
        int use_mq = 1;
        int ib_port = 1;
        int oob_barrier = 0;
#if defined(USE_MPI)
	MPI_Init(&argc, &argv);
	log_trace("MPI is enabled\n");
#endif

	/* Parse user provided command line parameters */
	rc = __parse_cmd_line(ctx, argc, argv);

        ib_devname = getenv("CC_IB_DEV");
        env = getenv("CC_USE_MQ");
        if (env) {
            use_mq = atoi(env);
        }
        ctx->conf.use_mq = use_mq;
        env = getenv("CC_IB_PORT");
        if (env) {
            ib_port = atoi(env);
        }

        env = getenv("CC_OOB_BARRIER");
        if (env) {
            oob_barrier = atoi(env);
        }
        ctx->conf.oob_barrier = oob_barrier;
	log_trace("my_proc: %d\n", ctx->conf.my_proc);
	log_trace("num_proc: %d\n", ctx->conf.num_proc);

	/* Get current IB device */
	if (!rc) {
		dev_list = ibv_get_device_list(NULL);
		if (!dev_list)
			log_fatal("No IB devices found\n");

		if (!ib_devname) {
			ib_dev = *dev_list;
			if (!ib_dev)
				log_fatal("No IB devices found\n");
			else
				ib_devname = ibv_get_device_name(ib_dev);
		} else {
			int i = 0;
			for (i = 0; dev_list[i]; ++i)
				if (!strcmp(ibv_get_device_name(dev_list[i]),
				    ib_devname))
					break;
			ib_dev = dev_list[i];
			if (!ib_dev)
				log_fatal("IB device %s not found\n", ib_devname);
		}
	}

	log_trace("device: %s\n", ib_devname);



	// /* Get active port for IB device */
	// if (!rc) {
	// 	int i = 0;
	// 	umad_ca_t ca;
	// 	umad_port_t port;

	// 	rc = umad_get_ca((char *)ib_devname, &ca);
	// 	if (rc)
	// 		log_fatal("umad_get_ca failed\n");

	// 	for (i = 0; i < ca.numports; i++) {
	// 		memset(&port, 0, sizeof(port));
	// 		if ((rc = umad_get_port(ca.ca_name, i+1, &port)) < 0)
	// 			log_fatal("IB device %s does not have active port\n", ib_devname);

	// 		if (port.state == 4) {
	// 			ctx->ib_port = i + 1;
	// 			break;
	// 		}
	// 	}
	// 	umad_release_ca(&ca);
	// }
        ctx->ib_port = ib_port;
	log_trace("port: %d\n", ctx->ib_port);

	/* Open IB device */
	if (!rc) {
		ctx->ib_ctx = ibv_open_device(ib_dev);
		if (!ctx->ib_ctx)
			log_fatal("IB device %s can not be opened\n", ib_devname);
	}

	/* Setup application context */
	if (!rc) {
		log_trace("initialization ...\n");
		rc = __init_ctx(ctx);
	}


	/* Launch target procedure */
	if (!rc && ctx->conf.algorithm) {
		if (!rc && ctx->conf.algorithm->setup) {
			rc = ctx->conf.algorithm->setup(ctx);
			log_trace("setup ...\n");
		}

		if (!rc && ctx->conf.check && ctx->conf.algorithm->check) {
                        rc = ctx->conf.algorithm->check(ctx);
                        if (0 == rc) {
                            log_info("check accuracy (self-test) result is %s\n", "OK");
                        } else {
                            log_fatal("check accuracy (self-test) result is %s\n", "FAIL");
                        }
		}

		if (!rc && ctx->conf.warmup) {
			int iters = ctx->conf.warmup;

			log_trace("warmup ...\n");
                        while (!rc && iters--) {
                            if (ctx->conf.oob_barrier) {
                                MPI_Barrier(MPI_COMM_WORLD);
                            }
                            rc = ctx->conf.algorithm->proc(ctx);
			}
                }

                MPI_Barrier(MPI_COMM_WORLD);
                if (!rc && ctx->conf.iters) {
			struct cc_timer start_time;
                        struct cc_timer end_time;
                        double wt = 0;
                        double cpus = 0;
			int iters = ctx->conf.iters;

			log_trace("start target procedure ...\n");

                        while (!rc && iters--) {
                            if (ctx->conf.oob_barrier) {
                                MPI_Barrier(MPI_COMM_WORLD);
                            }
                            __timer(&start_time);
                            rc = ctx->conf.algorithm->proc(ctx);
                            __timer(&end_time);
                            wt += (end_time.wall - start_time.wall);
                            cpus += (end_time.cpus - start_time.cpus);
                        }


                        cpus /= (double)ctx->conf.iters;
                        wt /= (double)ctx->conf.iters;

                        double wt_av = 0, wt_min = 0, wt_max = 0;
                        double cpus_av = 0;
                        MPI_Allreduce(&wt,&wt_av,1,MPI_DOUBLE,MPI_SUM,MPI_COMM_WORLD);
                        MPI_Allreduce(&wt,&wt_min,1,MPI_DOUBLE,MPI_MIN,MPI_COMM_WORLD);
                        MPI_Allreduce(&wt,&wt_max,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);
                        MPI_Allreduce(&cpus,&cpus_av,1,MPI_DOUBLE,MPI_SUM,MPI_COMM_WORLD);
                        wt_av = wt_av / ctx->conf.num_proc;
                        cpus_av = cpus_av / ctx->conf.num_proc;
                        if (ctx->conf.my_proc == 0) {
				log_info("Title                                      : %s\n", ctx->conf.algorithm->name);
				log_info("Description                                : %s\n", ctx->conf.algorithm->note);
				log_info("Number of processes                        : %d\n", ctx->conf.num_proc);
                                log_info("Iterations                                 : %d\n", ctx->conf.iters);
                                log_info("Time wall [us/op] (min : max : av : root)  : %0.4f : %0.4f : %0.4f : %0.4f us\n", wt_min, wt_max, wt_av, wt);
                                log_info("Time cpus (usec/op)  : %0.4f usec\n", cpus_av);
			}
		}

		if (!rc && ctx->conf.algorithm->close) {
			rc = ctx->conf.algorithm->close(ctx);
			log_trace("close ...\n");
		}
	}


	log_trace("finalization ...\n");
        if (ctx->ib_ctx)
                ibv_close_device(ctx->ib_ctx);
        if (dev_list)
                ibv_free_device_list(dev_list);

#if defined(USE_MPI)
        MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();
#endif

	return rc;
}
