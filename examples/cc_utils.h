#ifndef __CC_UTILS_H
#define __CC_UTILS_H

#if 0
#define DBG(color, fmt, ...) fprintf(stderr,color "rank %d: "fmt"\n" KNRM, \
                              ctx->conf.my_proc, ## __VA_ARGS__)

#define KNRM  "\x1B[0m"
#define KRED  "\x1B[31m"
#define KGRN  "\x1B[32m"
#define KYEL  "\x1B[33m"
#define KBLU  "\x1B[34m"
#define KMAG  "\x1B[35m"
#define KCYN  "\x1B[36m"
#define KWHT  "\x1B[37m"

static char _tmp_print[1000];
static inline
char *__int_arr_2_str(int *arr, int num) {
    sprintf(_tmp_print,"[");
    int i;
    for (i=0; i<num-1; i++)
        sprintf(_tmp_print+strlen(_tmp_print), "%d, ", arr[i]);
    sprintf(_tmp_print+strlen(_tmp_print), "%d]", arr[num-1]);
    return &_tmp_print[0];
}
#define PRINT_TREES 0
#else
#define DBG(color, fmt, ...)
#endif

static inline void create_qp_to_dest(int i, struct cc_context *ctx) {
    int rc;
    struct ibv_exp_cq_attr attr;
    struct ibv_exp_qp_init_attr init_attr;

    ctx->proc_array[i].rcq = ibv_create_cq(ctx->ib_ctx, ctx->conf.cq_rx_depth, NULL, NULL, 0);

    ctx->proc_array[i].scq = ctx->proc_array[i].rcq;//ctx->proc_array[ctx->conf.my_proc].scq;

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
    init_attr.cap.max_inline_data = 256;
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

#if defined(USE_MPI)
static inline  int __exchange_info_by_mpi(struct cc_context *ctx,
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

static inline void
exchange_qp_info_and_connect(int i, struct cc_context *ctx) {
    int rc;
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

#define CHECK_CONN(_dest, _ctx) do{                     \
        if (_ctx->proc_array[_dest].rcq == NULL){       \
            create_qp_to_dest(_dest, _ctx);             \
            exchange_qp_info_and_connect(_dest,_ctx);   \
        }                                               \
    }while(0)

static inline
int post_send_wr_no_sge(struct cc_context *ctx, int peer_id) {
    struct ibv_exp_send_wr  wr, *wr_bad;
    int rc = 0;
    CHECK_CONN(peer_id, ctx);
    memset(&wr,0,sizeof(wr));
    wr.wr_id = 0;
    wr.next = NULL;
    wr.exp_opcode = IBV_EXP_WR_RDMA_WRITE_WITH_IMM;
    wr.sg_list = NULL;
    wr.num_sge = 0;
    // fprintf(stderr,"rank %d: post send to %d\n",
            // ctx->conf.my_proc, peer_id);
    rc = ibv_exp_post_send(ctx->proc_array[peer_id].qp, &wr, &wr_bad);
    if (rc)
        log_fatal("can not post to QP[%d] : WR{wr_id=%lu, opcode=%u, send_flags=%lu}\n",
                  1, wr_bad->wr_id, wr_bad->exp_opcode, wr_bad->exp_send_flags);
    return 0;
}

static inline
int post_send_wr(struct cc_context *ctx, int peer_id,
                 struct ibv_sge *sg_list, int num_sge,
                 uintptr_t remote_addr, uint32_t rkey,
                 int calc,
                 enum ibv_exp_calc_op op,  int signaled, int inlined) {
    struct ibv_exp_send_wr  wr, *wr_bad;
    int rc = 0;
    CHECK_CONN(peer_id, ctx);

    memset(&wr,0,sizeof(wr));
    wr.wr_id = 0;
    wr.next = NULL;
    wr.exp_opcode = IBV_EXP_WR_RDMA_WRITE_WITH_IMM;
    wr.sg_list = sg_list;
    wr.num_sge = num_sge;
    if (inlined)
        wr.exp_send_flags = IBV_EXP_SEND_INLINE;
    if (signaled)
        wr.exp_send_flags |= IBV_EXP_SEND_SIGNALED;

    wr.wr.rdma.remote_addr  = remote_addr;
    wr.wr.rdma.rkey = rkey;

    if (calc) {
        wr.exp_send_flags |= IBV_EXP_SEND_WITH_CALC;
        wr.op.calc.calc_op = op;
        wr.op.calc.data_type = IBV_EXP_CALC_DATA_TYPE_FLOAT;
        wr.op.calc.data_size = IBV_EXP_CALC_DATA_SIZE_64_BIT;
    }
    // fprintf(stderr,"rank %d: post send to %d\n",
            // ctx->conf.my_proc, peer_id);
    rc = ibv_exp_post_send(ctx->proc_array[peer_id].qp, &wr, &wr_bad);
    if (rc)
        log_fatal("can not post to QP[%d] : WR{wr_id=%lu, opcode=%u, send_flags=%lu}\n",
                  1, wr_bad->wr_id, wr_bad->exp_opcode, wr_bad->exp_send_flags);
    return 0;
}


static inline void
init_enable_wr(struct cc_context *ctx, int enable_peer,
               struct ibv_exp_send_wr *enable_wr) {
    memset(enable_wr,0,sizeof *enable_wr);
    enable_wr->wr_id = 111;
    enable_wr->next = NULL;
    enable_wr->exp_opcode = IBV_EXP_WR_SEND_ENABLE;
    enable_wr->exp_send_flags = IBV_EXP_SEND_WAIT_EN_LAST;
    enable_wr->task.wqe_enable.qp = ctx->proc_array[enable_peer].qp;
    enable_wr->task.wqe_enable.wqe_count = 1;
    // fprintf(stderr,"rank %d: enable for %d\n",
            // ctx->conf.my_proc, enable_peer);
}

static inline void
init_wait_wr(struct cc_context *ctx, int wait_peer, int signaled,
             int wait_count, struct ibv_exp_send_wr *wait_wr) {
    memset(wait_wr,0,sizeof *wait_wr);
    wait_wr->wr_id = 0xdeadbeef;
    wait_wr->next = NULL;
    wait_wr->exp_opcode = IBV_EXP_WR_CQE_WAIT;
    wait_wr->exp_send_flags = IBV_EXP_SEND_WAIT_EN_LAST;
    if (signaled)
        wait_wr->exp_send_flags |= IBV_EXP_SEND_SIGNALED;
    // fprintf(stderr,"rank %d: waiting for %d, signaled %d\n",
            // ctx->conf.my_proc, wait_peer, signaled);
    wait_wr->task.cqe_wait.cq = ctx->proc_array[wait_peer].rcq;
    wait_wr->task.cqe_wait.cq_count = wait_count;
    ctx->proc_array[wait_peer].credits--;
    if (ctx->proc_array[wait_peer].credits <= 10) {
        if (__repost(ctx, ctx->proc_array[wait_peer].qp, ctx->conf.qp_rx_depth, wait_peer) != ctx->conf.qp_rx_depth)
            log_fatal("__post_read failed\n");
    }
}

static inline
int post_wait_wr(struct cc_context *ctx, int peer_id,
                 struct ibv_qp *wait_qp, int wait_count, int signaled) {
    int rc;
    struct ibv_exp_send_wr wait_wr, *bad_wr = NULL;
    CHECK_CONN(peer_id, ctx);
    init_wait_wr(ctx, peer_id, signaled, wait_count, &wait_wr);
    rc =  ibv_exp_post_send(wait_qp, &wait_wr, &bad_wr);
    return rc;
}

static inline
int post_enable_wr(struct cc_context *ctx, int peer_id,
                   struct ibv_qp *mqp) {
    int rc;
    struct ibv_exp_send_wr enable_wr, *bad_wr = NULL;
    CHECK_CONN(peer_id, ctx);
    init_enable_wr(ctx, peer_id, &enable_wr);
    rc =  ibv_exp_post_send(mqp, &enable_wr , &bad_wr);
    return rc;
}

enum {
    NODE_BASE,
    NODE_EXTRA,
    NODE_PROXY
};

typedef struct __double_t {
    union {
        double dv;
        uint64_t uv;
    };
} __double_t;


#endif
