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

static inline
int post_send_wr_no_sge(struct cc_context *ctx, int peer_id) {
    struct ibv_exp_send_wr  wr, *wr_bad;
    int rc = 0;
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
                 enum ibv_exp_calc_op op,  int signaled) {
    struct ibv_exp_send_wr  wr, *wr_bad;
    int rc = 0;
    memset(&wr,0,sizeof(wr));
    wr.wr_id = 0;
    wr.next = NULL;
    wr.exp_opcode = IBV_EXP_WR_RDMA_WRITE_WITH_IMM;
    wr.sg_list = sg_list;
    wr.num_sge = num_sge;
    // wr.exp_send_flags = IBV_EXP_SEND_INLINE;
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
    init_wait_wr(ctx, peer_id, signaled, wait_count, &wait_wr);
    rc =  ibv_exp_post_send(wait_qp, &wait_wr, &bad_wr);
    return rc;
}

static inline
int post_enable_wr(struct cc_context *ctx, int peer_id,
                   struct ibv_qp *mqp) {
    int rc;
    struct ibv_exp_send_wr enable_wr, *bad_wr = NULL;
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
