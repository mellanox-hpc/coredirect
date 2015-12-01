/*
 * Copyright (c) 2013 Mellanox Technologies.  All rights reserved.
 */


#include <assert.h>
enum {
    NODE_BASE,
    NODE_EXTRA,
    NODE_PROXY
};

static struct {
    int radix;
    int base_num;
    int type;
    int steps;
    int res_num;
    struct ibv_exp_send_wr *wr;
    struct ibv_exp_task *tasks;
    int cur_wr;
    int cur_task;
    union{
        int my_extra;
        int my_proxy;
    };
} __rk_barrier;

#define RK_BARRIER_RES_INIT(__rk_barrier) do{                                   \
        __rk_barrier.cur_wr = 0;                                            \
        __rk_barrier.cur_task = 0;                                          \
        memset(__rk_barrier.wr,0,sizeof(*__rk_barrier.wr)*__rk_barrier.res_num);    \
        memset(__rk_barrier.tasks,0,sizeof(*__rk_barrier.tasks)*__rk_barrier.res_num); \
    }while(0)

#define GET_NEXT_WR() &__rk_barrier.wr[__rk_barrier.cur_wr++];
#define GET_NEXT_TASK() &__rk_barrier.tasks[__rk_barrier.cur_task++]
static inline
int post_send_wr(struct cc_context *ctx, int peer_id) {
    struct ibv_exp_send_wr *wr, *wr_bad;
    int rc = 0;
    wr = GET_NEXT_WR();
    wr->wr_id = 0;
    wr->next = NULL;
    wr->exp_opcode = IBV_EXP_WR_RDMA_WRITE_WITH_IMM;
    // wr->exp_opcode = IBV_EXP_WR_SEND;
    // wr->exp_send_flags = IBV_EXP_SEND_SIGNALED;
    wr->sg_list = NULL;
    wr->num_sge = 0;

    // fprintf(stderr,"rank %d: post send to %d\n",
            // ctx->conf.my_proc, peer_id);
    rc = ibv_exp_post_send(ctx->proc_array[peer_id].qp, wr, &wr_bad);
    if (rc)
        log_fatal("can not post to QP[%d] : WR{wr_id=%lu, opcode=%u, send_flags=%lu}\n",
                  1, wr_bad->wr_id, wr_bad->exp_opcode, wr_bad->exp_send_flags);
    return 0;
}


static inline
struct ibv_exp_send_wr *
get_enable_wr(struct cc_context *ctx, int enable_peer) {
    struct ibv_exp_send_wr *enable_wr;
    enable_wr     = GET_NEXT_WR();
    enable_wr->wr_id = 111;
    enable_wr->next = NULL;
    enable_wr->exp_opcode = IBV_EXP_WR_SEND_ENABLE;
    enable_wr->exp_send_flags = IBV_EXP_SEND_WAIT_EN_LAST;
    enable_wr->task.wqe_enable.qp = ctx->proc_array[enable_peer].qp;
    enable_wr->task.wqe_enable.wqe_count = 1;
    // fprintf(stderr,"rank %d: enable for %d\n",
            // ctx->conf.my_proc, enable_peer);
    
    return enable_wr;
}

static inline
struct ibv_exp_send_wr *
get_wait_wr(struct cc_context *ctx, int wait_peer, int signaled) {
    struct ibv_exp_send_wr *wait_wr;
    wait_wr  = GET_NEXT_WR();
    wait_wr->wr_id = 0xdeadbeef;
    wait_wr->next = NULL;
    wait_wr->exp_opcode = IBV_EXP_WR_CQE_WAIT;
    wait_wr->exp_send_flags = IBV_EXP_SEND_WAIT_EN_LAST;
    if (signaled)
        wait_wr->exp_send_flags |= IBV_EXP_SEND_SIGNALED;
    // fprintf(stderr,"rank %d: waiting for %d, signaled %d\n",
            // ctx->conf.my_proc, wait_peer, signaled);
    wait_wr->task.cqe_wait.cq = ctx->proc_array[wait_peer].rcq;
    wait_wr->task.cqe_wait.cq_count = 1;
    ctx->proc_array[wait_peer].credits--;
    if (ctx->proc_array[wait_peer].credits <= 10) {
        if (__repost(ctx, ctx->proc_array[wait_peer].qp, ctx->conf.qp_rx_depth, wait_peer) != ctx->conf.qp_rx_depth)
            log_fatal("__post_read failed\n");
    }
    
    return wait_wr;
}

static inline
int post_wait_wr(struct cc_context *ctx, int peer_id,
                 struct ibv_qp *wait_qp, int signaled) {
    int rc;
    struct ibv_exp_send_wr *bad_wr = NULL;
    rc =  ibv_exp_post_send(wait_qp, get_wait_wr(ctx, peer_id, signaled), &bad_wr);
    return rc;
}

static inline
int post_enable_wr(struct cc_context *ctx, int peer_id,
                   struct ibv_qp *mqp) {
    int rc;
    struct ibv_exp_send_wr *bad_wr = NULL;
    rc =  ibv_exp_post_send(mqp, get_enable_wr(ctx, peer_id), &bad_wr);
    return rc;
}


static inline
struct ibv_exp_task* get_enable_wait_task(struct cc_context *ctx, int enable_peer,
                                          int wait_peer, int signaled) {
    struct ibv_exp_task *task;
    struct ibv_exp_send_wr *wait_wr, *enable_wr;

    task = GET_NEXT_TASK();
    enable_wr = get_enable_wr(ctx, enable_peer);
    wait_wr = get_wait_wr(ctx, wait_peer, signaled);
    enable_wr->next = wait_wr;
    // fprintf(stderr,"rank %d: enable_wait %d:%d\n",
            // ctx->conf.my_proc, enable_peer, wait_peer);
    task->task_type = IBV_EXP_TASK_SEND;
    task->item.qp = ctx->mqp;
    task->next = NULL;
    task->item.send_wr = enable_wr;
    return task;
}
static inline
struct ibv_exp_task* get_wait_task(struct cc_context *ctx,
                                          int wait_peer, int signaled) {
    struct ibv_exp_task *task;
    struct ibv_exp_send_wr *wait_wr;

    task = GET_NEXT_TASK();
    wait_wr = get_wait_wr(ctx, wait_peer, signaled);
    task->task_type = IBV_EXP_TASK_SEND;
    task->item.qp = ctx->mqp;
    task->next = NULL;
    task->item.send_wr = wait_wr;
    return task;
}

static inline
struct ibv_exp_task* get_enable_task(struct cc_context *ctx,
                                          int enable_peer, int signaled) {
    struct ibv_exp_task *task;
    struct ibv_exp_send_wr *enable_wr;

    task = GET_NEXT_TASK();
    enable_wr = get_enable_wr(ctx, enable_peer);
    task->task_type = IBV_EXP_TASK_SEND;
    task->item.qp = ctx->mqp;
    task->next = NULL;
    task->item.send_wr = enable_wr;
    return task;
}


#define KNRM  "\x1B[0m"
#define KRED  "\x1B[31m"
#define KGRN  "\x1B[32m"
#define KYEL  "\x1B[33m"
#define KBLU  "\x1B[34m"
#define KMAG  "\x1B[35m"
#define KCYN  "\x1B[36m"
#define KWHT  "\x1B[37m"


static int __rk_barrier_rec_doubling( void *context)
{
    int rc = 0;
    struct cc_context *ctx = context;
    int my_id = ctx->conf.my_proc;
    int peer_id = 0;
    int r = __rk_barrier.radix;

    RK_BARRIER_RES_INIT(__rk_barrier);

    if (__rk_barrier.type == NODE_EXTRA) {
        assert(0);
        post_send_wr(ctx, __rk_barrier.my_proxy);
        // tail = get_enable_wait_task(ctx, __rk_barrier.my_proxy,
                                    // __rk_barrier.my_proxy, 1);
    }

    if (__rk_barrier.type == NODE_PROXY) {
        assert(0);
        // task = get_wait_task(ctx,__rk_barrier.my_extra,0);
    }

    if (__rk_barrier.type == NODE_BASE || __rk_barrier.type == NODE_PROXY) {
        int round;
        int dist = 1;
        for (round=0; round < __rk_barrier.steps; round++) {
            int full_tree_size = dist*r;
            int i;
            int id = my_id % full_tree_size;
            int id_offset = my_id - id;
            for (i=0; i<r-1; i++) {
                peer_id = (id + (i+1)*dist) % full_tree_size + id_offset;
                // fprintf(stderr,"rank %d: round %d: i %d: peer_id %d, id %d, signaled wait %d\n",
                        // my_id, round, i, peer_id, id, signaled_wait);
                post_send_wr(ctx, peer_id);
                post_enable_wr(ctx, peer_id, ctx->mqp);

            }

            for (i=0; i<r-1; i++) {
                int signaled_wait =
                    ((round == __rk_barrier.steps - 1) &&
                     (i == r-2)) ? 1 : 0;
                peer_id = (id + (i+1)*dist) % full_tree_size + id_offset;
                post_wait_wr(ctx, peer_id, ctx->mqp, signaled_wait);
            }

            dist *= r;
        }
    }

    if (__rk_barrier.type == NODE_PROXY) {
        assert(0);
        post_send_wr(ctx, __rk_barrier.my_extra);
        // task = get_enable_task(ctx, __rk_barrier.my_extra, 0);
        // if (tail) {
            // tail->next =task;
        // }
        // tail = task;
    }
    // rc = ibv_exp_post_task(ctx->ib_ctx, &__rk_barrier.tasks[0], &bad_task);

    // if (rc)
    //     log_fatal("post task fail\n");

    int poll = 0;
    struct ibv_wc wc;
    while (poll == 0) {
        poll = ibv_poll_cq(ctx->mcq,
                           1, &wc);
    }
    if (poll < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr,"Got error wc: %s\n",ibv_wc_status_str(wc.status));
    }

    return rc;
}


static int __rk_barrier_setup( void *context )
{
    int rc = 0;
    char *var;
    struct cc_context *ctx = context;

    var = getenv("CC_RADIX");
    if (var) {
        __rk_barrier.radix = atoi(var);
    } else {
        __rk_barrier.radix = 2;
    }

    __rk_barrier.steps = 0;
    __rk_barrier.base_num = 1;
    while (__rk_barrier.base_num*__rk_barrier.radix <=
           ctx->conf.num_proc) {
        __rk_barrier.steps++;
        __rk_barrier.base_num *= __rk_barrier.radix;
    }
    log_info("allreduce radix: %d; base num: %d; steps: %d\n",
             __rk_barrier.radix, __rk_barrier.base_num,
             __rk_barrier.steps);
    int total_steps = __rk_barrier.steps;
    if (ctx->conf.my_proc >= __rk_barrier.base_num) {
        __rk_barrier.type = NODE_EXTRA;
        __rk_barrier.my_proxy = ctx->conf.my_proc - __rk_barrier.base_num;
        total_steps = 1;
    } else if (ctx->conf.my_proc <
               (ctx->conf.num_proc - __rk_barrier.base_num)) {
        __rk_barrier.type = NODE_PROXY;
        __rk_barrier.my_extra = __rk_barrier.base_num + ctx->conf.my_proc;
        total_steps++;
    } else {
        __rk_barrier.type = NODE_BASE;
    }

    int num_wrs = total_steps*3*(__rk_barrier.radix-1);

    __rk_barrier.wr = (struct ibv_exp_send_wr *)
        malloc(num_wrs*sizeof(struct ibv_exp_send_wr));
    assert(__rk_barrier.wr);

    __rk_barrier.res_num = num_wrs;
    __rk_barrier.tasks = (struct ibv_exp_task*)
        malloc(num_wrs*sizeof(struct ibv_exp_task));
    assert(__rk_barrier.tasks);
    __rk_barrier.res_num = num_wrs;


    return rc;
}

static int __rk_barrier_close( void *context )
{
    int rc = 0;
    if (__rk_barrier.wr)
        free(__rk_barrier.wr);

    return rc;
}
