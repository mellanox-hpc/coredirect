/*
 * Copyright (c) 2013 Mellanox Technologies.  All rights reserved.
 */


#include <assert.h>
enum {
    NODE_BASE,
    NODE_EXTRA,
    NODE_PROXY
};
// static char *type_str[3] = {"BASE", "EXTRA", "PROXY"};

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

#else
#define DBG(color, fmt, ...)
#endif
static struct {
    int radix;
    int base_num;
    int num_peers;
    int *base_peers;
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
        __rk_barrier.cur_task = 0;                                      \
        memset(__rk_barrier.wr,0,sizeof(*__rk_barrier.wr)*__rk_barrier.res_num); \
        memset(__rk_barrier.tasks,0,sizeof(*__rk_barrier.tasks)*__rk_barrier.res_num); \
        int i;                                                          \
        for (i=0; i<__rk_barrier.num_peers; i++)                        \
            ctx->proc_array[__rk_barrier.base_peers[i]].wait_count = 1;                             \
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
    wait_wr->task.cqe_wait.cq_count = ctx->proc_array[wait_peer].wait_count--;
    if (wait_wr->task.cqe_wait.cq_count < 0) wait_wr->task.cqe_wait.cq_count = 0;
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



static int __rk_barrier_rec_doubling( void *context)
{
    int rc = 0;
    struct cc_context *ctx = context;
    int peer_id = 0;
    int r = __rk_barrier.radix;

    RK_BARRIER_RES_INIT(__rk_barrier);
    if (__rk_barrier.type == NODE_EXTRA) {
        post_send_wr(ctx, __rk_barrier.my_proxy);
        post_enable_wr(ctx, __rk_barrier.my_proxy, ctx->mqp);
        post_wait_wr(ctx, __rk_barrier.my_proxy, ctx->mqp, 1);
        DBG(KCYN, "extra send to proxy %d and wait", __rk_barrier.my_proxy);
    }

    if (__rk_barrier.type == NODE_PROXY) {
        post_wait_wr(ctx, __rk_barrier.my_extra, ctx->mqp, 0);
        DBG(KCYN, "proxy wait for extra %d", __rk_barrier.my_proxy);
    }

    if (__rk_barrier.type == NODE_BASE || __rk_barrier.type == NODE_PROXY) {
        int round;
        int peer_count = 0;
        for (round=0; round < __rk_barrier.steps; round++) {
            int i;
            int round_peer_count = peer_count;
            for (i=0; i<r-1 && round_peer_count < __rk_barrier.num_peers; i++) {
                peer_id = __rk_barrier.base_peers[round_peer_count++];
                post_send_wr(ctx, peer_id);
                post_enable_wr(ctx, peer_id, ctx->mqp);

            }

            round_peer_count = peer_count;
            for (i=0; i<r-1 && round_peer_count < __rk_barrier.num_peers; i++) {
                int signaled_wait = (round_peer_count == __rk_barrier.num_peers - 1);
                peer_id = __rk_barrier.base_peers[round_peer_count++];
                post_wait_wr(ctx, peer_id, ctx->mqp, signaled_wait);
                DBG(KBLU, "round %d: i %d: peer_id %d, signaled wait %d",
                        round, i, peer_id, signaled_wait);
            }

            peer_count = round_peer_count;
        }

    }

    if (__rk_barrier.type == NODE_PROXY) {
        post_send_wr(ctx, __rk_barrier.my_extra);
        post_enable_wr(ctx, __rk_barrier.my_extra, ctx->mqp);
        DBG(KCYN, "proxy send to extra %d", __rk_barrier.my_extra);
    }

    int poll = 0;
    struct ibv_wc wc;
    while (poll == 0) {
        poll = ibv_poll_cq(ctx->mcq,
                           1, &wc);
    }
    if (poll < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr,"Got error wc: %s\n",ibv_wc_status_str(wc.status));
    }

    DBG(KGRN, "barrier done");
    return rc;
}

static int __rk_barrier_rec_doubling_no_mq( void *context)
{
    int rc = 0;
    struct cc_context *ctx = context;
    int peer_id = 0;
    int my_id = ctx->conf.my_proc;
    int r = __rk_barrier.radix;

    RK_BARRIER_RES_INIT(__rk_barrier);
    if (__rk_barrier.type == NODE_EXTRA) {
        post_send_wr(ctx, __rk_barrier.my_proxy);
        post_wait_wr(ctx, __rk_barrier.my_proxy, ctx->proc_array[my_id].qp, 1);
        DBG(KCYN, "extra send to proxy %d and wait", __rk_barrier.my_proxy);
    }

    if (__rk_barrier.type == NODE_PROXY) {
        // DBG(KCYN, "proxy wait for extra %d", __rk_barrier.my_proxy);
        int round_peer_count  = 0;
        int i;
        for (i=0; i<r-1 && round_peer_count < __rk_barrier.num_peers; i++) {
            peer_id = __rk_barrier.base_peers[round_peer_count++];
            post_wait_wr(ctx, __rk_barrier.my_extra, ctx->proc_array[peer_id].qp,0);
        }
    }

    if (__rk_barrier.type == NODE_BASE || __rk_barrier.type == NODE_PROXY) {
        int round;
        int peer_count = 0;
        for (round=0; round < __rk_barrier.steps; round++) {
            int i;
            int round_peer_count = peer_count;

            if (round > 0) {
                for (i=0; i<r-1 && round_peer_count < __rk_barrier.num_peers; i++) {
                    int j;
                    peer_id = __rk_barrier.base_peers[round_peer_count++];
                    if (__rk_barrier.type == NODE_PROXY) {
                        post_wait_wr(ctx, __rk_barrier.my_extra, ctx->proc_array[peer_id].qp, 0);
                    }
                    for (j=0; j<round*(r-1); j++){
                        DBG(KBLU, "round %d: wait [%d]: wait_for_peer %d, peer_id %d",
                            round, i,__rk_barrier.base_peers[j],  peer_id);

                        post_wait_wr(ctx, __rk_barrier.base_peers[j],ctx->proc_array[peer_id].qp, 0);
                    }
                }
            }

            round_peer_count = peer_count;
            for (i=0; i<r-1 && round_peer_count < __rk_barrier.num_peers; i++) {
                peer_id = __rk_barrier.base_peers[round_peer_count++];
                post_send_wr(ctx, peer_id);
                DBG(KBLU, "round %d: i %d: peer_id %d",
                        round, i, peer_id);
            }
            peer_count = round_peer_count;
        }

    }
    struct ibv_cq *poll_cq = NULL;
    if (__rk_barrier.type == NODE_PROXY) {
        int i;
        for (i=0; i<__rk_barrier.num_peers; i++) {
            post_wait_wr(ctx, __rk_barrier.base_peers[i],
                         ctx->proc_array[__rk_barrier.my_extra].qp,
                         i == (__rk_barrier.num_peers - 1));
        }
        poll_cq  = ctx->proc_array[__rk_barrier.my_extra].scq;
        post_send_wr(ctx, __rk_barrier.my_extra);
        DBG(KCYN, "proxy send to extra %d", __rk_barrier.my_extra);
    } else if (__rk_barrier.type == NODE_BASE) {
        int i;
        for (i=0; i<__rk_barrier.num_peers; i++) {
            DBG(KBLU, "final: wait_for_peer %d posted to self, signalled %d",
                __rk_barrier.base_peers[i], i == (__rk_barrier.num_peers - 1));
            post_wait_wr(ctx, __rk_barrier.base_peers[i],
                         ctx->proc_array[my_id].qp,
                         i == (__rk_barrier.num_peers - 1));
        }
        poll_cq  = ctx->proc_array[my_id].scq;
    } else {
        poll_cq  = ctx->proc_array[__rk_barrier.my_proxy].scq;
    }

    int poll = 0;
    struct ibv_wc wc;
    while (poll == 0) {
        poll = ibv_poll_cq(poll_cq,
                           1, &wc);
    }
    if (poll < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr,"Got error wc: %s\n",ibv_wc_status_str(wc.status));
    }

    DBG(KGRN, "barrier done");
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
    int is_full_tree = (ctx->conf.num_proc == __rk_barrier.base_num);
    if (!is_full_tree) __rk_barrier.steps++;

    int num_full_subtrees = ctx->conf.num_proc / __rk_barrier.base_num;
    __rk_barrier.base_num *= num_full_subtrees;
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
        __rk_barrier.my_extra = -1;
    }
    int r = __rk_barrier.radix;
    int my_id = ctx->conf.my_proc;

    __rk_barrier.base_peers = (int*)
        calloc(__rk_barrier.steps*(r-1), sizeof(int));

    int peer_count = 0;
    int round;
    int dist = 1;
    for (round=0; round < __rk_barrier.steps; round++) {
        int full_tree_size = dist*r;
        int i;
        int id = my_id % full_tree_size;
        int id_offset = my_id - id;
        for (i=0; i<r-1; i++) {
            int peer_id = (id + (i+1)*dist) % full_tree_size + id_offset;
            if (peer_id < __rk_barrier.base_num){
                __rk_barrier.base_peers[peer_count++] =
                    peer_id;
            }
        }
        dist *= r;
    }

    __rk_barrier.num_peers = peer_count;
    log_info("allreduce radix: %d; base num: %d; steps %d: num_peers %d\n",
             __rk_barrier.radix, __rk_barrier.base_num,
             __rk_barrier.steps, peer_count);

    // fprintf(stderr,"rank %d: type %s, base_num %d, peer_count %d, extra/proxy %d\n",
            // my_id, type_str[__rk_barrier.type], __rk_barrier.base_num,
            // __rk_barrier.num_peers, __rk_barrier.my_proxy);
    
    int num_wrs = total_steps*3*(peer_count);

    __rk_barrier.wr = (struct ibv_exp_send_wr *)
        memalign(sysconf(_SC_PAGESIZE), num_wrs*sizeof(struct ibv_exp_send_wr));
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
    if (__rk_barrier.tasks)
        free(__rk_barrier.tasks);
    if (__rk_barrier.base_peers)
        free(__rk_barrier.base_peers);
    return rc;
}
