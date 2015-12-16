/*
 * Copyright (c) 2013 Mellanox Technologies.  All rights reserved.
 */

#ifndef __CC_RK_BARRIER_H
#define __CC_RK_BARRIER_H
#include <assert.h>
#include "cc_utils.h"
#include "barrier_check.h"

static int __rk_barrier_setup(void*);
static int __rk_barrier_close(void*);
static int __rk_barrier_alg( void*);
static int __rk_barrier_no_mq( void*);
static int __rk_barrier_check(void *);
static struct cc_alg_info __rk_barrier_info = {
    "Barrier: recursive K-ing",
    "barrier_rk",
    "This algorithm uses Managed QP, IBV_WR_CQE_WAIT, IBV_WR_SEND_ENABLE",
    &__rk_barrier_setup,
    &__rk_barrier_close,
    &__rk_barrier_alg,
    &__rk_barrier_check
};

static int __rk_barrier_check(void *context) {
    return __barrier_check(context, &__rk_barrier_alg);
}

static int __rk_barrier_check_no_mq(void *context) {
    return __barrier_check(context, &__rk_barrier_no_mq);
}

static struct {
    int radix;
    int base_num;
    int num_peers;
    int *base_peers;
    int type;
    int steps;
    union{
        int my_extra;
        int my_proxy;
    };
} __rk_barrier;


static int __rk_barrier_alg( void *context)
{
    int rc = 0;
    struct cc_context *ctx = context;
    int peer_id = 0;
    int r = __rk_barrier.radix;

    if (__rk_barrier.type == NODE_EXTRA) {
        post_send_wr_no_sge(ctx, __rk_barrier.my_proxy);
        post_enable_wr(ctx, __rk_barrier.my_proxy, ctx->mqp);
        post_wait_wr(ctx, __rk_barrier.my_proxy, ctx->mqp, 1, 1);
        DBG(KCYN, "extra send to proxy %d and wait", __rk_barrier.my_proxy);
    }

    if (__rk_barrier.type == NODE_PROXY) {
        post_wait_wr(ctx, __rk_barrier.my_extra, ctx->mqp,1, 0);
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
                post_send_wr_no_sge(ctx, peer_id);
                post_enable_wr(ctx, peer_id, ctx->mqp);

            }

            round_peer_count = peer_count;
            for (i=0; i<r-1 && round_peer_count < __rk_barrier.num_peers; i++) {
                int signaled_wait = (round_peer_count == __rk_barrier.num_peers - 1);
                peer_id = __rk_barrier.base_peers[round_peer_count++];
                post_wait_wr(ctx, peer_id, ctx->mqp, 1, signaled_wait);
                DBG(KBLU, "round %d: i %d: peer_id %d, signaled wait %d",
                        round, i, peer_id, signaled_wait);
            }

            peer_count = round_peer_count;
        }

    }

    if (__rk_barrier.type == NODE_PROXY) {
        post_send_wr_no_sge(ctx, __rk_barrier.my_extra);
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

static int __rk_barrier_no_mq( void *context)
{
    int rc = 0;
    struct cc_context *ctx = context;
    int peer_id = 0;
    int my_id = ctx->conf.my_proc;
    int r = __rk_barrier.radix;
    struct ibv_cq *poll_cq = NULL;

    if (__rk_barrier.type == NODE_EXTRA) {
        post_send_wr_no_sge(ctx, __rk_barrier.my_proxy);
        post_wait_wr(ctx, __rk_barrier.my_proxy, ctx->proc_array[my_id].qp, 1, 1);
        DBG(KCYN, "extra send to proxy %d and wait", __rk_barrier.my_proxy);
    }

    if (__rk_barrier.type == NODE_PROXY) {
        // DBG(KCYN, "proxy wait for extra %d", __rk_barrier.my_proxy);
        int round_peer_count  = 0;
        int i;
        for (i=0; i<r-1 && round_peer_count < __rk_barrier.num_peers; i++) {
            peer_id = __rk_barrier.base_peers[round_peer_count++];
            post_wait_wr(ctx, __rk_barrier.my_extra, ctx->proc_array[peer_id].qp,
                         (i == 0) ? 1 : 0 , 0);
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
                        post_wait_wr(ctx, __rk_barrier.my_extra, ctx->proc_array[peer_id].qp, 0, 0);

                    }
                    for (j=0; j<round*(r-1); j++){
                        int wait_count  = (j < (round-1)*(r-1)) || (i > 0) ? 0 : 1;
                        DBG(KBLU, "round %d: wait [%d]: wait_for_peer %d, peer_id %d, wait_count %d",
                            round, i,__rk_barrier.base_peers[j],  peer_id, wait_count);
                        post_wait_wr(ctx, __rk_barrier.base_peers[j],ctx->proc_array[peer_id].qp, wait_count, 0);
                    }
                }
            }

            round_peer_count = peer_count;
            for (i=0; i<r-1 && round_peer_count < __rk_barrier.num_peers; i++) {
                peer_id = __rk_barrier.base_peers[round_peer_count++];
                post_send_wr_no_sge(ctx, peer_id);
                DBG(KBLU, "round %d: i %d: peer_id %d",
                        round, i, peer_id);
            }
            peer_count = round_peer_count;
        }

    }

    if (__rk_barrier.type == NODE_PROXY) {
        int i;
        post_wait_wr(ctx, __rk_barrier.my_extra,
                     ctx->proc_array[__rk_barrier.my_extra].qp, 0, 0);
        for (i=0; i<__rk_barrier.num_peers; i++) {
            int wait_count = i <  (__rk_barrier.steps-1)*(r-1) ? 0 : 1;
            post_wait_wr(ctx, __rk_barrier.base_peers[i],
                         ctx->proc_array[__rk_barrier.my_extra].qp,
                         wait_count,
                         i == (__rk_barrier.num_peers - 1));
        }
        poll_cq  = ctx->proc_array[__rk_barrier.my_extra].scq;
        post_send_wr_no_sge(ctx, __rk_barrier.my_extra);
        DBG(KCYN, "proxy send to extra %d", __rk_barrier.my_extra);
    } else if (__rk_barrier.type == NODE_BASE) {
        int i;
        for (i=0; i<__rk_barrier.num_peers; i++) {
            int wait_count = i <  (__rk_barrier.steps-1)*(r-1) ? 0 : 1;
            DBG(KBLU, "final: wait_for_peer %d posted to self, wait_count %d, signalled %d",
                __rk_barrier.base_peers[i], wait_count, i == (__rk_barrier.num_peers - 1));
            
            post_wait_wr(ctx, __rk_barrier.base_peers[i],
                         ctx->proc_array[my_id].qp,
                         wait_count,
                         i == (__rk_barrier.num_peers - 1));
        }
        poll_cq  = ctx->proc_array[my_id].scq;
    } else {
        poll_cq  = ctx->proc_array[my_id].scq;
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

typedef struct __double_t {
    union {
        double dv;
        uint64_t uv;
    };
} __double_t;

static int __rk_pingpong(void *context) {
    struct cc_context *ctx = context;
    int rank = ctx->conf.my_proc;
    int size = ctx->conf.num_proc;
    int peer = (rank + 1) % size;
    char buf[1024], *rbuf, *lbuf;
    uint32_t rkey;
    MPI_Status st;
    lbuf = &buf[0];
    struct ibv_mr *mr = ibv_reg_mr(ctx->pd, lbuf, 1024,
                                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    MPI_Sendrecv(&lbuf,sizeof(lbuf),MPI_BYTE, peer, 111,
                 &rbuf,sizeof(rbuf),MPI_BYTE, peer, 111,
                 MPI_COMM_WORLD, &st);

    MPI_Sendrecv(&mr->rkey,sizeof(uint32_t),MPI_BYTE, peer, 111,
                 &rkey,sizeof(uint32_t),MPI_BYTE, peer, 111,
                 MPI_COMM_WORLD, &st);
    fprintf(stderr,"rank %d, my_buf %p, my_rkey %d, rbuf %p, rkey %d\n",
            rank, (void*)lbuf, mr->rkey,
            rbuf, rkey);


    MPI_Barrier(MPI_COMM_WORLD);
    struct ibv_cq *cq;
    if (rank == 0){

        // char *buf = (char*)ctx->buf;
        uint64_t *v1 = (uint64_t*)lbuf;
        uint64_t *v2 = (uint64_t*)(lbuf+16);
        uint64_t *v3 = (uint64_t*)(lbuf+32);

        __double_t o1, o2, o3;
        o1.dv = 3.1415;
        o2.dv = 2.7182;
        o3.dv = 1000.0;

        *v1 = htonll(o1.uv);
        *v2 = htonll(o2.uv);
        *v3 = htonll(o3.uv);
        fprintf(stderr,"rank 0, v1 = %d, v2 = %d, v3 = %d\n",
                (int)ntohll(*v1), (int)ntohll(*v2),(int)ntohll(*v3));
        struct ibv_sge sge[2];
        sge[0].addr = (uintptr_t)v1;
        sge[0].length = 48;
        sge[0].lkey = mr->lkey;
        // sge[1].addr = (uintptr_t)v2;
        // sge[1].length = 16;
        // sge[1].lkey = mr->lkey;
        post_send_wr(ctx, 1, &sge[0],1,
                     (uintptr_t)rbuf,rkey,
                     1, IBV_EXP_CALC_OP_ADD, 1);
        post_enable_wr(ctx, 1, ctx->mqp);
        cq = ctx->proc_array[1].scq;
    } else {
        cq = ctx->proc_array[0].rcq;
    }



    int poll = 0;
    struct ibv_wc wc;
    while (poll == 0) {
        poll = ibv_poll_cq(cq,
                           1, &wc);
    }
    if (poll < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr,"rank %d, Got error wc: %s\n",rank, ibv_wc_status_str(wc.status));
    }

    if (rank == 1) {
        uint64_t *v = (uint64_t*)lbuf;
        __double_t rst;
        rst.uv = ntohll(*v);
        fprintf(stderr," rank 1, rst %g\n", rst.dv);
    }

    return 0;
}

static int __rk_pingpong_no_mq(void *context) {
    struct cc_context *ctx = context;
    int size = ctx->conf.num_proc;
    int rank = ctx->conf.my_proc;
    int peer = (rank+1) % size;
    post_send_wr_no_sge(ctx, peer);
    // post_wait_wr(ctx, peer, ctx->proc_array[peer].qp,1,1);
    ctx->proc_array[peer].credits--;
    if (ctx->proc_array[peer].credits <= 10) {
        if (__repost(ctx, ctx->proc_array[peer].qp, ctx->conf.qp_rx_depth, peer) != ctx->conf.qp_rx_depth)
            log_fatal("__post_read failed\n");
    }

    int poll = 0;
    struct ibv_wc wc;
    while (poll == 0) {
        poll = ibv_poll_cq(ctx->proc_array[peer].rcq,
                           1, &wc);
    }
    if (poll < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr,"Got error wc: %s\n",ibv_wc_status_str(wc.status));
    }
    return 0;
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

    if (!ctx->conf.use_mq) {
        __rk_barrier_info.proc = __rk_barrier_no_mq;
        __rk_barrier_info.check = __rk_barrier_check_no_mq;
    }

    var = getenv("CC_PP");
    if (var) {
        __rk_barrier_info.proc = __rk_pingpong;
        if (!ctx->conf.use_mq) {
            __rk_barrier_info.proc = __rk_pingpong_no_mq;
        }
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
    log_info("Knomial radix: %d; base num: %d; steps %d: num_peers %d\n",
             __rk_barrier.radix, __rk_barrier.base_num,
             __rk_barrier.steps, peer_count);


#if defined(PRINT_TREES) && PRINT_TREES > 0
    {
        int i;
        for (i=0; i<ctx->conf.num_proc; i++) {
            if (i == my_id) {
                DBG(KCYN, "peers: %s", __int_arr_2_str(__rk_barrier.base_peers, peer_count));
                usleep(10000);
            }
            MPI_Barrier(MPI_COMM_WORLD);
        }
    }
#endif


    return rc;
}

static int __rk_barrier_close( void *context )
{
    int rc = 0;
    if (__rk_barrier.base_peers)
        free(__rk_barrier.base_peers);
    return rc;
}

#endif
