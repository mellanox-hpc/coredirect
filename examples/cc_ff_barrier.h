/*
 * Copyright (c) 2013 Mellanox Technologies.  All rights reserved.
 */

#ifndef __CC_FF_BARRIER_H
#define __CC_FF_BARRIER_H

#include <assert.h>
#include "cc_utils.h"
#include "barrier_check.h"

static int __ff_barrier_alg(void*);
static int __ff_barrier_setup(void*);
static int __ff_barrier_close(void*);
static int __ff_barrier_check(void *);
static int __ff_barrier_alg_no_mq(void *context);
static int __ff_barrier_check_no_mq(void *context);
static struct cc_alg_info __ff_barrier_info = {
    "Barrier: Knomial Fanin/Fanout",
    "barrier_ff",
    "This algorithm uses Managed QP, IBV_WR_CQE_WAIT, IBV_WR_SEND_ENABLE",
    &__ff_barrier_setup,
    &__ff_barrier_close,
    &__ff_barrier_alg,
    &__ff_barrier_check
};


static struct {
    int radix;
    int fanin_root;
    int *fanin_children;
    int fanin_children_count;
} __ff_barrier;

static int __ff_barrier_check(void *context) {
    return __barrier_check(context, &__ff_barrier_alg);
}

static int __ff_barrier_check_no_mq(void *context) {
    return __barrier_check(context, &__ff_barrier_alg_no_mq);
}


static int __ff_barrier_alg(void *context)
{
    struct cc_context *ctx = context;
    int i;

    for (i=0; i<__ff_barrier.fanin_children_count; i++) {
        post_wait_wr(ctx, __ff_barrier.fanin_children[i],
                     ctx->mqp,1,
                     __ff_barrier.fanin_root == -1 &&
                     i == __ff_barrier.fanin_children_count - 1);
    }

    if (__ff_barrier.fanin_root != -1) {
        post_send_wr(ctx, __ff_barrier.fanin_root);
        post_enable_wr(ctx, __ff_barrier.fanin_root, ctx->mqp);
        post_wait_wr(ctx, __ff_barrier.fanin_root,
                     ctx->mqp,1,1);
    }

    for (i=0; i<__ff_barrier.fanin_children_count; i++) {
        int peer = __ff_barrier.fanin_children[__ff_barrier.fanin_children_count-1-i];
        post_send_wr(ctx, peer);
        post_enable_wr(ctx, peer, ctx->mqp);
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

    return 0;

}

static int __ff_barrier_alg_no_mq(void *context)
{
    struct cc_context *ctx = context;
    int i;
    int my_root = __ff_barrier.fanin_root;
    struct ibv_cq *poll_cq;
    if (my_root == -1) {
        int j;
        int first_peer = __ff_barrier.fanin_children[__ff_barrier.fanin_children_count-1];
        poll_cq = ctx->proc_array[first_peer].scq;
        for (j=0; j <__ff_barrier.fanin_children_count; j++) {
            int peer = __ff_barrier.fanin_children[__ff_barrier.fanin_children_count-1-j];
            for (i=0; i<__ff_barrier.fanin_children_count; i++) {
                post_wait_wr(ctx, __ff_barrier.fanin_children[i],
                             ctx->proc_array[peer].qp,
                             j == 0 ? 1 : 0,
                             (j==0 && i == __ff_barrier.fanin_children_count-1) ? 1 : 0);
            }
            post_send_wr(ctx, peer);
        }
    } else {
        for (i=0; i<__ff_barrier.fanin_children_count; i++) {
            post_wait_wr(ctx, __ff_barrier.fanin_children[i],
                         ctx->proc_array[my_root].qp,1,0);
        }
        post_send_wr(ctx, my_root);

        if (__ff_barrier.fanin_children_count) {
            int first_peer = __ff_barrier.fanin_children[__ff_barrier.fanin_children_count-1];
            poll_cq = ctx->proc_array[first_peer].scq;
            for (i=0; i<__ff_barrier.fanin_children_count; i++) {
                int peer = __ff_barrier.fanin_children[__ff_barrier.fanin_children_count-1-i];
                post_wait_wr(ctx, my_root,
                             ctx->proc_array[peer].qp,
                             i == 0 ? 1 : 0,
                             i == 0 ? 1 : 0);
                post_send_wr(ctx, peer);
            }
        } else {
            poll_cq = ctx->proc_array[my_root].rcq;
            ctx->proc_array[my_root].credits--;
            if (ctx->proc_array[my_root].credits <= 10) {
                if (__repost(ctx, ctx->proc_array[my_root].qp, ctx->conf.qp_rx_depth, my_root) != ctx->conf.qp_rx_depth)
                    log_fatal("__post_read failed\n");
            }
        }
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

    return 0;

}

static int __compare(const void *v1, const void *v2) {
    return *(int*)v1 > *(int*)v2;
}

static int __ff_barrier_setup( void *context )
{
    int rc = 0;
    char *var;
    int steps, base_num, my_id,r,round;
    struct cc_context *ctx = context;
    int dist;


    if (!ctx->conf.use_mq) {
        __ff_barrier_info.proc = __ff_barrier_alg_no_mq;
        __ff_barrier_info.check = __ff_barrier_check_no_mq;
    }
    var = getenv("CC_RADIX");
    if (var) {
        __ff_barrier.radix = atoi(var);
    } else {
        __ff_barrier.radix = 2;
    }
    r = __ff_barrier.radix;
    my_id = ctx->conf.my_proc;
    steps = 0;
    base_num = 1;
    while (base_num*r <= ctx->conf.num_proc) {
        steps++;
        base_num *= r;
    }
    int is_full_tree = (ctx->conf.num_proc == base_num);
    if (!is_full_tree) steps++;


    log_info("Knomial radix: %d\n",r);

    dist = 1;
    for (round=0; round < steps; round++) {
        dist *= r;
    }

    __ff_barrier.fanin_root = -1;
    __ff_barrier.fanin_children =
        (int*)calloc(steps*(r-1),sizeof(int));
    __ff_barrier.fanin_children_count = 0;

    dist /= r;
    int ch_count = 0;
    for (round = steps-1; round >= 0; round--) {
        int full_tree_size = dist*r;
        int i;
        int id = my_id % full_tree_size;
        int id_offset = my_id - id;
        if (id != 0) {
            for (i=0; i<r-1; i++) {
                int peer_id = (id + (i+1)*dist) % full_tree_size + id_offset;
                if (peer_id < ctx->conf.num_proc && (peer_id - id_offset == 0)){
                    __ff_barrier.fanin_root = peer_id;
                }
            }
        }else{
            for (i=0; i<r-1; i++) {
                int peer_id = (id + (i+1)*dist) % full_tree_size + id_offset;
                if (peer_id < ctx->conf.num_proc){
                    __ff_barrier.fanin_children[ch_count++] = peer_id;
                }
            }
            __ff_barrier.fanin_children_count = ch_count;
        }
        dist /= r;
    }

    if (__ff_barrier.fanin_children_count > 0) {
        qsort(__ff_barrier.fanin_children,
              __ff_barrier.fanin_children_count,
              sizeof(int), __compare);
    }
#if defined(PRINT_TREES) && PRINT_TREES > 0
    {
        int i;
        for (i=0; i<ctx->conf.num_proc; i++) {
            if (i == my_id) {
                DBG(KMAG, "FANIN: root %d, children %d : %s",
                    __ff_barrier.fanin_root, __ff_barrier.fanin_children_count,
                    __ff_barrier.fanin_children_count == 0 ? "" :
                    __int_arr_2_str(__ff_barrier.fanin_children, __ff_barrier.fanin_children_count));
            
                usleep(10000);
            }
            MPI_Barrier(MPI_COMM_WORLD);
        }
    }
#endif
    return rc;
}
static int __ff_barrier_close( void *context )
{
    int rc = 0;
    if (__ff_barrier.fanin_children)
        free(__ff_barrier.fanin_children);
    return rc;
}

#endif
