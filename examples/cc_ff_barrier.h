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
