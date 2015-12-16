#ifndef __CC_FANIN_H
#define __CC_FANIN_H

#include <assert.h>
#include "cc_utils.h"
#include "cc_ff_barrier.h"

static int __knomial_fanin(void*);
static int __ff_barrier_setup(void*);
static int __ff_barrier_close(void*);
static int __knomial_fanin_check(void *);

static struct cc_alg_info __knomial_fanin_info = {
    "Fanin: Knomial tree",
    "fanin",
    "This algorithm uses Managed QP, IBV_WR_CQE_WAIT, IBV_WR_SEND_ENABLE",
    &__ff_barrier_setup,
    &__ff_barrier_close,
    &__knomial_fanin,
    &__knomial_fanin_check
};




static int __knomial_fanin(void *context)
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
        post_send_wr_no_sge(ctx, __ff_barrier.fanin_root);
        post_enable_wr(ctx, __ff_barrier.fanin_root, ctx->mqp);
    }else {
        int poll = 0;
        struct ibv_wc wc;
        while (poll == 0) {
            poll = ibv_poll_cq(ctx->mcq,
                               1, &wc);
        }
        if (poll < 0 || wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr,"Got error wc: %s\n",ibv_wc_status_str(wc.status));
        }
        
    }
    return 0;

}

static int __knomial_fanin_check(void *context) {
    return 0;
}
#endif
