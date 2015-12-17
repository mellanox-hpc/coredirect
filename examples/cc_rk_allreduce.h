/*
 * Copyright (c) 2013 Mellanox Technologies.  All rights reserved.
 */

#ifndef __CC_RK_ALLREDUCE_H
#define __CC_RK_ALLREDUCE_H
#include <assert.h>
#include "cc_utils.h"
#include <math.h>

static int __rk_allreduce_setup(void*);
static int __rk_allreduce_close(void*);
static int __rk_allreduce_alg( void*);
static int __rk_allreduce_check(void *);
static int __rk_allreduce_alg_v( void *context, double *value);
static struct cc_alg_info __rk_allreduce_info = {
    "Allreduce: recursive K-ing",
    "allreduce_rk",
    "This algorithm uses Managed QP, IBV_WR_CQE_WAIT, IBV_WR_SEND_ENABLE",
    &__rk_allreduce_setup,
    &__rk_allreduce_close,
    &__rk_allreduce_alg,
    &__rk_allreduce_check
};

static int __rk_allreduce_check(void *context) {
    struct cc_context *ctx = context;
    int local_rst = 0, global_rst = 0;
    int i;
    srand((int)time(NULL));

    for (i=0; i<100; i++) {
        uint64_t uv = rand() % 1000;
        double dv = 3.1415+2.71828*(double)uv;
        double v1 = dv;
        __rk_allreduce_alg_v(context, &v1);
        double v2;
        MPI_Allreduce(&dv, &v2, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
        local_rst = fabs(v2-v1) < 0.1*uv ? 0 : 1;
        MPI_Allreduce(&local_rst, &global_rst, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
        if (global_rst) {
            fprintf(stderr,"rank %d: iteration %d: local value %g, rst %g, mpi_rst %g\n",
                    ctx->conf.my_proc, i, dv, v1, v2);
            break;
        }
    }
    if (ctx->conf.my_proc == 0) {
        DBG(KMAG, "local: %g, exp: %g\n",v1,v2);
    }
    return global_rst;
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
    void *buf;
    struct ibv_mr *mr;
    uintptr_t *addrs;
    uint32_t *rkeys;
} __rk_allreduce;


static int __rk_allreduce_alg_v( void *context, double *value)
{
    int rc = 0;
    struct cc_context *ctx = context;
    int peer_id = 0;
    int r = __rk_allreduce.radix;
    __double_t operand;
    operand.dv = *value;
    uint64_t net_value = htonll(operand.uv);
    int my_id = ctx->conf.my_proc;
    uint64_t *extra_buf = (uint64_t*)__rk_allreduce.buf;
    uint64_t *my_buf = (uint64_t*)((char*)__rk_allreduce.buf + 16);
    struct ibv_sge sge;
    sge.lkey = __rk_allreduce.mr->lkey;
    *my_buf = net_value;

    if (__rk_allreduce.type == NODE_EXTRA) {
        uintptr_t dest_addr = __rk_allreduce.addrs[__rk_allreduce.my_proxy];
        sge.addr   = (uintptr_t)((char*)my_buf);
        sge.length = 8;
        post_send_wr(ctx, __rk_allreduce.my_proxy, &sge,1,dest_addr,
                     __rk_allreduce.rkeys[__rk_allreduce.my_proxy], 0,
                     IBV_EXP_CALC_OP_ADD,0);
        post_enable_wr(ctx, __rk_allreduce.my_proxy, ctx->mqp);
        post_wait_wr(ctx, __rk_allreduce.my_proxy, ctx->mqp, 1, 1);
        DBG(KCYN, "extra send to proxy %d and wait", __rk_allreduce.my_proxy);
    }

    if (__rk_allreduce.type == NODE_PROXY) {
        DBG(KCYN, "proxy wait for extra %d", __rk_allreduce.my_proxy);
        post_wait_wr(ctx, __rk_allreduce.my_extra, ctx->mqp,1, 0);
        sge.addr = (uintptr_t)extra_buf;
        uintptr_t dest_addr =  (uintptr_t)my_buf;
        sge.length = 2*16;
        DBG(KBLU,"rank %d, REDUCE SELF with EXTRA, sge.addr pos %d, len %d, dest post %d\n",
            my_id, (int)((char*)sge.addr-(char*)my_buf)/16, sge.length,
            (int)((char*)dest_addr - (char*)my_buf)/16);
        post_send_wr(ctx, my_id,&sge,1,dest_addr,
                     __rk_allreduce.mr->rkey,1,IBV_EXP_CALC_OP_ADD,0);
        post_enable_wr(ctx, my_id, ctx->mqp);
        post_wait_wr(ctx, my_id, ctx->mqp, 1, 0);
    }

    if (__rk_allreduce.type == NODE_BASE || __rk_allreduce.type == NODE_PROXY) {
        int round;
        int peer_count = 0;
        for (round=0; round < __rk_allreduce.steps; round++) {
            int i;
            int round_peer_count = peer_count;
            int my_pos = 0;
            for (i=0; i<r-1 && round_peer_count < __rk_allreduce.num_peers; i++) {
                peer_id = __rk_allreduce.base_peers[round_peer_count++];
                if (peer_id < my_id)
                    my_pos++;
            }
            round_peer_count = peer_count;
            for (i=0; i<r-1 && round_peer_count < __rk_allreduce.num_peers; i++) {

                peer_id = __rk_allreduce.base_peers[round_peer_count++];
                int cur_pos = (peer_id < my_id) ? my_pos-1 : my_pos;
                uintptr_t dest_addr = (uintptr_t)
                    ((char*)__rk_allreduce.addrs[peer_id]
                     +round*r*16+
                     +16+16+cur_pos*16);
                sge.addr   = (uintptr_t)((char*)my_buf+round*r*16);
                sge.length = 8;
                int need_calc = 0;
                DBG(KBLU,"rank %d, send to %d, sge.addr pos %d, len %d, dest post %d, calc %d",
                        my_id, peer_id, (int)((char*)sge.addr-(char*)my_buf)/16, sge.length,
                        (int)((char*)dest_addr - (char*)__rk_allreduce.addrs[peer_id])/16, need_calc);
                post_send_wr(ctx, peer_id, &sge,1,dest_addr,
                             __rk_allreduce.rkeys[peer_id], need_calc,
                             IBV_EXP_CALC_OP_ADD,0);
                post_enable_wr(ctx, peer_id, ctx->mqp);

            }

            round_peer_count = peer_count;
            for (i=0; i<r-1 && round_peer_count < __rk_allreduce.num_peers; i++) {
                peer_id = __rk_allreduce.base_peers[round_peer_count++];
                post_wait_wr(ctx, peer_id, ctx->mqp, 1, 0);
                DBG(KBLU, "round %d: i %d: peer_id %d",
                        round, i, peer_id);
            }

            sge.addr = (uintptr_t)((char*)my_buf+round*r*16);
            uintptr_t dest_addr = (round == __rk_allreduce.steps - 1) ?
                (uintptr_t)extra_buf : (uintptr_t)((char*)my_buf+(round+1)*r*16);
            sge.length = (round_peer_count - peer_count + 1)*16;
            DBG(KBLU,"rank %d, REDUCE SELF, sge.addr pos %d, len %d, dest post %d",
                    my_id, (int)((char*)sge.addr-(char*)my_buf)/16, sge.length,
                    (int)((char*)dest_addr - (char*)my_buf)/16);
            post_send_wr(ctx, my_id,&sge,1,dest_addr,
                         __rk_allreduce.mr->rkey,1,IBV_EXP_CALC_OP_ADD,0);
            post_enable_wr(ctx, my_id, ctx->mqp);
            post_wait_wr(ctx, my_id, ctx->mqp, 1,
                         round == __rk_allreduce.steps - 1? 1: 0);
            peer_count = round_peer_count;
        }

    }

    if (__rk_allreduce.type == NODE_PROXY) {
        uintptr_t dest_addr = __rk_allreduce.addrs[__rk_allreduce.my_extra];
        sge.addr   = (uintptr_t)((char*)extra_buf);
        sge.length = 8;
        post_send_wr(ctx, __rk_allreduce.my_extra, &sge,1,dest_addr,
                     __rk_allreduce.rkeys[__rk_allreduce.my_extra], 0,
                     IBV_EXP_CALC_OP_ADD,0);
        post_enable_wr(ctx, __rk_allreduce.my_extra, ctx->mqp);
        DBG(KCYN, "proxy send to extra %d", __rk_allreduce.my_extra);
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
    operand.uv = ntohll(*extra_buf);
    *value = operand.dv;

#if 0
    int i;
    for (i=0; i<ctx->conf.num_proc; i++) {
        if (my_id == i) {
            uint64_t *b = (uint64_t*)__rk_allreduce.buf;
            fprintf(stderr,"rank %d buf: ", my_id);
            int j;
            for (j=0; j<__rk_allreduce.steps*r*2+1; j++) {
                __double_t tmp;
                tmp.uv = ntohll(b[j]);
                fprintf(stderr,"%g ",tmp.dv);
            }
            fprintf(stderr,"\n");
            usleep(1000);
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }
#endif

    DBG(KGRN, "allreduce done");
    return rc;
}
static int __rk_allreduce_alg( void *context) {
    double v = 1.1;
    struct cc_context *ctx = context;
    __rk_allreduce_alg_v(ctx, &v);
    return 0;
}

static int __rk_allreduce_setup( void *context )
{
    int rc = 0;
    char *var;
    struct cc_context *ctx = context;
    var = getenv("CC_RADIX");
    if (var) {
        __rk_allreduce.radix = atoi(var);
    } else {
        __rk_allreduce.radix = 2;
    }


    __rk_allreduce.steps = 0;
    __rk_allreduce.base_num = 1;
    while (__rk_allreduce.base_num*__rk_allreduce.radix <=
           ctx->conf.num_proc) {
        __rk_allreduce.steps++;
        __rk_allreduce.base_num *= __rk_allreduce.radix;
    }
    int is_full_tree = (ctx->conf.num_proc == __rk_allreduce.base_num);
    if (!is_full_tree) __rk_allreduce.steps++;

    int num_full_subtrees = ctx->conf.num_proc / __rk_allreduce.base_num;
    __rk_allreduce.base_num *= num_full_subtrees;
    int total_steps = __rk_allreduce.steps;
    if (ctx->conf.my_proc >= __rk_allreduce.base_num) {
        __rk_allreduce.type = NODE_EXTRA;
        __rk_allreduce.my_proxy = ctx->conf.my_proc - __rk_allreduce.base_num;
        total_steps = 1;
    } else if (ctx->conf.my_proc <
               (ctx->conf.num_proc - __rk_allreduce.base_num)) {
        __rk_allreduce.type = NODE_PROXY;
        __rk_allreduce.my_extra = __rk_allreduce.base_num + ctx->conf.my_proc;
        total_steps++;
    } else {
        __rk_allreduce.type = NODE_BASE;
        __rk_allreduce.my_extra = -1;
    }
    int r = __rk_allreduce.radix;
    int my_id = ctx->conf.my_proc;

    __rk_allreduce.base_peers = (int*)
        calloc(__rk_allreduce.steps*(r-1), sizeof(int));

    int peer_count = 0;
    int round;
    int dist = 1;
    for (round=0; round < __rk_allreduce.steps; round++) {
        int full_tree_size = dist*r;
        int i;
        int id = my_id % full_tree_size;
        int id_offset = my_id - id;
        for (i=0; i<r-1; i++) {
            int peer_id = (id + (i+1)*dist) % full_tree_size + id_offset;
            if (peer_id < __rk_allreduce.base_num){
                __rk_allreduce.base_peers[peer_count++] =
                    peer_id;
            }
        }
        dist *= r;
    }
    __rk_allreduce.num_peers = peer_count;
    log_info("Knomial radix: %d; base num: %d; steps %d: num_peers %d\n",
             __rk_allreduce.radix, __rk_allreduce.base_num,
             __rk_allreduce.steps, peer_count);


#if defined(PRINT_TREES) && PRINT_TREES > 0
    {
        int i;
        for (i=0; i<ctx->conf.num_proc; i++) {
            if (i == my_id) {
                DBG(KCYN, "peers: %s", __int_arr_2_str(__rk_allreduce.base_peers, peer_count));
                usleep(10000);
            }
            MPI_Allreduce(MPI_COMM_WORLD);
        }
    }
#endif

    size_t buf_len = 8*2*(r*__rk_allreduce.steps+1);
    __rk_allreduce.buf = malloc(buf_len);
    __rk_allreduce.mr = ibv_reg_mr(ctx->pd, __rk_allreduce.buf, buf_len,
                                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    memset(__rk_allreduce.buf,0,buf_len);
    __rk_allreduce.addrs = (uintptr_t*)calloc(sizeof(uintptr_t),ctx->conf.num_proc);
    __rk_allreduce.rkeys = (uint32_t*)calloc(sizeof(uint32_t),ctx->conf.num_proc);
    MPI_Allgather(&__rk_allreduce.buf,sizeof(void*),MPI_BYTE,
                  __rk_allreduce.addrs,sizeof(void*),MPI_BYTE,
                  MPI_COMM_WORLD);

    MPI_Allgather(&__rk_allreduce.mr->rkey,sizeof(uint32_t),MPI_BYTE,
                  __rk_allreduce.rkeys,sizeof(uint32_t),MPI_BYTE,
                  MPI_COMM_WORLD);

    return rc;
}

static int __rk_allreduce_close( void *context )
{
    int rc = 0;
    if (__rk_allreduce.base_peers)
        free(__rk_allreduce.base_peers);
    if (__rk_allreduce.buf)
        free(__rk_allreduce.buf);
    if (__rk_allreduce.addrs)
        free(__rk_allreduce.addrs);
    if (__rk_allreduce.rkeys)
        free(__rk_allreduce.rkeys);
    if (__rk_allreduce.mr)
        ibv_dereg_mr(__rk_allreduce.mr);

    return rc;
}

#endif
