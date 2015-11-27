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
    struct ibv_exp_send_wr *wr;
    struct ibv_sge *sge;
    struct ibv_exp_task *tasks;
    int res_num;
    int cur_wr;
    int cur_sge;
    int cur_task;
    int *round_peers;
    struct ibv_mr *mdescs;
    struct ibv_mr *my_mr;
    union{
        int my_extra;
        int my_proxy;
    };
} __allreduce;

#define ALLRED_RES_INIT(__allred) do{                                   \
        __allred.cur_wr = 0;                                            \
        __allred.cur_sge = 0;                                           \
        __allred.cur_task = 0;                                          \
        memset(__allred.wr,0,sizeof(*__allred.wr)*__allred.res_num);    \
        memset(__allred.tasks,0,sizeof(*__allred.tasks)*__allred.res_num); \
    }while(0)

#define GET_NEXT_WR() &__allreduce.wr[__allreduce.cur_wr++];
#define GET_NEXT_TASK() &__allreduce.tasks[__allreduce.cur_task++]
static inline
int post_send_wr(struct cc_context *ctx,
                 int peer_id, int num_sge,
                 int offset) {
    struct ibv_exp_send_wr *wr, *wr_bad;
    struct ibv_sge *start_sge =
        (__allreduce.type == NODE_PROXY) ?
        &__allreduce.sge[0] : &__allreduce.sge[1];
    int remote_offset = (__allreduce.type == NODE_EXTRA) ?
        0 : (2+offset)*16;
    int rc = 0;
    wr = GET_NEXT_WR();
    wr->wr_id = 0;
    wr->next = NULL;
    wr->exp_opcode = IBV_EXP_WR_RDMA_WRITE_WITH_IMM;
    // wr->exp_opcode = IBV_EXP_WR_SEND;
    // wr->exp_send_flags = IBV_EXP_SEND_SIGNALED;
    if (num_sge > 1)
        wr->exp_send_flags |= IBV_EXP_SEND_WITH_CALC;
    wr->op.calc.calc_op = IBV_EXP_CALC_OP_ADD;
    wr->op.calc.data_type = IBV_EXP_CALC_DATA_TYPE_INT;
    wr->op.calc.data_size = IBV_EXP_CALC_DATA_SIZE_64_BIT;
    wr->sg_list = start_sge;
    wr->num_sge = num_sge;

    wr->wr.rdma.remote_addr = (uint64_t)(uintptr_t)
        ((char*)__allreduce.mdescs[peer_id].addr + remote_offset);
    wr->wr.rdma.rkey = __allreduce.mdescs[peer_id].rkey;

    fprintf(stderr,"rank %d: post send to %d, remote_offset %d, num sge %d, sge.addr %p, v = %d\n",
            ctx->conf.my_proc, peer_id, offset, num_sge,
            (void*)wr->sg_list->addr,
            (int)ntohll(*((uint64_t*)wr->sg_list->addr)));
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
    wait_wr->task.cqe_wait.cq = ctx->proc_array[wait_peer].rcq;
    wait_wr->task.cqe_wait.cq_count = 1;
    return wait_wr;
}

static inline
struct ibv_exp_task* get_enable_wait_task(struct cc_context *ctx, int enable_peer,
                                          int wait_peer, int signaled) {
    struct ibv_exp_task *task;
    struct ibv_exp_send_wr *wait_wr, *enable_wr;

    task = GET_NEXT_TASK();
    wait_wr = get_wait_wr(ctx, wait_peer, signaled);
    enable_wr = get_enable_wr(ctx, enable_peer);
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


static int __allreduce_rec_doubling_p( void *context , uint64_t *in,
    uint64_t *out)
{
    int rc = 0;
    struct cc_context *ctx = context;
    int my_id = ctx->conf.my_proc;
    int peer_id = 0;
    int num_sge = 1;
    int r = __allreduce.radix;
    void *my_buf = (void*)((char*)__allreduce.my_mr->addr+16);
    uint64_t *b = (uint64_t*)__allreduce.my_mr->addr;
    *((uint64_t*)my_buf)=htonll(*in);
    // fprintf(stderr,"rank %d my_buf %p:%d, : %d, %d, %d\n",
            // my_id, my_buf,(int)ntohll(*((uint64_t*)my_buf)),
            // (int)ntohll(b[0]), (int)ntohll(b[2]), (int)ntohll(b[4]));

    struct ibv_exp_task *task, *tail = NULL, *bad_task;
    ALLRED_RES_INIT(__allreduce);

    if (__allreduce.type == NODE_EXTRA) {
        assert(0);
        post_send_wr(ctx, __allreduce.my_proxy,num_sge,0);
        tail = get_enable_wait_task(ctx, __allreduce.my_proxy,
                                    __allreduce.my_proxy, 1);
    }

    if (__allreduce.type == NODE_PROXY) {
        assert(0);
        task = get_wait_task(ctx,__allreduce.my_extra,0);
        num_sge++;
    }

    if (__allreduce.type == NODE_BASE || __allreduce.type == NODE_PROXY) {
        int round;
        int dist = 1;
        for (round=0; round < __allreduce.steps; round++) {
            int full_tree_size = dist*r;
            int i;
            int id = my_id % full_tree_size;
            int id_offset = my_id - id;
            int order = 0;
            for (i=0; i<r-1; i++) {
                peer_id = (id + (i+1)*dist) % full_tree_size + id_offset;
                if (my_id > peer_id) order++;
            }
            for (i=0; i<r-1; i++) {
                peer_id = (id + (i+1)*dist) % full_tree_size + id_offset;
                int target_buf_id = my_id < peer_id ? order : order - 1;

                fprintf(stderr,"rank %d: round %d: i %d: peer_id %d, id %d, target_buf_id %d\n",
                        my_id, round, i, peer_id, id, round*(r-1)+target_buf_id);
                post_send_wr(ctx, peer_id, num_sge, round*(r-1)+target_buf_id);
                task = get_enable_wait_task(ctx, peer_id, peer_id, 0);
                if (tail) {
                    tail->next =task;
                }
                tail = task;
            }
            num_sge += (r-1);
            dist *= r;
        }
    }

    if (__allreduce.type == NODE_PROXY) {
        assert(0);
        post_send_wr(ctx, __allreduce.my_extra, num_sge, 0);
        task = get_enable_task(ctx, __allreduce.my_extra, 0);
        if (tail) {
            tail->next =task;
        }
        tail = task;
    }
    if (__allreduce.type == NODE_BASE || __allreduce.type == NODE_PROXY) {
        post_send_wr(ctx, my_id, num_sge, -2);
        task = get_enable_wait_task(ctx, my_id, my_id, 1);
        if (tail) {
            tail->next =task;
        }
        tail = task;
    }
    rc = ibv_exp_post_task(ctx->ib_ctx, &__allreduce.tasks[0], &bad_task);

    if (rc)
        log_fatal("post task fail\n");

    int poll = 0;
    struct ibv_wc wc;
    while (poll == 0) {
        poll = ibv_poll_cq(ctx->mcq,
                           1, &wc);
    }
    if (poll < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr,"Got error wc: %s\n",ibv_wc_status_str(wc.status));
    }

    fprintf(stderr,"rank %d: %d, %d, %d, %d\n",
            my_id, (int)ntohll(b[0]), (int)ntohll(b[2]),
            (int)ntohll(b[4]),(int)ntohll(b[6]));
    *out = ntohll(*((uint64_t*)((char*)__allreduce.my_mr->addr)));
    return rc;
}

static int __allreduce_rec_doubling( void *context ) {
    uint64_t in = 11;
    uint64_t out;
    return __allreduce_rec_doubling_p(context, &in, &out);
}


static int __allreduce_check( void *context )
{
    int rc = 0;
    struct cc_context *ctx = context;
    int r = ctx->conf.my_proc;
    uint64_t *buf = (uint64_t*)__allreduce.mdescs[r].addr;
    uint64_t *b1 = buf;
    uint64_t *b2 = (uint64_t*)((char*)buf + 16);
    uint64_t *b3 = (uint64_t*)((char*)buf + 32);
    struct ibv_cq *poll_cq;

    memset(buf,0,32);
    MPI_Barrier(MPI_COMM_WORLD);


    // double rst_mpi;
    // MPI_Allreduce(&init_v,&rst_mpi,1,MPI_DOUBLE,
                  // MPI_SUM, MPI_COMM_WORLD);

    if (r == 0) {
        struct ibv_exp_send_wr wr;
        struct ibv_sge sge[3];
        uint32_t lkey =
            __allreduce.mdescs[r].lkey;
        *b1 = htonll(1261);
        *b2 = htonll(1301);
        *b3 = htonll(1401);

        memset(sge,0,sizeof(sge));
        wr.wr_id = 0;
        wr.next = NULL;
        wr.exp_opcode = IBV_EXP_WR_RDMA_WRITE_WITH_IMM;
        // wr.exp_opcode = IBV_EXP_WR_SEND;
        wr.exp_send_flags = IBV_EXP_SEND_SIGNALED;
        wr.exp_send_flags |= IBV_EXP_SEND_WITH_CALC;
        wr.op.calc.calc_op = IBV_EXP_CALC_OP_ADD;
        wr.op.calc.data_type = IBV_EXP_CALC_DATA_TYPE_INT;
        wr.op.calc.data_size = IBV_EXP_CALC_DATA_SIZE_64_BIT;
        wr.sg_list = sge;
        wr.num_sge = 3;

        sge[0].addr = (uint64_t)(uintptr_t)b1;
        sge[0].length = 16;
        sge[0].lkey = lkey;

        sge[1].addr = (uint64_t)(uintptr_t)b2;
        sge[1].length = 16;
        sge[1].lkey = lkey;

        sge[2].addr = (uint64_t)(uintptr_t)b3;
        sge[2].length = 16;
        sge[2].lkey = lkey;


        wr.wr.rdma.remote_addr = (uint64_t)(uintptr_t)
            ((char*)__allreduce.mdescs[1].addr);
        wr.wr.rdma.rkey = __allreduce.mdescs[1].rkey;

        struct ibv_exp_send_wr *wr_bad;
        rc = ibv_exp_post_send(ctx->proc_array[1].qp, &wr, &wr_bad);
        if (rc)
            log_fatal("can not post to QP[%d] : WR{wr_id=%lu, opcode=%u, send_flags=%lu}\n",
                      1, wr_bad->wr_id, wr_bad->exp_opcode, wr_bad->exp_send_flags);

#if 0
        struct ibv_exp_task wait, *bad_task;
        struct ibv_exp_send_wr wait_wr, ewr;


        memset(&wait,0,sizeof(wait));

        memset(&ewr, 0, sizeof(ewr));
        ewr.wr_id = 2;
        ewr.next = &wait_wr;
        ewr.exp_opcode = IBV_EXP_WR_SEND_ENABLE;
        ewr.exp_send_flags = IBV_EXP_SEND_WAIT_EN_LAST;
        ewr.task.wqe_enable.qp = ctx->proc_array[1].qp;
        ewr.task.wqe_enable.wqe_count = 1;

        wait.task_type = IBV_EXP_TASK_SEND;
        wait.item.qp = ctx->mqp;
        wait.next = NULL;
        wait.item.send_wr = &ewr;
        memset(&wait_wr,0,sizeof(wait_wr));
        wait_wr.wr_id = 0xdeadbeef;
        wait_wr.next = NULL;
        wait_wr.exp_opcode = IBV_EXP_WR_CQE_WAIT;
        wait_wr.exp_send_flags = IBV_EXP_SEND_SIGNALED | IBV_EXP_SEND_WAIT_EN_LAST;
        wait_wr.task.cqe_wait.cq = ctx->proc_array[1].scq;
        wait_wr.task.cqe_wait.cq_count = 1;
        rc = ibv_exp_post_task(ctx->ib_ctx, &wait, &bad_task);
        poll_cq = ctx->mcq;
        if (rc)
            log_fatal("post task fail\n");
#endif
        poll_cq = ctx->proc_array[1].scq;
        int poll = 0;
        struct ibv_wc wc;
        while (poll == 0) {
            poll = ibv_poll_cq(poll_cq,
                               1, &wc);
        }
        if (poll < 0 || wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr,"Got error wc: %s\n",ibv_wc_status_str(wc.status));
        }
        
    } else if (r == 1){
        b1 = (uint64_t*)((char*)__allreduce.mdescs[1].addr);
        b2 = (uint64_t*)((char*)b1+16);
        // b1 = (uint64_t*)((char*)ctx->buf);
        poll_cq = ctx->proc_array[0].rcq;
        int poll = 0;
        struct ibv_wc wc;
        while (poll == 0) {
            poll = ibv_poll_cq(poll_cq,
                               1, &wc);
        }
        if (poll < 0 || wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr,"Got error wc: %s\n",ibv_wc_status_str(wc.status));
        }
        
    } else {
        return 0;
    }


    fprintf(stderr,KGRN "rank %i: rst=[%d : %d : %d]\n" KNRM,
            r, (int)ntohll(*b1),
            (int)ntohll(*b2), (int)ntohll(*b3));
    return rc;
}

static int __allreduce_setup( void *context )
{
    int rc = 0;
    char *var;
    struct cc_context *ctx = context;

    var = getenv("CC_RADIX");
    if (var) {
        __allreduce.radix = atoi(var);
    } else {
        __allreduce.radix = 2;
    }

    __allreduce.steps = 0;
    __allreduce.base_num = 1;
    while (__allreduce.base_num*__allreduce.radix <=
           ctx->conf.num_proc) {
        __allreduce.steps++;
        __allreduce.base_num *= __allreduce.radix;
    }
    log_info("allreduce radix: %d; base num: %d; steps: %d\n",
             __allreduce.radix, __allreduce.base_num,
             __allreduce.steps);
    int total_steps = __allreduce.steps;
    if (ctx->conf.my_proc >= __allreduce.base_num) {
        __allreduce.type = NODE_EXTRA;
        __allreduce.my_proxy = ctx->conf.my_proc - __allreduce.base_num;
        total_steps = 1;
    } else if (ctx->conf.my_proc <
               (ctx->conf.num_proc - __allreduce.base_num)) {
        __allreduce.type = NODE_PROXY;
        __allreduce.my_extra = __allreduce.base_num + ctx->conf.my_proc;
        total_steps++;
    } else {
        __allreduce.type = NODE_BASE;
    }

    int num_wrs = (total_steps+2)*3;
    int size = num_wrs*16;
    void *addr = memalign(sysconf(_SC_PAGESIZE), size);
    memset(addr,0,size);

    __allreduce.my_mr = ibv_reg_mr(ctx->pd, addr, size,
                                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    __allreduce.mdescs = calloc(ctx->conf.num_proc,
                              sizeof (struct ibv_mr));
    MPI_Allgather(__allreduce.my_mr, sizeof(struct ibv_mr), MPI_BYTE,
                  __allreduce.mdescs,sizeof(struct ibv_mr), MPI_BYTE,
                  MPI_COMM_WORLD);
    assert(__allreduce.my_mr->addr == addr &&
        __allreduce.mdescs[ctx->conf.my_proc].addr == addr);

    __allreduce.wr = (struct ibv_exp_send_wr *)
        malloc(num_wrs*sizeof(struct ibv_exp_send_wr));
    assert(__allreduce.wr);
    __allreduce.sge = (struct ibv_sge*)
        malloc(num_wrs*sizeof(struct ibv_sge));
    assert(__allreduce.sge);
    __allreduce.res_num = num_wrs;
    __allreduce.tasks = (struct ibv_exp_task*)
        malloc(num_wrs*sizeof(struct ibv_exp_task));
    assert(__allreduce.tasks);
    __allreduce.res_num = num_wrs;

    __allreduce.round_peers =
        (int*)malloc(__allreduce.radix*sizeof(int));
    assert(__allreduce.round_peers);
    int i;
    for (i=0; i<num_wrs; i++) {
        __allreduce.sge[i].addr = (uint64_t)(uintptr_t)(void*)((char*)addr + 16*i);
        __allreduce.sge[i].length = 16;
        __allreduce.sge[i].lkey = __allreduce.my_mr->lkey;
        // fprintf(stderr,"rank %d, sge[%d].addr = %p, len = %d\n",
                // ctx->conf.my_proc,i,(void*)__allreduce.sge[i].addr,
                // __allreduce.sge[i].length);
    }
    fprintf(stderr,"rank %d, num_wrs %d, rst: %llu\n",
            ctx->conf.my_proc, num_wrs,
            (long long unsigned)*((uint64_t*)((char*)__allreduce.my_mr->addr+2*16)));
    // uint64_t in=11*(ctx->conf.my_proc+1);
    // uint64_t out = -1;
    // sleep(1);
    // MPI_Barrier(MPI_COMM_WORLD);

    // __allreduce_rec_doubling_p(context, &in, &out);
    // fprintf(stderr,"rank %d, rst: %llu\n",
            // ctx->conf.my_proc,
            // (long long unsigned)out);
    
    return rc;
}

static int __allreduce_close( void *context )
{
    int rc = 0;
    if (__allreduce.wr)
        free(__allreduce.wr);

    if (__allreduce.sge)
        free(__allreduce.sge);

    ibv_dereg_mr(__allreduce.my_mr);
    free(__allreduce.mdescs);
    return rc;
}

static struct cc_alg_info __allreduce_algorithm_recursive_doubling_info = {
    "Allreduce: recursive doubling",
    "allreduce",
    "This algorithm uses Managed QP, IBV_WR_CQE_WAIT, IBV_WR_SEND_ENABLE",
    &__allreduce_setup,
    &__allreduce_close,
    &__allreduce_rec_doubling,
    &__allreduce_check
};
