/*
 * Copyright (c) 2013 Mellanox Technologies.  All rights reserved.
 */

#include <infiniband/mlx5_hw.h>

int num_sends_per_iteration = 100;   // number of sends (TBD: make this a param)


static struct {
	struct ibv_exp_send_wr *wr;
	struct ibv_wc *wc;
	int num_wr;
	int num_wc;
	int cur_iteration;
	int histogram[100];  // count number of occurrences from 0 to 100 usec.
	double tot_time;
	int num_iter;
} __lat_test_alg_obj;


// The following copied from mxm and slghtly shortened
double get_cpu_freq()
{
    double mhz = 0.0;
    double m;
    int rc;
    FILE* f;
    char buf[256];

    f = fopen("/proc/cpuinfo","r");
    if (!f) {
        return 0.0;
    }

    while (fgets(buf, sizeof(buf), f)) {

#if defined (__ia64__)
        /* Use the ITC frequency on IA64 */
        rc = sscanf(buf, "itc MHz : %lf", &m);
#elif defined (__PPC__) || defined (__PPC64__)
        /* PPC has a different format as well */
        rc = sscanf(buf, "timebase : %lf", &m);
#else
        rc = sscanf(buf, "cpu MHz : %lf", &m);
#endif
        if (rc != 1) {
            continue;
        }
        if (m > mhz) {
            mhz = m;
            continue;
        }
    }
    fclose(f);
    return mhz * 1e6;
}



typedef  unsigned long long timestamp_t;

static inline timestamp_t get_tsc()
{
    uint32_t low, high;
    asm volatile ("rdtsc" : "=a" (low), "=d" (high));
    return ((timestamp_t)high << 32) | (timestamp_t)low;
}

static inline double ts_to_usec(timestamp_t t)
{
	static double freq = 0.0;
	if (freq == 0.0) {
		freq = get_cpu_freq();
	}
	return 1000.0 * 1000.0 *  t/freq;
}


static int wait_for_completions(struct ibv_cq * cq, struct ibv_wc *wc, int expected_completions, int my_id, char* which)
{
	//printf("my_id: %d  %s waiting for %d completions. [%s] cq = %p\n", my_id, __FUNCTION__, expected_completions, which, cq);

	int rc;
	unsigned long start_time_msec;
	unsigned long cur_time_msec;
	struct timeval cur_time;
	int ne = 0;
	int last_ne = -1;

	gettimeofday(&cur_time, NULL);
	start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);

	do {
		rc = ibv_poll_cq(cq, expected_completions, wc);
		if (rc >= 0) {
			ne += rc;

//#define DUMP_ALL_COMPLETIONS
#ifdef DUMP_ALL_COMPLETIONS
			int i;
			for (i = 0 ; i < rc; i++) {
				printf("my_id: %d  %s  got %d completions. # %d : [%s]  cq = %p   wr_id=%d\n", my_id, __FUNCTION__, ne, i, which, cq, ne ? (int)wc[i].wr_id : 0)	;
			}
#endif

		}
		else {
			log_fatal("poll CQ failed!!!!!!!!!!!!!!!!!!\n");
			exit(-1);
		}

		if (wc->status != IBV_WC_SUCCESS) {
			printf("%s Failed status %s (%d) for wr_id %d\n", __FUNCTION__, ibv_wc_status_str(wc->status), wc->status, (int)wc->wr_id);
			return -1;
		}

		rc = 0;
		gettimeofday(&cur_time, NULL);
		cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
		if ((cur_time_msec - start_time_msec) > (3 * 60000)) {
			log_fatal("timeout exceeded\n");
		}
		if (last_ne != ne) {
			//if (ne > 0) {
				//printf("my_id: %d  %s  got %d completions. [%s]  cq = %p   wr_id=%d\n", my_id, __FUNCTION__, ne, which, cq, ne ? (int)wc->wr_id : 0)	;
			//}
			last_ne = ne;
		}
	} while (ne < expected_completions);
	return rc;
}


#define SIG my_id, __FUNCTION__


char *opcode_to_str(int op)
{
	switch (op) {
		case IBV_EXP_WR_SEND:
			return "IBV_EXP_WR_SEND";
		case IBV_EXP_WR_CQE_WAIT:
			return "IBV_EXP_WR_CQE_WAIT";
		case IBV_EXP_WR_SEND_ENABLE:
			return "IBV_EXP_WR_SEND_ENABLE";

		case IBV_EXP_WR_RDMA_WRITE:
		case IBV_EXP_WR_RDMA_WRITE_WITH_IMM:
		case IBV_EXP_WR_SEND_WITH_IMM:
		case IBV_EXP_WR_RDMA_READ:
		case IBV_EXP_WR_ATOMIC_CMP_AND_SWP:
		case IBV_EXP_WR_ATOMIC_FETCH_AND_ADD:
		case IBV_EXP_WR_SEND_WITH_INV:
		case IBV_EXP_WR_LOCAL_INV:
		case IBV_EXP_WR_BIND_MW	:

		case IBV_EXP_WR_RECV_ENABLE:
		case IBV_EXP_WR_EXT_MASKED_ATOMIC_CMP_AND_SWP:
		case IBV_EXP_WR_EXT_MASKED_ATOMIC_FETCH_AND_ADD:
		case IBV_EXP_WR_NOP:
		case IBV_EXP_WR_UMR_FILL:
		case IBV_EXP_WR_UMR_INVALIDATE:
		default:
			printf("%s Got unknown value: %d\n", __FUNCTION__, op);
			return "Other" ;
}

}
void dump_task_info(struct ibv_exp_task *ptask)
{
	// comment for verbose debug
	return;

	printf("%s Task: %p  Type: %s qp=%p      ",
			__FUNCTION__, ptask,
			(ptask->task_type == IBV_EXP_TASK_SEND ? "TASK_SEND" : "RASK_RECV"),
			ptask->item.qp);


	struct ibv_exp_send_wr *wr = ptask->item.send_wr;

	int count = 0;
	while (wr != NULL) {
		printf("send_wr: %s  %p   %s    wr_id=%llu \n", __FUNCTION__, wr,   opcode_to_str(wr->exp_opcode),  (unsigned long long) wr->wr_id);
		wr = wr->next;
		count ++;
	}
	printf("  %d entries\n", count);

}

static int __latency_test_proc_root(void* context)
{
		int completion_on_each_receive = 0;
		struct cc_context *ctx = context;
		int my_id = ctx->conf.my_proc;
		int rc = 0;
		int peer_rank = 1;

		struct ibv_exp_send_wr * wr_mqp;

		int idx = 0;
		int iter = 0;

		struct ibv_exp_send_wr *wr1;
		struct ibv_exp_send_wr *wr2;
		struct ibv_exp_send_wr *wr3;

		struct ibv_exp_send_wr * wr_send_to_qp = &__lat_test_alg_obj.wr[idx];

		// get a series of 'Send' entries.
		for (iter = 0 ; iter < num_sends_per_iteration; iter++) {
				wr1 					= &__lat_test_alg_obj.wr[idx];
				wr2 					= &__lat_test_alg_obj.wr[idx+1];
				wr1->wr_id 				= 1000 + iter;
				wr1->next 				= wr2;
				wr1->exp_opcode 		= IBV_EXP_WR_SEND;
				wr1->exp_send_flags 	= 0;
				idx++;
		}

		wr1->next = NULL;

		// get a series of 'WAIT' and "Send Enable'
		// Wait on the rcq, enable the 'peer qp'
		wr_mqp = &__lat_test_alg_obj.wr[idx];

		for (iter = 0 ; iter < num_sends_per_iteration; iter++) {
				wr1 					= &__lat_test_alg_obj.wr[idx];
				wr2 					= &__lat_test_alg_obj.wr[idx+1];
				wr3 					= &__lat_test_alg_obj.wr[idx+2];

				// enable the 'Send' posted to 'ctx->proc_array[root_rank].qp' by posting a 'SEND_ENABLE' to the mqp.
				wr1->wr_id						= 3000 + iter;
				wr1->next						= wr2;
				wr1->exp_opcode					= IBV_EXP_WR_SEND_ENABLE;
				wr1->exp_send_flags 			= 0;
				wr1->task.wqe_enable.qp			= ctx->proc_array[peer_rank].qp;
				wr1->task.wqe_enable.wqe_count	= 1; // num_sends_per_iteration;

				if (iter == (num_sends_per_iteration -1)) {
					wr1->exp_send_flags	    	|= IBV_EXP_SEND_WAIT_EN_LAST;
				}

				// wait for incoming packet from peer
				wr2->wr_id						= 5000 + iter;
				wr2->next						= wr3;
				wr2->exp_opcode					= IBV_EXP_WR_CQE_WAIT;
				wr2->exp_send_flags				= completion_on_each_receive ? IBV_EXP_SEND_SIGNALED : 0; // IBV_EXP_SEND_SIGNALED; // ask for completion on each receive  // 0; // aaaa
				if (iter == (num_sends_per_iteration -1)) {
					// last WAIT should trigger the completion
					wr2->exp_send_flags	    	= (IBV_EXP_SEND_SIGNALED|IBV_EXP_SEND_WAIT_EN_LAST);
				}
				wr2->task.cqe_wait.cq			= ctx->proc_array[peer_rank].rcq;
				wr2->task.cqe_wait.cq_count		= 1; // num_sends_per_iteration;

				idx += 2;
		}

		wr2->next = NULL;

		struct ibv_exp_task task[2];
		struct ibv_exp_task *task_bad;

		memset(task, 0, sizeof(*task) * 2);

		// Send to root
		task[0].task_type = IBV_EXP_TASK_SEND;
		task[0].item.qp = ctx->proc_array[peer_rank].qp;
		task[0].item.send_wr = wr_send_to_qp;
		task[0].next = &task[1];

		// wait for packet from root, enable packet sent in task1, wait for completion
		task[1].task_type = IBV_EXP_TASK_SEND;
		task[1].item.qp = ctx->mqp;
		task[1].item.send_wr = wr_mqp;
		task[1].next = NULL;

		dump_task_info(&task[0]);
		dump_task_info(&task[1]);

		rc = ibv_exp_post_task(ctx->ib_ctx, task, &task_bad);

		if (rc != 0) {
			printf("my_id: %d %s  ibv_exp_post_task returned %d\n", my_id, __FUNCTION__, rc);
			exit(-1);
		}

		//printf("root waiting for send completions mqpn=0x%x wait enable from cqn 0x%x\n", ctx->mqp->qp_num,
		//		to_mcq(ctx->proc_array[peer_rank].rcq)->cqn);

		int expected_completions = completion_on_each_receive ? num_sends_per_iteration : 1; //  // 1; // num_iterations;  // was 1 // aaaa
		rc = wait_for_completions(ctx->mcq, __lat_test_alg_obj.wc, expected_completions, my_id, "mcq");

		return rc;
	}


static int __latency_test_proc_nonroot(void* context)
{
	int completion_on_each_receive = 0;
	struct cc_context *ctx = context;
	int my_id = ctx->conf.my_proc;
	int rc = 0;
	int root_rank = 0;

	//struct ibv_exp_send_wr * wr_peer_qp;
	struct ibv_exp_send_wr * wr_mqp;

	int idx = 0;

	int iter = 0;

	struct ibv_exp_send_wr *wr1;
	struct ibv_exp_send_wr *wr2;
	struct ibv_exp_send_wr *wr3;

	struct ibv_exp_send_wr * wr_send_to_qp = &__lat_test_alg_obj.wr[idx];

	// get a series of 'Send' entries.

	for (iter = 0 ; iter < num_sends_per_iteration; iter++) {
			// first message sent to each non-root
			wr1 					= &__lat_test_alg_obj.wr[idx];
			wr2 					= &__lat_test_alg_obj.wr[idx+1];
			wr1->wr_id 				= 2000 + iter;
			wr1->next 				= wr2;
			wr1->exp_opcode 		= IBV_EXP_WR_SEND;
			wr1->exp_send_flags 	= 0;
			idx++;
	}
	wr1->next = NULL;

	// get a series of 'WAIT' and "Send Enable'
	// Wait on the rqp, enable the 'peer qp'
	wr_mqp = &__lat_test_alg_obj.wr[idx];

	for (iter = 0 ; iter < num_sends_per_iteration; iter++) {

			wr1 					= &__lat_test_alg_obj.wr[idx];
			wr2 					= &__lat_test_alg_obj.wr[idx+1];
			wr3 					= &__lat_test_alg_obj.wr[idx+2];

			// wait for incoming packet from root rank
			wr1->wr_id						= 6000 + iter;
			wr1->next						= wr2;
			wr1->exp_opcode					= IBV_EXP_WR_CQE_WAIT;
			//wr1->exp_send_flags				= 0;
			wr1->exp_send_flags				= completion_on_each_receive ? IBV_EXP_SEND_SIGNALED : 0; // IBV_EXP_SEND_SIGNALED; // ask for completion on each receive  // 0; // aaaa
			wr1->task.cqe_wait.cq			= ctx->proc_array[root_rank].rcq;
			wr1->task.cqe_wait.cq_count		= 1; // num_sends_per_iteration;

			if (iter == (num_sends_per_iteration -1)) {
				wr1->exp_send_flags	|= IBV_EXP_SEND_WAIT_EN_LAST;
			}

			// enable the 'Send' posted to 'ctx->proc_array[root_rank].qp' by posting a 'SEND_ENABLE' to the mqp.
			wr2->wr_id						= 4000 + iter;
			wr2->next						= wr3;
			wr2->exp_opcode					= IBV_EXP_WR_SEND_ENABLE;
			wr2->exp_send_flags 			= 0;
			wr2->task.wqe_enable.qp			= ctx->proc_array[root_rank].qp;
			wr2->task.wqe_enable.wqe_count	= 1; // num_sends_per_iteration;

			if (iter == (num_sends_per_iteration -1)) {
				wr2->exp_send_flags	|= (IBV_EXP_SEND_SIGNALED | IBV_EXP_SEND_WAIT_EN_LAST);
			}

			idx += 2;
	}

	wr2->next = NULL;


	struct ibv_exp_task task[2];
	struct ibv_exp_task *task_bad;

	memset(task, 0, sizeof(*task) * 2);

	//printf("my_id: %d %s cur_iteration=%d   start\n", SIG, __lat_test_alg_obj.cur_iteration);



	// ------------------------------------------------------------------------------------------------
	// Send to root
	// ------------------------------------------------------------------------------------------------

	task[0].task_type = IBV_EXP_TASK_SEND;
	task[0].item.qp = ctx->proc_array[root_rank].qp;
	task[0].item.send_wr = wr_send_to_qp;
	task[0].next = &task[1];

	// wait for packet from root, enable packet sent in task1, wait for completion
	task[1].task_type = IBV_EXP_TASK_SEND;
	task[1].item.qp = ctx->mqp;
	task[1].item.send_wr = wr_mqp;
	task[1].next = NULL;

	dump_task_info(&task[0]);
	dump_task_info(&task[1]);

	rc = ibv_exp_post_task(ctx->ib_ctx, task, &task_bad);
	if (rc != 0) {
		printf("my_id: %d %s  ibv_exp_post_task returned %d\n", my_id, __FUNCTION__, rc);
		printf("ibv_exp_post_task failed: %d \n", rc);
		exit(-1);
	}

	int expected_completions = completion_on_each_receive ? num_sends_per_iteration: 1;
	rc = wait_for_completions(ctx->mcq, __lat_test_alg_obj.wc, expected_completions, my_id, "mcq");

	//rc = wait_for_completions(ctx->proc_array[root_rank].scq, __lat_test_alg_obj.wc, num_iterations, my_id, "scq");

	//printf("my_id: %d %s cur_iteration=%d   end\n---------------------------------------\n", SIG, __lat_test_alg_obj.cur_iteration);

	return rc;
}


static void update_histogram(double t)
{
	static int max_idx = (sizeof( __lat_test_alg_obj.histogram) / sizeof(__lat_test_alg_obj.histogram[0])) - 1;
	// update histogram
		int idx = (int) t;
		if (idx > max_idx) {
			idx = max_idx;
		}
		__lat_test_alg_obj.histogram[idx]++;
		//printf("idx=%d val=%d\n", idx, __lat_test_alg_obj.histogram[idx]);

		// for average
		__lat_test_alg_obj.tot_time+= t;
		__lat_test_alg_obj.num_iter++;

}

static int __latency_test_proc( void *context )
{

	int rc = 0;
	struct cc_context *ctx = context;

	__lat_test_alg_obj.cur_iteration++;

	memset(__lat_test_alg_obj.wr, 0, __lat_test_alg_obj.num_wr * sizeof(*__lat_test_alg_obj.wr));
	memset(__lat_test_alg_obj.wc, 0, __lat_test_alg_obj.num_wc * sizeof(*__lat_test_alg_obj.wc));


	if (ctx->conf.my_proc == 0) {
		timestamp_t t0 = get_tsc();
		rc = __latency_test_proc_root(context);
		double t = ts_to_usec((get_tsc() - t0));
		update_histogram(t);
		printf("my_id: 0   iteration took: %6.6f  usec  iter=%d\n", t, __lat_test_alg_obj.cur_iteration);
	}
	else {
		rc = __latency_test_proc_nonroot(context);
	}


	int to_post = ctx->conf.qp_rx_depth;
	int peer = (ctx->conf.my_proc + 1) % 2;

	if (__post_read(ctx, ctx->proc_array[peer].qp, to_post) != to_post) {
		printf("%d error in post_read on %p\n", ctx->conf.my_proc, ctx->proc_array[peer].qp);
		exit(-1);
	}

	return rc;
}

static int __latency_test_check( void *context )
{
	int rc = 0;
	return rc;
}

static int __latency_test_setup( void *context )
{
	//int i;
	int rc = 0;
	struct cc_context *ctx = context;
	struct ibv_exp_send_wr *wr;
	struct ibv_wc *wc;

	if (ctx->conf.num_proc != 2) {
		printf("Can only be run with np=2\n");
		return -1;
	}

	int num_wr =  500 * ctx->conf.num_proc;
	int num_wc =  200;

	wr = (struct ibv_exp_send_wr *)malloc(num_wr * sizeof(*wr));
	if (!wr) {
		log_fatal("can not allocate memory for WRs\n");
	}
	memset(wr, 0, num_wr * sizeof(*wr));

	wc = (struct ibv_wc *)malloc(num_wc * sizeof(*wc));
	if (!wc) {
		log_fatal("can not allocate memory for WCs\n");
	}
	memset(wc, 0, num_wc * sizeof(*wc));

	__lat_test_alg_obj.num_wr = num_wr;
	__lat_test_alg_obj.num_wc = num_wc;
	__lat_test_alg_obj.wr = wr;
	__lat_test_alg_obj.wc = wc;
	__lat_test_alg_obj.cur_iteration = 0;

	//for (i = 0; i < sizeof(__lat_test_alg_obj.histogram); i++) {
	//	__lat_test_alg_obj.histogram[i] = 0;
	//}

	log_trace("%s my_proc=%d  num_procs=%d num_wr=%d, num_wc=%d\n", __FUNCTION__, ctx->conf.my_proc, ctx->conf.num_proc, num_wr, num_wc);
	return rc;
}

static int __latency_test_close( void *context )
{
	struct cc_context *ctx = context;
	if (ctx->conf.my_proc == 0) {
		int i;
		int max_idx = (sizeof( __lat_test_alg_obj.histogram) / sizeof(__lat_test_alg_obj.histogram[0])) - 1;
		printf("Results Histogram\n");
		printf("------------------------------------------------------------------\n");
		printf("usec, count\n");
		for (i = 0 ; i <= max_idx; i++) {
			if (__lat_test_alg_obj.histogram[i] > 0) {
				printf("%d , %d\n", i, __lat_test_alg_obj.histogram[i]);
			}
		}
		printf("------------------------------------------------------------------\n");

		printf("Average %6.6f.   Number of sends per iteration: %d\n",  (__lat_test_alg_obj.tot_time / (double)	__lat_test_alg_obj.num_iter), num_sends_per_iteration );
	}

	int rc = 0;

	if (__lat_test_alg_obj.wr) {
		free(__lat_test_alg_obj.wr);
	}

	if (__lat_test_alg_obj.wc) {
		free(__lat_test_alg_obj.wc);
	}

	return rc;
}

static struct cc_alg_info __latency_test_info = {
		"Multi ping pong latency",
		"latency",
		"This algorithm uses Managed QP, IBV_WR_CQE_WAIT, IBV_WR_SEND_ENABLE to implement multiple Core-Direct based PingPong iterations",
		&__latency_test_setup,
		&__latency_test_close,
		&__latency_test_proc,
		&__latency_test_check
};
