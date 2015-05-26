/*
 * Copyright (c) 2013 Mellanox Technologies.  All rights reserved.
 */


static struct {
	struct ibv_exp_send_wr *wr;
	struct ibv_exp_wc *wc;
	int total_round;
	int num_proc_basic_group;
	int cur_iteration;
} __alg_obj;


static int __algorithm_recursive_doubling_proc( void *context )
{
	int rc = 0;
	struct cc_context *ctx = context;
	int cur_round = 0;
	int my_id = ctx->conf.my_proc;
	int peer_id = 0;
	int ne = 0;
	struct ibv_exp_send_wr wr;
	struct ibv_exp_send_wr *wr_bad;

	__alg_obj.cur_iteration++;

	memset(__alg_obj.wr, 0, 3 * __alg_obj.total_round * sizeof(*__alg_obj.wr));
	memset(__alg_obj.wc, 0, __alg_obj.total_round * sizeof(*__alg_obj.wc));

	/* Wait for a peer from extra group */
	if ((ctx->conf.num_proc - __alg_obj.num_proc_basic_group) > my_id) {
		/* I am in basic group, my partner is a node (my_id + num_proc_basic_group)
		 * in extra group */
		peer_id = my_id + __alg_obj.num_proc_basic_group;

		memset(&wr, 0, sizeof(wr));
		wr.wr_id = cur_round;
		wr.next = NULL;
		wr.exp_opcode = IBV_EXP_WR_CQE_WAIT;
		wr.exp_send_flags = IBV_SEND_SIGNALED | IBV_EXP_SEND_WAIT_EN_LAST;
		wr.task.cqe_wait.cq = ctx->proc_array[peer_id].rcq;
		wr.task.cqe_wait.cq_count = 1;

		rc = ibv_exp_post_send(ctx->mqp, &wr, &wr_bad);
		if (rc)
			log_fatal("can not post to MQP : WR{wr_id=%ld, opcode=%d, send_flags=%ld}\n",
					wr_bad->wr_id, wr_bad->exp_opcode, wr_bad->exp_send_flags);

		if (__alg_obj.cur_iteration >= (ctx->conf.qp_rx_depth - 10))
			if (__post_read(ctx, ctx->proc_array[peer_id].qp, ctx->conf.qp_rx_depth) != ctx->conf.qp_rx_depth)
				log_fatal("__post_read failed\n");
	}

	/* Pairwise exchange inside basic group (and single single step for nodes from extra group) */
	for (cur_round = 0;
			cur_round < (my_id < (ctx->conf.num_proc - __alg_obj.num_proc_basic_group) ?
					__alg_obj.total_round - 1 :
					__alg_obj.total_round);
				cur_round++) {
		if (my_id >= __alg_obj.num_proc_basic_group)
			/* I am in extra group, my partner is a node (my_id - num_proc_basic_group)
			 * in basic group
			 */
			peer_id = my_id - __alg_obj.num_proc_basic_group;
		else
			/* Me and my partner are inside basic group */
			peer_id = my_id ^ (1 << cur_round);

		/* Post SEND to a peer */
		__alg_obj.wr[3 * cur_round + 0].wr_id = cur_round;
		__alg_obj.wr[3 * cur_round + 0].next = NULL;
		__alg_obj.wr[3 * cur_round + 0].exp_opcode = IBV_WR_SEND;

		rc = ibv_exp_post_send(ctx->proc_array[peer_id].qp, &__alg_obj.wr[3 * cur_round + 0], &wr_bad);
		if (rc)
			log_fatal("can not post to QP[%d] : WR{wr_id=%lu, opcode=%u, send_flags=%lu}\n",
					peer_id, wr_bad->wr_id, wr_bad->exp_opcode, wr_bad->exp_send_flags);

		/* Enable SEND to a peer using Managed QP */
		__alg_obj.wr[3 * cur_round + 1].wr_id = cur_round;
		__alg_obj.wr[3 * cur_round + 1].next = &__alg_obj.wr[3 * cur_round + 2];
		__alg_obj.wr[3 * cur_round + 1].exp_opcode = IBV_EXP_WR_SEND_ENABLE;
		__alg_obj.wr[3 * cur_round + 1].exp_send_flags = IBV_EXP_SEND_WAIT_EN_LAST;
		__alg_obj.wr[3 * cur_round + 1].task.wqe_enable.qp = ctx->proc_array[peer_id].qp;
		__alg_obj.wr[3 * cur_round + 1].task.wqe_enable.wqe_count = 1;

		/* Post WAIT for a peer */
		__alg_obj.wr[3 * cur_round + 2].wr_id = cur_round;
		__alg_obj.wr[3 * cur_round + 2].next = NULL;
		__alg_obj.wr[3 * cur_round + 2].exp_opcode = IBV_EXP_WR_CQE_WAIT;
		__alg_obj.wr[3 * cur_round + 2].exp_send_flags = IBV_SEND_SIGNALED | IBV_EXP_SEND_WAIT_EN_LAST;
		__alg_obj.wr[3 * cur_round + 2].task.cqe_wait.cq = ctx->proc_array[peer_id].rcq;
		__alg_obj.wr[3 * cur_round + 2].task.cqe_wait.cq_count = 1;

		rc = ibv_exp_post_send(ctx->mqp, &__alg_obj.wr[3 * cur_round + 1], &wr_bad);
		if (rc)
			log_fatal("can not post to MQP : WR{wr_id=%lu, opcode=%d, send_flags=%ld}\n",
					wr_bad->wr_id, wr_bad->exp_opcode, wr_bad->exp_send_flags);

		if (__alg_obj.cur_iteration >= (ctx->conf.qp_rx_depth - 10))
			if (__post_read(ctx, ctx->proc_array[peer_id].qp, ctx->conf.qp_rx_depth) != ctx->conf.qp_rx_depth)
				log_fatal("__post_read failed\n");
	}

	/* Notify a peer from extra group */
	if ((ctx->conf.num_proc - __alg_obj.num_proc_basic_group) > my_id) {
		/* I am in basic group, my partner is a node (my_id + num_proc_basic_group)
		 * in extra group
		 */
		peer_id = my_id + __alg_obj.num_proc_basic_group;

		memset(&wr, 0, sizeof(wr));
		wr.wr_id = cur_round;
		wr.next = NULL;
		wr.exp_opcode = IBV_WR_SEND;

		rc = ibv_exp_post_send(ctx->proc_array[peer_id].qp, &wr, &wr_bad);
		if (rc)
			log_fatal("can not post to QP[%d] : WR{wr_id=%lu, opcode=%d, send_flags=%ld}\n",
					peer_id, wr_bad->wr_id, wr_bad->exp_opcode, wr_bad->exp_send_flags);

		memset(&wr, 0, sizeof(wr));
		wr.wr_id = cur_round;
		wr.next = NULL;
		wr.exp_opcode = IBV_EXP_WR_SEND_ENABLE;
		wr.exp_send_flags = IBV_EXP_SEND_WAIT_EN_LAST;
		wr.task.wqe_enable.qp = ctx->proc_array[peer_id].qp;
		wr.task.wqe_enable.wqe_count = 1;

		rc = ibv_exp_post_send(ctx->mqp, &wr, &wr_bad);
		if (rc)
			log_fatal("can not post to MQP : WR{wr_id=%lu, opcode=%d, send_flags=%lu}\n",
					wr_bad->wr_id, wr_bad->exp_opcode, wr_bad->exp_send_flags);
	}

	{
		unsigned long start_time_msec;
		unsigned long cur_time_msec;
		struct timeval cur_time;

		gettimeofday(&cur_time, NULL);
		start_time_msec = (cur_time.tv_sec * 1000)
				+ (cur_time.tv_usec / 1000);

		do {
			rc = ibv_exp_poll_cq(ctx->mcq, __alg_obj.total_round, __alg_obj.wc, 1);
			if (rc >= 0)
				ne += rc;
			else
				log_fatal("poll CQ failed\n");

			rc = 0;
			gettimeofday(&cur_time, NULL);
			cur_time_msec = (cur_time.tv_sec * 1000)
					+ (cur_time.tv_usec / 1000);
			if ((cur_time_msec - start_time_msec) > 60000)
				log_fatal("timeout exceeded\n");
		} while (ne < __alg_obj.total_round);
	}


	return rc;
}

static int __algorithm_recursive_doubling_check( void *context )
{
	int rc = 0;
	struct cc_context *ctx = context;
	time_t start;
	time_t finish;
	time_t wait;
	time_t expect_value = 0;
	int num_proc = 0;
	int my_proc = 0;
	const int wait_period = 5;

	num_proc = ctx->conf.num_proc;
	my_proc = ctx->conf.my_proc;

	wait = my_proc * wait_period;
	expect_value = ( (num_proc - my_proc - 1) > 0 ? (num_proc - my_proc - 1) * wait_period - 2 : 0 );

	__sleep(wait);
	start = time(NULL);
	__algorithm_recursive_doubling_proc(context);
	finish = time(NULL);

	rc = (((finish - start) >= expect_value) ? 0 : -1);

	log_trace("my_proc = %d wait = %ld limit = %ld actual wait = %ld\n",
                       my_proc, (unsigned long)wait, (unsigned long)expect_value, (unsigned long)(finish - start));

	return rc;
}

static int __algorithm_recursive_doubling_setup( void *context )
{
	int rc = 0;
	struct cc_context *ctx = context;
	struct ibv_exp_send_wr *wr;
	struct ibv_exp_wc *wc;
	int total_round = 0;
	int num_proc_basic_group = 0;

	/* calculate total number of procs in basic group and number of rounds
	 */
	total_round = __log2(ctx->conf.num_proc);
	num_proc_basic_group = 1 << total_round;
	if (ctx->conf.my_proc >= num_proc_basic_group)
		total_round = 1;
	if (ctx->conf.my_proc < (ctx->conf.num_proc - num_proc_basic_group))
		total_round++;

	wr = (struct ibv_exp_send_wr *)malloc(3 * total_round * sizeof(*wr));
	if (!wr)
		log_fatal("can not allocate memory for WRs\n");
	memset(wr, 0, 3 * total_round * sizeof(*wr));

	wc = (struct ibv_exp_wc *)malloc(total_round * sizeof(*wc));
	if (!wc)
		log_fatal("can not allocate memory for WCs\n");
	memset(wc, 0, total_round * sizeof(*wc));

	__alg_obj.cur_iteration = 0;
	__alg_obj.total_round = total_round;
	__alg_obj.num_proc_basic_group = num_proc_basic_group;
	__alg_obj.wr = wr;
	__alg_obj.wc = wc;

	return rc;
}

static int __algorithm_recursive_doubling_close( void *context )
{
	int rc = 0;

	if (__alg_obj.wr)
		free(__alg_obj.wr);

	if (__alg_obj.wc)
		free(__alg_obj.wc);

	return rc;
}

static struct cc_alg_info __algorithm_recursive_doubling_info = {
		"Barrier: recursive doubling",
		"This algorithm uses Managed QP, IBV_WR_CQE_WAIT, IBV_WR_SEND_ENABLE",
		&__algorithm_recursive_doubling_setup,
		&__algorithm_recursive_doubling_close,
		&__algorithm_recursive_doubling_proc,
		&__algorithm_recursive_doubling_check
};
