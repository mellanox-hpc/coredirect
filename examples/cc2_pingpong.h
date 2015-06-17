/*
 * Copyright (c) 2006 Cisco Systems.  All rights reserved.
 * Copyright (c) 2009-2010 Mellanox Technologies.  All rights reserved.
 */

#ifndef IBV_MVERBS_TEST_H
#define IBV_MVERBS_TEST_H

#define _GNU_SOURCE

#include <stdio.h>
#include <infiniband/verbs.h>
#include <infiniband/verbs_exp.h>


enum ibv_m_wr_data_type {
		IBV_M_DATA_TYPE_INT8 = 0,
		IBV_M_DATA_TYPE_INT16,
		IBV_M_DATA_TYPE_INT32,
		IBV_M_DATA_TYPE_INT64,
		IBV_M_DATA_TYPE_INT128,
		IBV_M_DATA_TYPE_FLOAT32,
		IBV_M_DATA_TYPE_FLOAT64,
		IBV_M_DATA_TYPE_FLOAT96,
		IBV_M_DATA_TYPE_FLOAT128,
		IBV_M_DATA_TYPE_COMPLEX ,
		IBV_M_DATA_TYPE_INVALID
};

//ibv_m_wr_opcode

enum ibv_m_wr_calc_op {
	IBV_M_CALC_OP_LXOR = 0,
	IBV_M_CALC_OP_BXOR,
	IBV_M_CALC_OP_LOR,
	IBV_M_CALC_OP_BOR,
	IBV_M_CALC_OP_LAND,
	IBV_M_CALC_OP_BAND,
	IBV_M_CALC_OP_ADD,
	IBV_M_CALC_OP_MAX,
	IBV_M_CALC_OP_MIN,
	IBV_M_CALC_OP_MAXLOC,
	IBV_M_CALC_OP_MINLOC,
	IBV_M_CALC_OP_PROD,
	IBV_M_CALC_OP_INVALID
};

static struct {
	char size;
	const char str[32];
} ibv_m_wr_data_type_str[] = {
	[IBV_M_DATA_TYPE_INT8]     = { .size = 1,  .str = "INT8" },
	[IBV_M_DATA_TYPE_INT16]    = { .size = 2,  .str = "INT16"},
	[IBV_M_DATA_TYPE_INT32]    = { .size = 4,  .str = "INT32"},
	[IBV_M_DATA_TYPE_INT64]    = { .size = 8,  .str = "INT64"},
	[IBV_M_DATA_TYPE_INT128]   = { .size = 16, .str = "INT128"},
	[IBV_M_DATA_TYPE_FLOAT32]  = { .size = 4,  .str = "FLOAT32"},
	[IBV_M_DATA_TYPE_FLOAT64]  = { .size = 8,  .str = "FLOAT64"},
	[IBV_M_DATA_TYPE_FLOAT96]  = { .size = 12, .str = "FLOAT96"},
	[IBV_M_DATA_TYPE_FLOAT128] = { .size = 16, .str = "FLOAT128"},
	[IBV_M_DATA_TYPE_COMPLEX]  = { .size = 16, .str = "COMPLEX"}
};

static const char ibv_m_wr_calc_op_str[][32] = {
	[IBV_M_CALC_OP_LXOR]    = "XOR",
	[IBV_M_CALC_OP_BXOR]    = "BXOR",
	[IBV_M_CALC_OP_LOR]     = "LOR",
	[IBV_M_CALC_OP_BOR]     = "BOR",
	[IBV_M_CALC_OP_LAND]    = "LAND",
	[IBV_M_CALC_OP_BAND]    = "BAND",
	[IBV_M_CALC_OP_ADD]     = "ADD",
	[IBV_M_CALC_OP_MAX]     = "MAX",
	[IBV_M_CALC_OP_MIN]     = "MIN",
	[IBV_M_CALC_OP_MAXLOC]  = "MAXLOC",
	[IBV_M_CALC_OP_MINLOC]  = "MINLOC",
	[IBV_M_CALC_OP_PROD]    = "PROD"
};

static inline void ibv_m_print_data_type(void)
{
	int i;

	for (i = 0; i < IBV_M_DATA_TYPE_INVALID; i++)
		printf("\t%s\n", ibv_m_wr_data_type_str[i].str);
}

static inline const char *ibv_m_data_type_to_str(enum ibv_m_wr_data_type data_type)
{
	if (data_type < sizeof(ibv_m_wr_data_type_str)/sizeof(ibv_m_wr_data_type_str[0]))
		return ibv_m_wr_data_type_str[data_type].str;

	return "INVALID DATA TYPE";

}

static inline int ibv_m_data_type_to_size(enum ibv_m_wr_data_type data_type)
{
	if (data_type < sizeof(ibv_m_wr_data_type_str)/sizeof(ibv_m_wr_data_type_str[0]))
		return ibv_m_wr_data_type_str[data_type].size;

	return -1;
}

static inline enum ibv_m_wr_data_type ibv_m_str_to_data_type(const char *data_type_str)
{
	int i;

	for (i = 0; i < sizeof(ibv_m_wr_data_type_str)/sizeof(ibv_m_wr_data_type_str[0]); i++) {
		if (!strcmp(data_type_str, ibv_m_wr_data_type_str[i].str))
			break;
	}

	return i;
}

static inline void ibv_m_print_calc_op(void)
{
	int i;

	for (i = 0; i < IBV_M_CALC_OP_INVALID; i++)
		printf("\t%s\n", ibv_m_wr_calc_op_str[i]);
}

static inline const char *ibv_m_calc_op_to_str(enum ibv_m_wr_calc_op calc_op)
{
	if (calc_op < sizeof(ibv_m_wr_calc_op_str)/sizeof(ibv_m_wr_calc_op_str[0]))
		return ibv_m_wr_calc_op_str[calc_op];

	return "INVALID OPERATION OPCODE";

}

static inline enum ibv_m_wr_calc_op ibv_m_str_to_calc_op(const char *calc_op)
{
	int i;

	for (i = 0; i < sizeof(ibv_m_wr_calc_op_str)/sizeof(ibv_m_wr_calc_op_str[0]); i++) {
		if (!strcmp(calc_op, ibv_m_wr_calc_op_str[i]))
			break;
	}

	return i;
}

static inline void ibv_m_print_dev_calc_ops(struct ibv_context	*context)
{
	int i, j, flag, supp;

	for (i = 0; i < IBV_M_CALC_OP_INVALID; i++) {
		flag = 0;

		for (j = 0; j < IBV_M_DATA_TYPE_INVALID; j++) {
			supp = ibv_m_query_calc_cap(context, i, j, NULL, NULL);

			if (!supp) {
				if (!flag) {
					printf("\t%s:\n", ibv_m_calc_op_to_str(i));
					flag = 1;
				}

				printf("\t\t%s\n", ibv_m_data_type_to_str(j));
			}
		}
	}
}

static inline enum ibv_mtu pp_mtu_to_enum(int mtu)
{
	switch (mtu) {
	case 256:  return IBV_MTU_256;
	case 512:  return IBV_MTU_512;
	case 1024: return IBV_MTU_1024;
	case 2048: return IBV_MTU_2048;
	case 4096: return IBV_MTU_4096;
	default:   return -1;
	}
}

static inline uint16_t pp_get_local_lid(struct ibv_context *context, int port)
{
	struct ibv_port_attr attr;

	if (ibv_query_port(context, port, &attr))
		return 0;

	return attr.lid;
}

#endif /* IBV_MVERBS_TEST_H */
