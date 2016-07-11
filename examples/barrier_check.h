/*
 * Copyright (c) 2013 Mellanox Technologies.  All rights reserved.
 */

#ifndef __BARRIER_CHECK_H
#define __BARRIER_CHECK_H

typedef int (*barrier_fn_t)(void*);

static int __barrier_check( void *context, barrier_fn_t barrier_fn )
{
    int rc = 0;
    struct cc_context *ctx = context;
    int num_proc = ctx->conf.num_proc;
    int my_proc = ctx->conf.my_proc;
    int i;


    srand(my_proc*time(NULL));
    usleep(rand() % 1000000);

    for (i=0; i<num_proc; i++) {
        if (my_proc == i) {
            fprintf(stderr,"barrier check, rank %d\n",my_proc);
            usleep(10000);
        }
        barrier_fn(context);
    }

    return rc;
}

#endif
