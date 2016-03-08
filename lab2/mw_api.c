#include<stdio.h>
#include<stdlib.h>
#include<mpi.h>
#include "mw_api.h"


#define TAG_TERM 0
#define TAG_WORK 1

#define MASTER_RESULT_LENGTH 1000

void master(int argc, char **argv, int n_proc, struct mw_api_spec *f);
void slave(int argc, char **argv, int n_proc, struct mw_api_spec *f);


void killAllWorkers(int n_proc) {
    MPI_Status status;
    int index;
    char end = 'e';

    for (index = 1; index < n_proc; ++index) {
        MPI_Send(&end, 1, MPI_BYTE, index, TAG_TERM, MPI_COMM_WORLD);
    }

}

void MW_Run (int argc, char **argv, struct mw_api_spec *f) {
    int myid, n_proc;

    MPI_Comm_size(MPI_COMM_WORLD, &n_proc);
    MPI_Comm_rank(MPI_COMM_WORLD, &myid);


    if (myid == 0) {
        master(argc, argv, n_proc, f);
    } else {
        slave(argc, argv, n_proc, f);
    }

    MPI_Finalize();
}


void master(int argc, char **argv, int n_proc, struct mw_api_spec *f) {

    MPI_Status status;

    mw_work_t **work_list = f->create(argc, argv);
    if (NULL == work_list) {
        printf("work creation failed!");
        return;
    }

    //init work list and result list
    mw_work_t **mw_work_ptr = work_list;

    mw_result_t **result_list = (mw_result_t **)malloc(MASTER_RESULT_LENGTH * (sizeof(mw_result_t *)));
    mw_result_t **mw_result_ptr = result_list;

    // start sending to each slave node
    int rank, need_receive = 0, res_count = 0;
    for (rank = 1; rank < n_proc; ++rank) {
        MPI_Send(*mw_work_ptr, f->work_sz, MPI_BYTE, rank, TAG_WORK, MPI_COMM_WORLD);
        mw_work_ptr += 1;
        ++need_receive;

        if (NULL == *mw_work_ptr) {
            break;
        }
    }

    //if there are extra works, keep receiving and sending
    while (NULL != *mw_work_ptr) {

        *mw_result_ptr = (mw_result_t *) malloc(f->res_sz);
        MPI_Recv(*mw_result_ptr, f->res_sz, MPI_BYTE, MPI_ANY_SOURCE, TAG_WORK, MPI_COMM_WORLD, &status);
        mw_result_ptr += 1;
        ++res_count;

        MPI_Send(*mw_work_ptr, f->work_sz, MPI_BYTE, status.MPI_SOURCE, TAG_WORK, MPI_COMM_WORLD);
        mw_work_ptr += 1; // since receive then send, no need to increment need_receive
    }


    // receive results from slave nodes
    while (need_receive > 0) {
        *mw_result_ptr = (mw_result_t *) malloc(f->res_sz);
        MPI_Recv(*mw_result_ptr, f->res_sz, MPI_BYTE, MPI_ANY_SOURCE, TAG_WORK, MPI_COMM_WORLD, &status);
        mw_result_ptr += 1;
        ++res_count;
        --need_receive;
    }

    killAllWorkers(n_proc);
    f->result(res_count, result_list);
    free(result_list);
}

void slave(int argc, char **argv, int n_proc, struct mw_api_spec *f) {
    MPI_Status status;

    mw_work_t *sw_work = malloc(f->work_sz);

    while (1) {
        MPI_Recv(sw_work, f->work_sz, MPI_BYTE, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        if (status.MPI_TAG == TAG_TERM) {
            break;
        }

        mw_result_t* result = f->compute(sw_work);
        MPI_Send(result, f->res_sz, MPI_BYTE, 0, TAG_WORK, MPI_COMM_WORLD);
    }

    free(sw_work);
}

