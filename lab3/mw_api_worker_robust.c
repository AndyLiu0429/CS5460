#include<stdio.h>
#include<stdlib.h>
#include <time.h>
#include <string.h>
#include<mpi.h>
#include "mw_api_worker_robust.h"

#define TAG_TERM 0
#define TAG_WORK_START 1
#define DEAD 1
#define LIVE 0
#define OCCUPIED 2


#define MASTER_RESULT_LENGTH 1000
#define THRESHOLD_P 0.2
#define TIMEOUT 0.01

void master(int argc, char **argv, int n_proc, struct mw_api_spec *f);
void slave(int argc, char **argv, int n_proc, struct mw_api_spec *f, int id);
int pick_next_dest(int* worker_status, int size);

static int random_fail() {
    double num = (double)rand() / (double)RAND_MAX;
    //printf("%lf\n", num);
    if (num < THRESHOLD_P) {

        return 1;
    } else {
        return 0;
    }
}

int F_Send(void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm)
{
    if (random_fail()) {
        //printf("process failure with tag %d that sent to %d\n", tag, dest);
        MPI_Finalize();
        exit (0);
        return 0;

    } else {
        return MPI_Send (buf, count, datatype, dest, tag, comm);
    }
}



int pick_next_dest(int* worker_status, int size) {
    int ptr = 1;

    while (ptr < size) {
        if (worker_status[ptr] == LIVE) {
            return ptr;
        }

        ++ptr;
    }

    printf("All workers are failed");
    return -1;
}

void killAllWorkers(int n_proc) {
    MPI_Status status;
    MPI_Request request;

    int index;
    char end = 'e';

    for (index = 1; index < n_proc; ++index) {
        MPI_Isend(&end, 1, MPI_BYTE, index, TAG_TERM, MPI_COMM_WORLD, &request);
    }

}

void MW_Run (int argc, char **argv, struct mw_api_spec *f) {
    int myid, n_proc;

    MPI_Comm_size(MPI_COMM_WORLD, &n_proc);
    MPI_Comm_rank(MPI_COMM_WORLD, &myid);


    if (myid == 0) {
        master(argc, argv, n_proc, f);
    } else {
        slave(argc, argv, n_proc, f, myid);
    }

    MPI_Finalize();
}


void master(int argc, char **argv, int n_proc, struct mw_api_spec *f) {

    MPI_Status status;
    MPI_Request rrequest, dummy_request;
    int flag = 0;
    int dead = 0;
    srand(time(NULL) * getpid());

    mw_work_t **work_list = f->create(argc, argv);
    if (NULL == work_list) {
        printf("work creation failed!");
        return;
    }

    // init failure list
    int *result_received = calloc(f->work_length, sizeof(int));
    int *worker_status = calloc(n_proc, sizeof(int));
    int *work_workers = calloc(f->work_length, sizeof(int));

    // init result struct
    mw_result_t *pos_result = malloc(f->res_sz);

    //init result list

    mw_result_t **result_list = (mw_result_t **) malloc(MASTER_RESULT_LENGTH * (sizeof(mw_result_t *)));
    mw_result_t **mw_result_ptr = result_list;

    // start sending to each slave node
    int rank, work_done = 0;
    int work_send = 0;

    // store how many resend happens
    int resend = 0;

    for (rank = 1; rank < n_proc; ++rank) {
        MPI_Isend(*(work_list + work_send), f->work_sz,
                 MPI_BYTE, rank,
                 TAG_WORK_START + work_send, MPI_COMM_WORLD, &dummy_request);

        work_workers[work_send] = rank;
        ++work_send;
        worker_status[rank] = OCCUPIED;

        if (work_send == f->work_length) {
            break;
        }
    }

    // receive results from slave nodes
    while (work_done < f->work_length) {

        MPI_Irecv(pos_result, f->res_sz,
                  MPI_BYTE, MPI_ANY_SOURCE,
                  MPI_ANY_TAG, MPI_COMM_WORLD,
                  &rrequest);

        time_t base_time = time(NULL);

        while (time(NULL) < base_time + TIMEOUT) {
            MPI_Test(&rrequest, &flag, &status);

            if (flag) {

                int res_id = status.MPI_TAG - TAG_WORK_START;
                *mw_result_ptr = (mw_result_t *) malloc(f->res_sz);
                memcpy(*mw_result_ptr, pos_result, f->res_sz);
                mw_result_ptr++;
                ++work_done;
                result_received[res_id] = 1;
                worker_status[status.MPI_SOURCE] = LIVE;

                if (work_send < f->work_length) {
                    MPI_Isend(*(work_list + work_send), f->work_sz,
                             MPI_BYTE, status.MPI_SOURCE,
                             TAG_WORK_START + work_send, MPI_COMM_WORLD, &dummy_request);

                    work_workers[work_send] = status.MPI_SOURCE;
                    work_send++;
                    worker_status[status.MPI_SOURCE] = OCCUPIED;
                }
                break;


            }

        }

        // if after time out, no worker responds, check whether all dead
        if (!flag) {

            MPI_Cancel(&rrequest);
            int i;

            for (i = 0; i < f->work_length; ++i) {
                //printf("%d::%d::%d  ", i, result_received[i], work_workers[i]);
                if (!result_received[i] && (work_workers[i] != 0)) {
                    if (worker_status[work_workers[i]] != DEAD) {
                        worker_status[work_workers[i]] = DEAD;
                        ++dead;
                    }
                }
            }

            if (dead >= n_proc - 1) {
                printf("All dead:(");
                MPI_Finalize();
                exit(1);
            }


            for (i = 0; i < f->work_length; ++i) {
                if (!result_received[i]) {
                    int liveWorker = pick_next_dest(worker_status, n_proc);
                    if (liveWorker == -1) {
                        break;
                    }

                    worker_status[liveWorker] = OCCUPIED;
                    MPI_Isend(*(work_list + i), f->work_sz,
                              MPI_BYTE, liveWorker,
                              TAG_WORK_START + i, MPI_COMM_WORLD,
                              &dummy_request);
                    work_workers[i] = liveWorker;
                    ++resend;
                }
            }

        }


    }

    killAllWorkers(n_proc);
    f->result(f->work_length, result_list);

    printf("%d resend happen\n", resend);

    free(result_list);
    free(pos_result);
    free(result_received);
    free(work_workers);
    free(worker_status);

}

void slave(int argc, char **argv, int n_proc, struct mw_api_spec *f, int id) {
    MPI_Status status;

    mw_work_t *sw_work = malloc(f->work_sz);
    srand(time(NULL) * getpid());

    while (1) {
        MPI_Recv(sw_work, f->work_sz,
                 MPI_BYTE, 0, MPI_ANY_TAG,
                 MPI_COMM_WORLD, &status);
        //printf("worker %d received from %d\n", id, status.MPI_TAG);
        if (status.MPI_TAG == TAG_TERM) {
            break;
        }

        mw_result_t* result = f->compute(sw_work);
        F_Send(result, f->res_sz,
               MPI_BYTE, 0,
               status.MPI_TAG, MPI_COMM_WORLD);
    }

    free(sw_work);
}


