#include<stdio.h>
#include<stdlib.h>
#include <time.h>
#include<mpi.h>
#include <string.h>
#include "mw_api_master_robust.h"

#define TAG_TERM 0
#define TAG_WORK_START 2
#define DEAD 1
#define LIVE 0
#define OCCUPIED 2

#define MASTER_RESULT_LENGTH 1000
#define THRESHOLD_P 0.1
#define TIMEOUT 0.01

void master(int argc, char **argv, int n_proc, struct mw_api_spec *f);
void slave(int argc, char **argv, int n_proc, struct mw_api_spec *f);
void backup_master(int argc, char **argv, int n_proc, struct mw_api_spec *f);
int F_Isend(void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request* request);
int F_send(void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm);
int isMasterAlive(time_t timeout, mw_result_t* pos_result, int res_sz, MPI_Status* status);
int pick_next_dest(int* worker_status, int size);
void killAllWorkers(int n_proc);

static int random_fail() {
    double num = (double)rand() / (double)RAND_MAX;

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

int F_Isend(void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request* request)
{
    if (random_fail()) {
        //printf("process failure with tag %d that send to %d\n", tag, dest);
        MPI_Finalize();
        exit (0);
        return 0;

    } else {

        return MPI_Isend (buf, count, datatype, dest, tag, comm, request);
    }
}

int isMasterAlive(time_t timeout, mw_result_t* pos_result, int res_sz, MPI_Status* status) {
    MPI_Request request;
    int flag = 0;

    MPI_Irecv(pos_result,
              res_sz,
              MPI_BYTE,
              0,
              MPI_ANY_TAG,
              MPI_COMM_WORLD,
              &request);

    time_t base_time = time(NULL);

    while (time(NULL) < base_time + timeout) {
        MPI_Test(&request, &flag, status);
        if (flag) {
            break;
        }
    }

    //printf("%d\n", flag);
    // master might be dead if flag == 0
    if(!flag){
        printf("master is likely failed, backup master now take over!\n");
    }

    return flag;
}


int pick_next_dest(int* failed_q, int size) {
    int ptr = 2;

    while (ptr < size) {
        if ( failed_q[ptr] == LIVE) {
            return ptr;
        }

        ++ptr;
    }

    //printf("All workers are failed");
//    MPI_Finalize();
//    exit(1);
    return -1;
}
//
//void killAllWorkers(int n_proc) {
//    MPI_Status status;
//    MPI_Request request;
//
//    int index;
//    char end = 'e';
//
//    for (index = 1; index < n_proc; ++index) {
//        MPI_Isend(&end, 1, MPI_BYTE, index, TAG_TERM,
//                  MPI_COMM_WORLD, &request);
//    }
//
//}

void MW_Run (int argc, char **argv, struct mw_api_spec *f) {
    int myid, n_proc;

    MPI_Comm_size(MPI_COMM_WORLD, &n_proc);
    MPI_Comm_rank(MPI_COMM_WORLD, &myid);

    if (myid == 0) {
        master(argc, argv, n_proc, f);
    } else if (myid == 1) {
        backup_master(argc, argv, n_proc, f);
    } else {
        slave(argc, argv, n_proc, f);
    }

    MPI_Finalize();
}


void master(int argc, char **argv, int n_proc, struct mw_api_spec *f) {

    MPI_Status status;
    MPI_Request rrequest, dummy_request;
    int flag = 0;
    srand(time(NULL) * getpid());
    //MPI_Finalize();
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

    mw_result_t **result_list = (mw_result_t **)malloc(MASTER_RESULT_LENGTH * (sizeof(mw_result_t *)));
    mw_result_t **mw_result_ptr = result_list;

    // start sending to each slave node
    int rank, work_done = 0;
    int work_send = 0;
    int resend = 0;

    for (rank = 2; rank < n_proc; ++rank) {
        F_Isend(*(work_list + work_send), f->work_sz,
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
                //printf("received result %d from worker %d\n", res_id, status.MPI_SOURCE);
                *mw_result_ptr = (mw_result_t *) malloc(f->res_sz);
                memcpy(*mw_result_ptr, pos_result, f->res_sz);
                mw_result_ptr++;
                // send data to back up master in case master down
                F_Isend(pos_result, f->res_sz,
                       MPI_BYTE, 1,
                       status.MPI_TAG, MPI_COMM_WORLD, &dummy_request);

                ++work_done;
                result_received[res_id] = 1;
                worker_status[status.MPI_SOURCE] = LIVE;

                if (work_send < f->work_length) {
                    F_Isend(*(work_list + work_send), f->work_sz,
                             MPI_BYTE, status.MPI_SOURCE,
                             TAG_WORK_START + work_send, MPI_COMM_WORLD, &dummy_request);

                    work_workers[work_send] = status.MPI_SOURCE;
                    worker_status[status.MPI_SOURCE] = OCCUPIED;
                    work_send++;
                }
                break;
            }

        }

        // if after time out, no worker responds, check whether all dead
        if (!flag) {
            MPI_Cancel(&rrequest);
            int i, dead = 0;

            for (i = 0; i < f->work_length; ++i) {
                if (!result_received[i] && (work_workers[i] != 0)) {
                    worker_status[work_workers[i]] = DEAD;
                    ++dead;
                }
            }

            if (dead >= n_proc - 2) {
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

                    F_Isend(*(work_list + i), f->work_sz,
                              MPI_BYTE, liveWorker,
                              TAG_WORK_START + i, MPI_COMM_WORLD,
                              &dummy_request);
                    work_workers[i] = liveWorker;
                    worker_status[liveWorker] = OCCUPIED;
                    resend++;
                }
            }

        }


    }

    //killAllWorkers(n_proc);
    f->result(f->work_length, result_list);
    printf("%d resend happens\n", resend);
    free(result_list);
    free(pos_result);
    free(result_received);
    free(work_workers);
    free(worker_status);
    MPI_Finalize();
    //MPI_Abort(MPI_COMM_WORLD,0);
}


void backup_master(int argc, char **argv, int n_proc, struct mw_api_spec *f) {

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

    mw_result_t **result_list = (mw_result_t **)malloc(MASTER_RESULT_LENGTH * (sizeof(mw_result_t *)));
    mw_result_t **mw_result_ptr = result_list;

    // start sending to each slave node
    int rank, work_done = 0;
    int work_send = 0;
    int takeOver = 0;


    while (work_done < f->work_length) {
//        if (status.MPI_TAG == TAG_TERM) {
//            break;
//        }

        if (isMasterAlive(2 * TIMEOUT, pos_result, f->res_sz, &status)) {

            int res_id = status.MPI_TAG - TAG_WORK_START;
            result_received[res_id] = 1;
            //printf("backup master get result from %d\n", res_id);

            if (!result_received[res_id]) {
                *mw_result_ptr = (mw_result_t *) malloc(f->res_sz);
                memcpy(*mw_result_ptr, pos_result, f->res_sz);
                mw_result_ptr++;
                ++work_done;
            }
        } else { // master is dead, take over as real master

            takeOver = 1;
            break;
        }

    }

    // if not take over, hung up myself
    if (!takeOver) {
        return;
    }

    int i;
    for (i = 0; i < f->work_length; ++i) {
        if (!result_received[i]) {
            int aliveWorker = pick_next_dest(worker_status, n_proc);
            if (aliveWorker == -1) {
                break;
            }

            MPI_Isend(*(work_list + i), f->work_sz,
                    MPI_BYTE, aliveWorker,
                    TAG_WORK_START + i, MPI_COMM_WORLD,
                    &dummy_request);
            worker_status[aliveWorker] = OCCUPIED;
            work_workers[i] = aliveWorker;

        }
    }

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
                if (!result_received[res_id]) {
                    *mw_result_ptr = (mw_result_t *) malloc(f->res_sz);
                    memcpy(*mw_result_ptr, pos_result, f->res_sz);
                    mw_result_ptr++;
                    ++work_done;
                    result_received[res_id] = 1;
                }
                worker_status[status.MPI_SOURCE] = LIVE;
                break;
            }

        }

        if (!flag) {
            MPI_Cancel(&rrequest);


            for (i = 0; i < f->work_length; ++i) {
                //printf("%d::%d::%d  ", i, result_received[i], work_workers[i]);
                if (!result_received[i] && (work_workers[i] != 0)) {
                    if (worker_status[work_workers[i]] != DEAD) {
                        ++dead;
                        worker_status[work_workers[i]] = DEAD;
                    }
                }
            }

            //printf("%d\n", dead);
            if (dead >= n_proc - 2) {
                printf("All dead:(");
                MPI_Finalize();
                exit(1);

            }

            for (i = 0; i < f->work_length; ++i) {
                if (!result_received[i]) {
                    int aliveWorker = pick_next_dest(worker_status, n_proc);
                    if (aliveWorker == -1) {
                        break;
                    }

                    MPI_Isend(*(work_list + i), f->work_sz,
                            MPI_BYTE, aliveWorker,
                            TAG_WORK_START + i, MPI_COMM_WORLD,
                            &dummy_request);
                    work_workers[i] = aliveWorker;
                    worker_status[aliveWorker] = OCCUPIED;
                }

            }
        }
    }


    // if take over, need to wrap up the computation


    //killAllWorkers(n_proc);
    f->result(f->work_length, result_list);


    free(result_list);
    free(pos_result);
    free(result_received);
    free(work_workers);
    free(worker_status);

    //MPI_Finalize();
    MPI_Abort(MPI_COMM_WORLD,0);
}

void slave(int argc, char **argv, int n_proc, struct mw_api_spec *f) {

    MPI_Status status, send_status;
    MPI_Request request;
    mw_work_t *sw_work = malloc(f->work_sz);
    srand(time(NULL) * getpid());
    int flag = 0;

    while (1) {
        MPI_Recv(sw_work, f->work_sz,
                 MPI_BYTE, MPI_ANY_SOURCE,
                 MPI_ANY_TAG, MPI_COMM_WORLD,
                 &status);

//        if (status.MPI_TAG == TAG_TERM) {
//            break;
//        }

        mw_result_t* result = f->compute(sw_work);
        F_Isend(result, f->res_sz,
               MPI_BYTE, status.MPI_SOURCE,
                status.MPI_TAG,
                MPI_COMM_WORLD, &request);

        time_t base_time = time(NULL);
        while(time(NULL) < base_time + TIMEOUT){

            MPI_Test(&request, &flag, &send_status);

            if(flag){
                break;

            }
        }

        if(!flag){
            MPI_Cancel(&request);
        }
    }


    free(sw_work);
}



