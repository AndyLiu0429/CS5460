#include<stdlib.h>
#include<stdio.h>
#include<time.h>
#include<string.h>
#include<mpi.h>
#include "mw_api_master_robust.h"


#define LENGTH 1000
#define CHUNK_SIZE 100

struct userdef_work_t {
    float vec1[CHUNK_SIZE];
    float vec2[CHUNK_SIZE];
};

struct userdef_result_t {
    float chunk_res;
};

void gen_random_vector(int len, float vector[]) {
    int i;

    for (i = 0; i < len; i++) {
        //vector[i] = drand48();
        vector[i] = 1;
    }
}


mw_work_t **create_work(int argc, char** argv) {

    float vector1[LENGTH], vector2[LENGTH];

    gen_random_vector(LENGTH, vector1);
    gen_random_vector(LENGTH, vector2);

    int work_cnt = LENGTH / CHUNK_SIZE;
    mw_work_t** work_list = (mw_work_t**) malloc((work_cnt+1)*sizeof(mw_work_t *));
    work_list[work_cnt] = NULL; // NULL-terminated list

    int i;
    for (i = 0; i < work_cnt; i++) {
        *(work_list + i) = (mw_work_t*) malloc(sizeof(mw_work_t));

        memcpy((*(work_list + i))->vec1,vector1 + i * CHUNK_SIZE, CHUNK_SIZE * sizeof(float));
        memcpy((*(work_list + i))->vec2,vector2 + i * CHUNK_SIZE, CHUNK_SIZE * sizeof(float));

    }

    return work_list;
}

int process_results(int sz, mw_result_t **res) {
    float sum = 0.0;
    int i;

    for (i = 0; i < sz; i++) {
        //printf("part result is %f\n", (*res)->chunk_res);
        sum += (*res)->chunk_res;
        res += 1;
    }

    printf("The result is %f.\n", sum);
    return 1;
}

mw_result_t* do_work(mw_work_t *work) {
    int i;

    float res = 0.0;
    for (i = 0; i < CHUNK_SIZE; i++) {
        res += (work->vec1)[i] * (work->vec2[i]);
    }

    mw_result_t *res_t = malloc(sizeof(mw_result_t));
    res_t->chunk_res = res;

    return res_t;
}


int main (int argc, char **argv)
{
    struct mw_api_spec f;


    MPI_Init (&argc, &argv);


    f.create = create_work;
    f.result = process_results;
    f.compute = do_work;
    f.work_length = LENGTH / CHUNK_SIZE;
    f.work_sz = sizeof (struct userdef_work_t);
    f.res_sz = sizeof (struct userdef_result_t);

    MW_Run (argc, argv, &f);


    return 0;

}

