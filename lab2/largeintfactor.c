#include<stdlib.h>
#include<stdio.h>
#include<time.h>
#include<string.h>
#include<gmp.h>
#include<mpi.h>
#include "mw_api.h"

#define MAX_LONGINT_LENGTH 200

struct userdef_work_t {
    char dividend[MAX_LONGINT_LENGTH];
    char divisor[MAX_LONGINT_LENGTH];
    int chunk_size;
};

struct userdef_result_t {
    char divisor[MAX_LONGINT_LENGTH];
    int offsets[1000]; //stores the factors, fair enough guess for possible factor numbers in the range
    int nums; // factor numbers
};

mw_work_t **create_work(int argc, char** argv) {

    mpz_t dividend, divisor, chunk_sz_long, dividend_root, dv_q, dv_r;

    mpz_init_set_str(dividend, argv[1], 10);
    mpz_init_set_str(chunk_sz_long, argv[2], 10);
    mpz_init_set_ui(divisor, 1);

    mpz_init(dividend_root);
    mpz_init(dv_q);
    mpz_init(dv_r);

    mpz_sqrt(dividend_root, dividend);
    mpz_cdiv_qr(dv_q, dv_r, dividend_root, chunk_sz_long);

    int work_cnt = mpz_get_ui(dv_q) + 1; // need to consider last chunk
    int chunk_size = mpz_get_ui(chunk_sz_long);

    mw_work_t** work_list = (mw_work_t **) malloc((work_cnt + 1) * sizeof(mw_work_t *));
    work_list[work_cnt] = NULL;

    int i;
    for (i = 0; i < work_cnt; i++) {
        *(work_list + i) = (mw_work_t *) malloc(sizeof(mw_work_t));

        mpz_get_str((*(work_list + i))->dividend, 10, dividend);
        mpz_get_str((*(work_list + i))->divisor, 10, divisor);
        (*(work_list + i))->chunk_size = chunk_size;
        mpz_add_ui(divisor, divisor, chunk_size);
    }

    return work_list;
}

int process_results(int sz, mw_result_t **res) {
    mpz_t divisor, factor;

    mpz_init(factor);
    int i, j, total_nums = 0;


    for (i = 0; i < sz; i++) {
        total_nums += (*res)->nums;
        mpz_init_set_str(divisor, (*res)->divisor, 10);
        for (j = 0; j < (*res)->nums; j++) {
            mpz_add_ui(factor, divisor, (*res)->offsets[j]);
            gmp_printf("%Zd.\n", factor);
        }

        res += 1;
    }

    printf("total factor numbers: %d.\n", total_nums);

    return 1;
}

mw_result_t* do_work(mw_work_t *work) {
    mpz_t divisor, dividend;

    mw_result_t *res_t = malloc(sizeof(mw_result_t));
    int factor_nums = 0;
    memcpy(res_t->divisor, work->divisor, MAX_LONGINT_LENGTH *sizeof(char));

    mpz_init_set_str(divisor, work->divisor, 10);
    mpz_init_set_str(dividend, work->dividend, 10);

    long i;
    for (i = 0; i < work->chunk_size; i++) {
        if (mpz_divisible_p(dividend, divisor)) {
            res_t->offsets[factor_nums++] = i;
        }
        mpz_add_ui(divisor, divisor, 1);
    }

    res_t->nums = factor_nums;

    return res_t;
}


int main (int argc, char **argv)
{
    struct mw_api_spec f;


    MPI_Init (&argc, &argv);


    f.create = create_work;
    f.result = process_results;
    f.compute = do_work;
    f.work_sz = sizeof (struct userdef_work_t);
    f.res_sz = sizeof (struct userdef_result_t);

    MW_Run (argc, argv, &f);


    return 0;

}
