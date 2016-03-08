#include <stdio.h>
#include <mpi.h>

#define SIZE 100000

int main(int argc, char **argv)
{
    int myid, sz;
    int statusCode;
    int index;
    double startTime, endTime;
    float dot_sum, local_dot_sum;
    MPI_Status status;
    
    
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &sz);
    MPI_Comm_rank(MPI_COMM_WORLD, &myid);
    
    if (sz / 2 * 2 != sz) {
        printf("Even processors please..\n");
        MPI_Abort(MPI_COMM_WORLD, statusCode);
        exit(0);
    }
    
    float array1[SIZE], array2[SIZE];
    for (index = 0; index < SIZE; ++index) {
        array1[index] = 1.0;
        array2[index] = 1.0;
    }
    
    int chunk = SIZE / sz;
    float local_x[chunk], local_y[chunk];
    
    MPI_Barrier(MPI_COMM_WORLD); // wait for each processor init float array
    
    // root starts counting
    if (myid == 0) {
        startTime = MPI_Wtime();
    }
    
    // transfer part of the array to different processors
    MPI_Scatter(array1, chunk, MPI_FLOAT, local_x, chunk, MPI_FLOAT, 0, MPI_COMM_WORLD);
    MPI_Scatter(array2, chunk, MPI_FLOAT, local_y, chunk, MPI_FLOAT, 0, MPI_COMM_WORLD);
    
    // calculate part of dot sum
    for (index = 0; index < chunk; ++index) {
        local_dot_sum += local_x[index] * local_y[index];
    }
    
    // reduce to get final sum
    MPI_Reduce(&local_dot_sum, &dot_sum, 1, MPI_FLOAT, MPI_SUM, 0, MPI_COMM_WORLD);
    
    if (myid == 0) {
        endTime = MPI_Wtime();
        double ct = (endTime - startTime) * 1000000000.0 / SIZE;
        printf("Computation Time per Floating Operation: %4.2f ns\n", ct);
    }
    
    MPI_Finalize();
    exit(0);
    
}
