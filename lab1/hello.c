#include <stdio.h>
#include <mpi.h>

int main (int argc, char **argv)
{
  int world_rank, world_size;
    double start_time, end_time;

  MPI_Init (&argc, &argv);

  MPI_Comm_size (MPI_COMM_WORLD, &world_size);

  MPI_Comm_rank (MPI_COMM_WORLD, &world_rank);

  printf ("Hello, I am %d of %d processors!\n", world_rank, world_size);
    

    int number;
    
    if (world_rank == 0) {
        number = -1;
        MPI_Send(&number, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);
    } else if (world_rank == 1){
        MPI_Recv(&number, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("Process 1 received number %d from process 0.", number);
    }

  MPI_Finalize ();
  exit (0);
}
