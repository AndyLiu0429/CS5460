#include <stdio.h>
#include <mpi.h>

#define TRIALS          10
#define START           0
#define FINISH          1000
#define REPS            100
#define MSG_SIZE_INCR            100
#define MAXSIZE         10000
#define COMP_TRIALS     1000
#define COMP_REPS       1000000


int main(int argc, char **argv)
{
	
    int    sz, myid, msgsize, this, index, k, msgsizes[MAXSIZE], dest, src, tag = 998;
    float  bandwidth, nbytes, comp, results_bw[MAXSIZE], results_lat[MAXSIZE];
    double startTime, endTime, ttime, avg_time;
    char   msgbuff[FINISH];
    MPI_Status status;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &sz);
    MPI_Comm_rank(MPI_COMM_WORLD, &myid);
    
    if (sz / 2 * 2 != sz) {
        printf("Even processors please..\n");
        MPI_Abort(MPI_COMM_WORLD, statusCode);
        exit(0);
    }

    dest = src = myid < sz/2 ? (myid + sz/2) : (myid - sz/2);
    printf("myid: %d dest: %d src: %d\n", myid, dest, src);

    this = 0;
    for (msgsize = START; msgsize <= FINISH; msgsize += MSG_SIZE_INCR) {
        msgsizes[this] = msgsize;
        results_bw[this] = 0.0;
        results_lat[this] = 0.0;
        ++this;
    }
	
    for (index = 0; index < FINISH; ++index) {
        msgbuff[index] = 'x';
    }
    
    // wait for each processor initiation
    MPI_Barrier(MPI_COMM_WORLD);
	
    this = 0;
    // first half send msg
    if (myid < sz/2) {
 
    // try for different message size
    for (msgsize = START; msgsize <= FINISH; msgsize += MSG_SIZE_INCR) {
        startTime = MPI_Wtime();
        
        // repeat multiple times to get stable result
        for (index = 1; index <= REPS; ++index) {
            MPI_Send(msgbuff, msgsize, MPI_CHAR, dest, tag, MPI_COMM_WORLD);
            MPI_Recv(msgbuff, msgsize, MPI_CHAR, src, tag, MPI_COMM_WORLD, &status);
        }
        
        endTime = MPI_Wtime();
        ttime = endTime - startTime;
        /* Compute latency of one way trip*/
        /*using microseconds because seconds too small*/
        avg_time = ttime * 1000000.0 / REPS;
        
        // get worst latency
        if (results_lat[this] < (avg_time / 2.0)) {
            results_lat[this] = avg_time / 2.0;
        }
	  
        /* Compute bandwidth */
        nbytes = sizeof(char) * msgsize;
        bandwidth = nbytes / avg_time;
        
        // get largest bandwidth
        if (results_bw[this] < bandwidth) {
            results_bw[this] = bandwidth;
        }
	
        ++this;
  }
}       

    // second half receive msg
    if (myid >= sz/2) {
  
        for (msgsize = START; msgsize <= FINISH; msgsize += MSG_SIZE_INCR) {
            for (index = 1; index <= REPS; ++index){
                MPI_Recv(msgbuff, msgsize, MPI_CHAR, src, tag, MPI_COMM_WORLD, &status);
                MPI_Send(msgbuff, msgsize, MPI_CHAR, dest, tag, MPI_COMM_WORLD);
            }
        }
    
    }

    this = 0;
    
    if (myid == 0) {
    printf("Message Size | Latency | Bandwidth \n");
    for (msgsize = START; msgsize <= FINISH; msgsize += MSG_SIZE_INCR) {
        printf("%9d 	%4.2f 	%16d\n", msgsizes[this], results_lat[this], (int) results_bw[this]);
        ++this;
        }
    }
    
    MPI_Finalize();
    exit(0);
    
}
