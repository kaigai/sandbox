extern "C" {

#include <stdio.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define BUFFER_LEN		100

__global__ static void
uvm_shmem_update(char *buffer)
{
	if (buffer[threadIdx.x])
	{
		printf("buffer[%d] is not zero\n", threadIdx.x);
	}
}

__host__ static int child_process(char *buffer)
{
	int		i, loop;

	for (loop=0; loop < 4; loop++)
	{
		for (i=0; i < BUFFER_LEN; i++)
		{
			if (i % 10 == loop)
				buffer[i] = 1;
			if (i % 10 >= 4 && buffer[i])
				printf("device set buffer[%d]\n", i);
		}
		sleep(2);
	}
	puts("child process exit");
	return 0;
}

int main(int argc, const char *argv[])
{
	int			shmid;
	char	   *buffer;
	char	   *dev_buf;
	int			i, status;
	pid_t		child;
	cudaError_t	rc;

	/* create a shared memory region */
	shmid = shmget(IPC_PRIVATE, sizeof(int) * BUFFER_LEN, IPC_CREAT | 0600);
	if (shmid < 0)
	{
		printf("failed on shmget : %m\n");
		return 1;
	}
	/* attach shared memory */
	buffer = (char *)shmat(shmid, NULL, 0);
	if (buffer == (void *)-1)
	{
		printf("failed on shmat : %m\n");
		return 1;
	}

	/* initialize */
	memset(buffer, 0, BUFFER_LEN);

	/* fork a child process */
	child = fork();
	if (child == 0)
		return child_process(buffer);
	else if (child < 0)
	{
		printf("failed on fork(2) : %m\n");
		return 1;
	}

	/* shmem as host registered memory */
	rc = cudaHostRegister(buffer, sizeof(int) * BUFFER_LEN,
						  cudaHostRegisterPortable |
						  cudaHostRegisterMapped);
	if (rc != cudaSuccess)
	{
		printf("failed on cudaHostRegister: %s\n",
			   cudaGetErrorString(rc));
		return 1;
	}
	rc = cudaHostGetDevicePointer(&dev_buf, buffer, 0);
	if (rc != cudaSuccess)
	{
		printf("failed on cudaHostGetDevicePointer: %s\n",
			   cudaGetErrorString(rc));
		return 1;
	}

	for (i=1; i <= 10; i++)
	{
		/* kernel launch */
		printf("====== %dth kernel call ======\n", i);

		uvm_shmem_update<<<1, BUFFER_LEN>>>(dev_buf);

		rc = cudaDeviceSynchronize();
		if (rc != cudaSuccess)
		{
			printf("failed on cudaDeviceSynchronize: %s\n",
				   cudaGetErrorString(rc));
			return 1;
		}
		sleep(1);
	}
	puts("exits device kernel call");
	waitpid(child, &status, 0);
	puts("exist parent kernel");
	return 0;
}

}
