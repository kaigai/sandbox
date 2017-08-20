#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cuda.h>
#include <pthread.h>

extern int kgscan_image[];
extern int kds_src_image[];
extern int kds_dst_image[];
extern int prog_image[];
extern size_t kgscan_length;
extern size_t kds_src_length;
extern size_t kds_dst_length;
extern size_t prog_length;

static CUdevice		cuda_device;
static CUcontext	cuda_context;
static CUmodule		cuda_module;
static CUfunction	cuda_function;
static int			nloops = 20;
static void		   *kds_src_dmabuf;

static inline void
error_exit(const char *func_name, CUresult errcode)
{
	const char     *error_name;
	const char     *error_string;

	if (cuGetErrorName(errcode, &error_name) != CUDA_SUCCESS)
		error_name = NULL;
	if (cuGetErrorString(errcode, &error_string) != CUDA_SUCCESS)
		error_string = "????";

	if (error_name)
		fprintf(stderr, "failed on %s: %s - %s\n",
				func_name, error_name, error_string);
	else
		fprintf(stderr, "failed on %s: %d - %s\n",
				func_name, errcode, error_string);
	exit(1);
}

static void *
worker_main(void *private)
{
	CUdeviceptr	m_kgscan;
	CUdeviceptr	m_kds_src;
	CUdeviceptr	m_kds_dst;
	CUstream	cuda_stream = CU_STREAM_PER_THREAD;
	CUevent		cuda_event;
	CUresult	rc;
	size_t		bytesize = 128UL << 20;
	void	   *kern_args[4];
	int			i;

	rc = cuCtxSetCurrent(cuda_context);
	if (rc != CUDA_SUCCESS)
		error_exit("cuCtxSetCurrent", rc);

	rc = cuEventCreate(&cuda_event, (CU_EVENT_BLOCKING_SYNC |
									 CU_EVENT_DISABLE_TIMING));
	if (rc != CUDA_SUCCESS)
		error_exit("cuEventCreate", rc);

	rc = cuMemAlloc(&m_kds_src, kds_src_length);
	if (rc != CUDA_SUCCESS)
		error_exit("cuMemAlloc", rc);

	rc = cuMemAllocManaged(&m_kgscan, bytesize, CU_MEM_ATTACH_GLOBAL);
	if (rc != CUDA_SUCCESS)
		error_exit("cuMemAllocManaged", rc);

	rc = cuMemAdvise(m_kgscan, bytesize,
					 CU_MEM_ADVISE_SET_PREFERRED_LOCATION,
					 0);
	if (rc != CUDA_SUCCESS)
		error_exit("cuMemAdvise", rc);

	rc = cuMemAdvise(m_kgscan, bytesize,
					 CU_MEM_ADVISE_SET_ACCESSED_BY,
					 0);
	if (rc != CUDA_SUCCESS)
		error_exit("cuMemAdvise", rc);
	m_kds_dst = m_kgscan + (64UL << 20);

	for (i=0; i < nloops; i++)
	{
		memcpy((void *)m_kgscan, kgscan_image, kgscan_length);
		rc = cuMemPrefetchAsync(m_kgscan, kgscan_length,
								cuda_device,
								cuda_stream);
		if (rc != CUDA_SUCCESS)
			error_exit("cuMemPrefetchAsync", rc);

		rc = cuMemcpyHtoDAsync(m_kds_src,
							   kds_src_dmabuf,
							   kds_src_length,
							   cuda_stream);
		if (rc != CUDA_SUCCESS)
			error_exit("cuMemcpyHtoDAsync", rc);

		memcpy((void *)m_kds_dst, kds_dst_image, kds_dst_length);
		rc = cuMemPrefetchAsync(m_kds_dst, kds_dst_length,
								cuda_device,
								cuda_stream);
		if (rc != CUDA_SUCCESS)
			error_exit("cuMemPrefetchAsync", rc);

		kern_args[0] = &m_kgscan;
		kern_args[1] = &m_kds_src;
		kern_args[2] = &m_kds_dst;

		rc = cuLaunchKernel(cuda_function,
							1, 1, 1,
							1, 1, 1,
							0,
							cuda_stream,
							kern_args,
							NULL);
		if (rc != CUDA_SUCCESS)
			error_exit("cuLaunchKernel", rc);

		rc = cuEventRecord(cuda_event, cuda_stream);
		if (rc != CUDA_SUCCESS)
			error_exit("cuEventRecord", rc);

		/* point of synchronization */
		rc = cuEventSynchronize(cuda_event);
		if (rc != CUDA_SUCCESS)
			error_exit("cuEventSynchronize", rc);
		memset((void *)m_kgscan, -1, kgscan_length);
	}
	return NULL;
}

int main(int argc, const char *argv[])
{
	int			i, nthreads = 16;
	pthread_t  *thread_ids;
	CUresult	rc;

	if (argc > 1)
		nthreads = atoi(argv[1]);
	if (argc > 2)
		nloops = atoi(argv[2]);
	thread_ids = calloc(nthreads, sizeof(pthread_t));

	rc = cuInit(0);
	if (rc != CUDA_SUCCESS)
		error_exit("cuInit", rc);

	rc = cuDeviceGet(&cuda_device, 0);
	if (rc != CUDA_SUCCESS)
		error_exit("cuDeviceGet", rc);

	rc = cuCtxCreate(&cuda_context, CU_CTX_SCHED_AUTO, cuda_device);
	if (rc != CUDA_SUCCESS)
		error_exit("cuCtxCreate", rc);

	rc = cuModuleLoadData(&cuda_module, prog_image);
	if (rc != CUDA_SUCCESS)
		error_exit("cuModuleLoadData", rc);

	rc = cuModuleGetFunction(&cuda_function, cuda_module,
							 "gpuscan_main");
	if (rc != CUDA_SUCCESS)
		error_exit("cuModuleGetFunction", rc);

	rc = cuMemAllocHost(&kds_src_dmabuf, kds_src_length);
	if (rc != CUDA_SUCCESS)
		error_exit("cuMemAllocHost", rc);
	memcpy(kds_src_dmabuf, kds_src_image, kds_src_length);

	for (i=0; i < nthreads; i++)
	{
		if (pthread_create(&thread_ids[i], NULL,
						   worker_main, (void *)((long)i)) != 0)
		{
			fprintf(stderr, "failed on pthread_create\n");
			exit(1);
		}
	}

	for (i=0; i < nthreads; i++)
		pthread_join(thread_ids[i], NULL);

	return 0;
}
