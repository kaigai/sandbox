#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>

#define Max(a,b)		((a) > (b) ? (a) : (b))
#define Min(a,b)		((a) < (b) ? (a) : (b))
#define NREGS			16

static void *memcpy_avx(void *__dst, const void *__src, size_t nbytes)
{
	char	   *dst = __dst;
	const char *src = __src;

	/* AVX if 256bits aligned */
	if (((uintptr_t)src & 0x1f) == 0 && ((uintptr_t)dst & 0x1f) == 0)
	{
		while (nbytes >= NREGS * 0x20)
		{
			/* read */
			asm volatile("vmovdqa %0,%%ymm0" : : "m" (src[0x00]));
#if NREGS > 1
			asm volatile("vmovdqa %0,%%ymm1" : : "m" (src[0x20]));
#endif
#if NREGS > 2
			asm volatile("vmovdqa %0,%%ymm2" : : "m" (src[0x40]));
#endif
#if NREGS > 3
			asm volatile("vmovdqa %0,%%ymm3" : : "m" (src[0x60]));
#endif
#if NREGS > 4
			asm volatile("vmovdqa %0,%%ymm4" : : "m" (src[0x80]));
#endif
#if NREGS > 5
			asm volatile("vmovdqa %0,%%ymm5" : : "m" (src[0xa0]));
#endif
#if NREGS > 6
			asm volatile("vmovdqa %0,%%ymm6" : : "m" (src[0xc0]));
#endif
#if NREGS > 7
			asm volatile("vmovdqa %0,%%ymm7" : : "m" (src[0xe0]));
#endif
#if NREGS > 8
			asm volatile("vmovdqa %0,%%ymm8" : : "m" (src[0x100]));
#endif
#if NREGS > 9
			asm volatile("vmovdqa %0,%%ymm9" : : "m" (src[0x120]));
#endif
#if NREGS > 10
			asm volatile("vmovdqa %0,%%ymm10" : : "m" (src[0x140]));
#endif
#if NREGS > 11
			asm volatile("vmovdqa %0,%%ymm11" : : "m" (src[0x160]));
#endif
#if NREGS > 12
			asm volatile("vmovdqa %0,%%ymm12" : : "m" (src[0x180]));
#endif
#if NREGS > 13
			asm volatile("vmovdqa %0,%%ymm13" : : "m" (src[0x1a0]));
#endif
#if NREGS > 14
			asm volatile("vmovdqa %0,%%ymm14" : : "m" (src[0x1c0]));
#endif
#if NREGS > 15
			asm volatile("vmovdqa %0,%%ymm15" : : "m" (src[0x1e0]));
#endif
			/* write */
			asm volatile("vmovdqa %%ymm0,%0" : : "m" (dst[0x00]));
#if NREGS > 1
			asm volatile("vmovdqa %%ymm1,%0" : : "m" (dst[0x20]));
#endif
#if NREGS > 2
			asm volatile("vmovdqa %%ymm2,%0" : : "m" (dst[0x40]));
#endif
#if NREGS > 3
			asm volatile("vmovdqa %%ymm3,%0" : : "m" (dst[0x60]));
#endif
#if NREGS > 4
			asm volatile("vmovdqa %%ymm4,%0" : : "m" (dst[0x80]));
#endif
#if NREGS > 5
			asm volatile("vmovdqa %%ymm5,%0" : : "m" (dst[0xa0]));
#endif
#if NREGS > 6
			asm volatile("vmovdqa %%ymm6,%0" : : "m" (dst[0xc0]));
#endif
#if NREGS > 7
			asm volatile("vmovdqa %%ymm7,%0" : : "m" (dst[0xe0]));
#endif
#if NREGS > 8
			asm volatile("vmovdqa %%ymm8,%0" : : "m" (dst[0x100]));
#endif
#if NREGS > 9
			asm volatile("vmovdqa %%ymm9,%0" : : "m" (dst[0x120]));
#endif
#if NREGS > 10
			asm volatile("vmovdqa %%ymm10,%0" : : "m" (dst[0x140]));
#endif
#if NREGS > 11
			asm volatile("vmovdqa %%ymm11,%0" : : "m" (dst[0x160]));
#endif
#if NREGS > 12
			asm volatile("vmovdqa %%ymm12,%0" : : "m" (dst[0x180]));
#endif
#if NREGS > 13
			asm volatile("vmovdqa %%ymm13,%0" : : "m" (dst[0x1a0]));
#endif
#if NREGS > 14
			asm volatile("vmovdqa %%ymm14,%0" : : "m" (dst[0x1c0]));
#endif
#if NREGS > 15
			asm volatile("vmovdqa %%ymm15,%0" : : "m" (dst[0x1e0]));
#endif
			dst += NREGS * 0x20;
			src += NREGS * 0x20;
			nbytes -= NREGS * 0x20;
		}
		if (nbytes > 0)
			memcpy(dst, src, nbytes);
		return __dst;
	}
	printf("AVX fallback due to alignment\n");
	return memcpy(dst, src, nbytes);
}

static void *memcpy_sse(void *__dst, const void *__src, size_t nbytes)
{
	char	   *dst = __dst;
	const char *src = __src;

	/* SSE2 if 128bits aligned */
	if (((uintptr_t)src & 0x0f) == 0 && ((uintptr_t)dst & 0x0f) == 0)
	{
		while (nbytes >= NREGS * 0x10)
		{
			/* read */
			asm volatile("movdqa %0,%%xmm0" : : "m" (src[0x00]));
#if NREGS > 1
			asm volatile("movdqa %0,%%xmm1" : : "m" (src[0x10]));
#endif
#if NREGS > 2
			asm volatile("movdqa %0,%%xmm2" : : "m" (src[0x20]));
#endif
#if NREGS > 3
			asm volatile("movdqa %0,%%xmm3" : : "m" (src[0x30]));
#endif
#if NREGS > 4
			asm volatile("movdqa %0,%%xmm4" : : "m" (src[0x40]));
#endif
#if NREGS > 5
			asm volatile("movdqa %0,%%xmm5" : : "m" (src[0x50]));
#endif
#if NREGS > 6
			asm volatile("movdqa %0,%%xmm6" : : "m" (src[0x60]));
#endif
#if NREGS > 7
			asm volatile("movdqa %0,%%xmm7" : : "m" (src[0x70]));
#endif
#if NREGS > 8
			asm volatile("movdqa %0,%%xmm8" : : "m" (src[0x80]));
#endif
#if NREGS > 9
			asm volatile("movdqa %0,%%xmm9" : : "m" (src[0x90]));
#endif
#if NREGS > 10
			asm volatile("movdqa %0,%%xmm10" : : "m" (src[0xa0]));
#endif
#if NREGS > 11
			asm volatile("movdqa %0,%%xmm11" : : "m" (src[0xb0]));
#endif
#if NREGS > 12
			asm volatile("movdqa %0,%%xmm12" : : "m" (src[0xc0]));
#endif
#if NREGS > 13
			asm volatile("movdqa %0,%%xmm13" : : "m" (src[0xd0]));
#endif
#if NREGS > 14
			asm volatile("movdqa %0,%%xmm14" : : "m" (src[0xe0]));
#endif
#if NREGS > 15
			asm volatile("movdqa %0,%%xmm15" : : "m" (src[0xf0]));
#endif

			/* write */
			asm volatile("movdqa %%xmm0,%0" : : "m" (dst[0x00]));
#if NREGS > 0
			asm volatile("movdqa %%xmm1,%0" : : "m" (dst[0x10]));
#endif
#if NREGS > 1
			asm volatile("movdqa %%xmm2,%0" : : "m" (dst[0x20]));
#endif
#if NREGS > 2
			asm volatile("movdqa %%xmm3,%0" : : "m" (dst[0x30]));
#endif
#if NREGS > 3
			asm volatile("movdqa %%xmm4,%0" : : "m" (dst[0x40]));
#endif
#if NREGS > 4
			asm volatile("movdqa %%xmm5,%0" : : "m" (dst[0x50]));
#endif
#if NREGS > 5
			asm volatile("movdqa %%xmm6,%0" : : "m" (dst[0x60]));
#endif
#if NREGS > 6
			asm volatile("movdqa %%xmm7,%0" : : "m" (dst[0x70]));
#endif
#if NREGS > 7
			asm volatile("movdqa %%xmm8,%0" : : "m" (dst[0x80]));
#endif
#if NREGS > 8
			asm volatile("movdqa %%xmm9,%0" : : "m" (dst[0x90]));
#endif
#if NREGS > 9
			asm volatile("movdqa %%xmm10,%0" : : "m" (dst[0xa0]));
#endif
#if NREGS > 10
			asm volatile("movdqa %%xmm11,%0" : : "m" (dst[0xb0]));
#endif
#if NREGS > 11
			asm volatile("movdqa %%xmm12,%0" : : "m" (dst[0xc0]));
#endif
#if NREGS > 12
			asm volatile("movdqa %%xmm13,%0" : : "m" (dst[0xd0]));
#endif
#if NREGS > 13
			asm volatile("movdqa %%xmm14,%0" : : "m" (dst[0xe0]));
#endif
#if NREGS > 14
			asm volatile("movdqa %%xmm15,%0" : : "m" (dst[0xf0]));
#endif
			dst += NREGS * 0x10;
			src += NREGS * 0x10;
			nbytes -= NREGS * 0x10;
		}
		if (nbytes > 0)
			memcpy(dst, src, nbytes);
		return __dst;
	}
	printf("SSE fallback due to alignment\n");
	return memcpy(dst, src, nbytes);
}

int main(int argc, char * const argv[])
{
	size_t		nbytes;
	int			i, j, nloops = 20;
	char	   *src;
	char	   *dst;
	char	   *tmp;
	struct timeval tv1, tv2, tv3, tv4;

	if (argc != 2)
		return 1;
	nbytes = atol(argv[1]) << 20;	/* MB */

	src = malloc(nbytes + 1024);
	if (!src)
	{
		printf("out of memory: %m\n");
		return 1;
	}
	dst = malloc(nbytes + 1024);
	if (!dst)
	{
		printf("out of memory: %m\n");
		return 1;
	}
	memset(src, 0x12345678, nbytes);
	memset(dst, 0x87654321, nbytes);

	src = (char *)(((uintptr_t)src + 0x1f) & ~0x1fUL);
	dst = (char *)(((uintptr_t)src + 0x1f) & ~0x1fUL);

	gettimeofday(&tv1, NULL);
	for (i=0; i < nloops; i++)
	{
		memcpy_avx(dst, src, nbytes);
		tmp = src;
		src = dst;
		dst = tmp;
	}
	gettimeofday(&tv2, NULL);
	for (i=0; i < nloops; i++)
	{
		memcpy_sse(dst, src, nbytes);
		tmp = src;
		src = dst;
		dst = tmp;
	}
	gettimeofday(&tv3, NULL);
	for (i=0; i < nloops; i++)
	{
		memcpy(dst, src, nbytes);
		tmp = src;
		src = dst;
		dst = tmp;
	}
	gettimeofday(&tv4, NULL);

	printf("x86: %.3fMB/s, SSE: %.3fMB/s, AVX: %.3fMB/s\n",
		   (double)((nloops * nbytes) >> 20) /
		   ((double)((tv4.tv_sec * 1000 + tv4.tv_usec / 1000) -
					 (tv3.tv_sec * 1000 + tv3.tv_usec / 1000)) / 1000.0),
		   (double)((nloops * nbytes) >> 20) /
		   ((double)((tv3.tv_sec * 1000 + tv3.tv_usec / 1000) -
					 (tv2.tv_sec * 1000 + tv2.tv_usec / 1000)) / 1000.0),
		   (double)((nloops * nbytes) >> 20) /
		   ((double)((tv2.tv_sec * 1000 + tv2.tv_usec / 1000) -
					 (tv1.tv_sec * 1000 + tv1.tv_usec / 1000)) / 1000.0));
	return 0;
}
