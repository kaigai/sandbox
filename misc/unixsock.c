#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>

static void
my_sighup(int sig)
{
	printf("got signal: %d\n", sig);
}

static int
server_main(struct sockaddr_un *saddr)
{
	int		sockfd;
	int		client;
	struct sockaddr_un	caddr;
	struct timeval		timeout;
	socklen_t caddr_len;

	sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (sockfd < 0)
	{
		fprintf(stderr, "failed on socket(2): %s\n", strerror(errno));
		return 1;
	}

	if (bind(sockfd,
			 (struct sockaddr *)saddr,
			 sizeof(struct sockaddr_un)) != 0)
	{
		fprintf(stderr, "failed on bind(2): %s\n", strerror(errno));
		return 1;
	}

	if (listen(sockfd, 0) < 0)
	{
		fprintf(stderr, "failed on listen(2): %s\n", strerror(errno));
		return 1;
	}
	printf("begin to accept\n");

	signal(SIGHUP, my_sighup);

	timeout.tv_sec = 0;
	timeout.tv_usec = 600000;

	if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO,
				   &timeout, sizeof(timeout)) != 0)
	{
		fprintf(stderr, "failed on setsockopt(2): %s\n", strerror(errno));
		return 1;
	}

retry:
	puts("server accept");
	while ((client = accept(sockfd, NULL, NULL)) >= 0)
	{
		printf("connection from client\n");
		write(client, "hello world", 11);
		sleep(10);
		puts("server accept");
	}
	printf("accept failed: %m\n");
	if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS)
		goto retry;


	return 0;
}

int main(int argc, const char *argv[])
{
	pid_t	serv;
	struct sockaddr_un saddr;
	int		sockfd = -1;
	int		c;

	saddr.sun_family = AF_UNIX;
	snprintf(saddr.sun_path, sizeof(saddr.sun_path),
			 "/tmp/.unixsock.%u", getpid());

	serv = fork();
	if (serv == 0)
		return server_main(&saddr);
	else if (serv < 0)
	{
		fprintf(stderr, "failed to launch server process\n");
		return 1;
	}

	while ((c = getchar()) == '\n')
	{
		char	buf[100];
		ssize_t	len;

		if (sockfd < 0 || 1+1==2)
		{
			struct timeval timeout;

			printf("try to open connection\n");
			sockfd = socket(AF_UNIX, SOCK_STREAM, 0);

			if (sockfd < 0)
			{
				fprintf(stderr, "failed on socket(2): %s\n", strerror(errno));
				break;
			}

#if 0
			fval = fcntl(sockfd, F_GETFL, 0);
			fval |= O_NONBLOCK;
			if (fcntl(sockfd, F_SETFL, fval) < 0)
			{
				fprintf(stderr, "failed on fcntl(2): %s\n", strerror(errno));
				break;
			}
#endif
			timeout.tv_sec = 0;
			timeout.tv_usec = 500 * 1000;

			if (setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO,
						   &timeout, sizeof(timeout)) != 0)
			{
				fprintf(stderr, "failed on setsockopt(SO_SNDTIMEO): %m\n");
				break;
			}

			if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO,
						   &timeout, sizeof(timeout)) != 0)
			{
				fprintf(stderr, "failed on setsockopt(SO_RCVTIMEO): %m\n");
				break;
			}

			puts("connect(2)");
			if (connect(sockfd, (struct sockaddr *)&saddr, sizeof(saddr)) != 0)
			{
				fprintf(stderr, "failed on connect(2): %s\n", strerror(errno));
				break;
			}
			puts("done");

			while ((len = read(sockfd, buf, sizeof(buf))) > 0)
			{
				buf[len] = '\0';
				printf("message [%s]\n", buf);
			}
			printf("len=%d\n", (int)len);

#if 0
			fval &= ~O_NONBLOCK;
			if (fcntl(sockfd, F_SETFL, fval) < 0)
			{
				fprintf(stderr, "Failed on fcntl(2): %s\n", strerror(errno));
				break;
			}
#endif
		}
		else
		{
			printf("close connection\n");
			close(sockfd);
			sockfd = -1;
		}
	}
	printf("end loop\n");
	kill(serv, SIGTERM);

	return 0;
}
