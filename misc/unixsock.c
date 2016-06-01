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

static int
server_main(struct sockaddr_un *saddr)
{
	int		sockfd;
	int		client;
	struct sockaddr_un	caddr;
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

	if (listen(sockfd, 1) < 0)
	{
		fprintf(stderr, "failed on listen(2): %s\n", strerror(errno));
		return 1;
	}
	printf("begin to accept\n");

	while ((client = accept(sockfd, NULL, NULL)) >= 0)
	{
		printf("connection from client\n");
		sleep(5);
	}
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
		if (sockfd < 0 || 1+1==2)
		{
			int		fval;

			printf("open connection\n");
			sockfd = socket(AF_UNIX, SOCK_STREAM, 0);

			if (sockfd < 0)
			{
				fprintf(stderr, "failed on socket(2): %s\n", strerror(errno));
				break;
			}

			fval = fcntl(sockfd, F_GETFL, 0);
			fval |= O_NONBLOCK;
			if (fcntl(sockfd, F_SETFL, fval) < 0)
			{
				fprintf(stderr, "failed on fcntl(2): %s\n", strerror(errno));
				break;
			}

			if (connect(sockfd, (struct sockaddr *)&saddr, sizeof(saddr)) != 0)
			{
				fprintf(stderr, "failed on connect(2): %s\n", strerror(errno));
				break;
			}

			fval &= ~O_NONBLOCK;
			if (fcntl(sockfd, F_SETFL, fval) < 0)
			{
				fprintf(stderr, "Failed on fcntl(2): %s\n", strerror(errno));
				break;
			}
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
