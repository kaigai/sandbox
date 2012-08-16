/*
 * fcgi-cat.c
 *
 * A FastCGI application that references a static document.
 * It just output content of the specified file with SCRIPT_FILENAME
 * environment vriable.
 *
 * Prerequisites:
 *   fcgi-devel package
 *
 * Step to install:
 *   % gcc -g fcgi-cat.c -lfcgi -o fcgi-cat
 *   % sudo install -m 755 fcgi-cat /path/to/bin
 */
#include <fcgi_stdio.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>
#include <errno.h>
#include <getopt.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

/* HTTP ERROR CODES */
#define HTTP_BAD_REQUEST		400
#define HTTP_FORBIDDEN			403
#define HTTP_NOT_FOUND			404
#define HTTP_INTERNAL_ERROR		500

int main(int argc, char *argv[])
{
	const char *weeks[] = { "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
	const char *months[] = { "Jan", "Feb", "Mar", "Apr", "May", "Jun",
							 "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };

	while (FCGI_Accept() == 0) {
		const char	   *filename = getenv("SCRIPT_FILENAME");
		const char	   *content_type = NULL;
		struct stat		stbuf;
		struct tm	   *tm;
		int				fdesc;
		int				sockfd;
		ssize_t			sent;
		ssize_t			total;
		int				status = 0;

		if (!filename)
		{
			FCGI_fprintf(FCGI_stderr,
						 "fcgi-cat: no filename given with SCRIPT_FILENAME\n");
			status = HTTP_BAD_REQUEST;
			goto skip;
		}

		fdesc = open(filename, O_RDONLY);
		if (fdesc < 0)
		{
			FCGI_fprintf(FCGI_stderr,
						 "fcgi-cat: could not open file: \"%s\"\n", filename);
			if (errno == EACCES || errno == EPERM)
				status = HTTP_FORBIDDEN;
			else if (errno == ENOENT)
				status = HTTP_NOT_FOUND;
			else
				status = HTTP_INTERNAL_ERROR;
			goto skip;
		}

		if (fstat(fdesc, &stbuf) != 0)
		{
			FCGI_fprintf(FCGI_stderr,
						 "fcgi-cat: could not stat file: \"%s\"\n", filename);
			if (errno == EACCES || errno == EPERM)
				status = HTTP_FORBIDDEN;
			else
				status = HTTP_INTERNAL_ERROR;
			close(fdesc);
			goto skip;
		}

		sockfd = FCGI_fileno(FCGI_stdout);

		/*
		 * Generate HTTP Headers
		 */
		tm = gmtime(&stbuf.st_mtime);
		FCGI_printf("Last-Modified: %s, %02d %s %04d %02d:%02d:%02d GMT\n",
					weeks[tm->tm_wday], tm->tm_mday, months[tm->tm_mon],
					tm->tm_year + 1900, tm->tm_hour, tm->tm_min, tm->tm_sec);
		FCGI_printf("Content-Length: %" PRIu64 "\n",
					(uint64_t)stbuf.st_size);
		
		FCGI_putchar('\n');

		for (total=0; total < stbuf.st_size; total += sent)
		{
			sent = sendfile(sockfd, fdesc, NULL, stbuf.st_size);
			if (sent < 1)
			{
				FCGI_fprintf(FCGI_stderr,
							 "fcgi-cat: error during sendfile: \"%s\" (%m)\n",
							 filename);
				status = HTTP_INTERNAL_ERROR;
				break;
			}
		}
		close(fdesc);

	skip:
		FCGI_SetExitStatus(status);
		FCGI_Finish();
	}
	return 0;
}
