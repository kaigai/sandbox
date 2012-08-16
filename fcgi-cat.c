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
#include <string.h>
#include <errno.h>
#include <getopt.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <sys/sendfile.h>

/* HTTP ERROR CODES */
#define HTTP_OK					"Status: 200 OK\n"
#define HTTP_BAD_REQUEST		"Status: 400 Bad Request\n"
#define HTTP_FORBIDDEN			"Status: 403 Forbidden\n"
#define HTTP_NOT_FOUND			"Status: 404 Not Found\n"
#define HTTP_INTERNAL_ERROR		"Status: 500 Server Internal Error\n"

#define FCGICAT_BUFSIZE			(4*1024*1024)

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
		int				status = 0;
		ssize_t			total;
		ssize_t			length;
		char			buffer[FCGICAT_BUFSIZE];

		if (!filename)
		{
			FCGI_puts(HTTP_BAD_REQUEST);
			FCGI_fprintf(FCGI_stderr,
						 "fcgi-cat: no filename given with SCRIPT_FILENAME\n");
			goto skip;
		}

		fdesc = open(filename, O_RDONLY);
		if (fdesc < 0)
		{
			FCGI_fprintf(FCGI_stderr,
						 "fcgi-cat: failed to open file \"%s\" (%s)",
						 filename, strerror(errno));
			if (errno == EACCES || errno == EPERM)
				FCGI_puts(HTTP_FORBIDDEN);
			else if (errno == ENOENT)
				FCGI_puts(HTTP_NOT_FOUND);
			else
				FCGI_puts(HTTP_INTERNAL_ERROR);
			goto skip;
		}

		if (fstat(fdesc, &stbuf) != 0)
		{
			FCGI_fprintf(FCGI_stderr,
						 "fcgi-cat: failed to stat file \"%s\" (%s)",
						 filename, strerror(errno));
			if (errno == EACCES || errno == EPERM)
				FCGI_puts(HTTP_FORBIDDEN);
			else
				FCGI_puts(HTTP_INTERNAL_ERROR);
			close(fdesc);
			goto skip;
		}

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

		for (total=0; total < stbuf.st_size; total += length)
		{
			length = read(fdesc, buffer, sizeof(buffer));
			if (length < 0)
			{
				FCGI_fprintf(FCGI_stderr,
							 "fcgi-cat: failed on read from \"%s\" (%s)",
							 filename, strerror(errno));
				status = 1;
				break;
			}
			if (FCGI_fwrite(buffer, 1, length, FCGI_stdout) != length)
			{
				FCGI_fprintf(FCGI_stderr,
							 "fcgi-cat: failed on write to IPC (%s)",
							 strerror(errno));
				status = 1;
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
