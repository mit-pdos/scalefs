#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>

#include "user.h"

#define NUMFILES	10000
#define FILESIZE	1024

#define NTEST		100000 /* No. of tests of gettimeofday() */

int timer_overhead;

int create_files(const char *dirname, char *buf);
int read_files(const char *dirname, char *buf);
int unlink_files(const char *dirname, char *buf);

void usage(char *prog)
{
	fprintf(stderr, "Usage: %s <working directory>\n", prog);
	exit(1);
}

int main(int argc, char **argv)
{
	int i;
	char *buf;
	char *dirname;

	int usec;
	float sec;
	float throughput;
	struct timeval before, after, dummy;

	if (argc != 2) {
		usage(argv[0]);
	}
	dirname = argv[1];

	buf = (char *)malloc(FILESIZE);
	if (!buf) {
		fprintf(stderr, "ERROR: Failed to allocate buffer");
		exit(1);
	}

	/* Compute the overhead of the gettimeofday() call */

	gettimeofday(&before, NULL);
	for (i = 0; i < NTEST; i++)
		gettimeofday(&dummy, NULL);
	gettimeofday(&after, NULL);

	timer_overhead = (after.tv_sec - before.tv_sec) * 1000000 +
			 (after.tv_usec - before.tv_usec);
	timer_overhead /= NTEST;

	/*
	 * Now we just do the tests in sequence, printing the timing
	 * statistics as we go.
	 */

	printf ( "\n\n\n" );
	fflush ( stdout );
	printf ( "Running smallfile test on %s\n", argv[1] );
	printf ( "File Size = %d bytes\n", FILESIZE );
	printf ( "No. of files = %d\n", NUMFILES );
	printf ( "Test            Time(sec)       Files/sec\n" );
	printf ( "----            ---------       ---------\n" );

	usec = create_files(dirname, buf);
	sec = (float) usec / 1000000.0;
	throughput = ((float) NUMFILES / sec);
	printf ( "create_files\t%7.3f\t\t%7.3f\n", sec, throughput );
	fflush ( stdout );


	usec = read_files(dirname, buf);
	sec = (float) usec / 1000000.0;
	throughput = ((float) NUMFILES / sec);
	printf ( "read_files\t%7.3f\t\t%7.3f\n", sec, throughput );
	fflush ( stdout );

	usec = unlink_files(dirname, buf);
	sec = (float) usec / 1000000.0;
	throughput = ((float) NUMFILES / sec);
	printf ( "unlink_files\t%7.3f\t\t%7.3f\n", sec, throughput );
	fflush ( stdout );

	return 0;
}

int create_files(const char *dirname, char *buf)
{
	int i, fd;
	ssize_t size;
	char filename[128];
	unsigned long time;
	struct timeval before, after;

	time = 0;
	gettimeofday ( &before, NULL );
	/* Create phase */
	for (i = 0; i < NUMFILES; i++) {
		snprintf(filename, FILESIZE, "%s/file-%d", dirname,  i);
		fd = open(filename, O_WRONLY | O_CREAT | O_EXCL, S_IWUSR | S_IRUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
		if (fd == -1) {
			die("open");
			exit(1);
		}

		size = write(fd, buf, FILESIZE);
		if (size == -1) {
			die("write");
			exit(1);
		}

		close(fd);
	}
	gettimeofday ( &after, NULL );
	time = time + (after.tv_sec - before.tv_sec) * 1000000 +
		(after.tv_usec - before.tv_usec);
	time -= timer_overhead;
	return time;
}

int read_files(const char *dirname, char *buf)
{
	int i, fd;
	ssize_t size;
	char filename[128];
	unsigned long time;
	struct timeval before, after;

	time = 0;
	gettimeofday ( &before, NULL );
	/* Read phase */
	for (i = 0; i < NUMFILES; i++) {
		snprintf(filename, FILESIZE, "%s/file-%d", dirname, i);
		fd = open(filename, O_RDONLY);
		if (fd == -1) {
			die("open");
			exit(1);
		}

		size = read(fd, buf, FILESIZE);
		if (size == -1) {
			die("read");
			exit(1);
		}

		close(fd);
	}
	gettimeofday ( &after, NULL );
	time = time + (after.tv_sec - before.tv_sec) * 1000000 +
		(after.tv_usec - before.tv_usec);
	time -= timer_overhead;
	return time;
}

int unlink_files(const char *dirname, char *buf)
{
	int i, ret;
	char filename[128];
	unsigned long time;
	struct timeval before, after;

	time = 0;
	gettimeofday ( &before, NULL );
	/* Unlink phase */
	for (i = 0; i < NUMFILES; i++) {
		snprintf(filename, FILESIZE, "%s/file-%d", dirname, i);
		ret = unlink(filename);
		if (ret == -1) {
			die("unlink");
			exit(1);
		}
	}
	gettimeofday ( &after, NULL );
	time = time + (after.tv_sec - before.tv_sec) * 1000000 +
		(after.tv_usec - before.tv_usec);
	time -= timer_overhead;
	return time;
}
