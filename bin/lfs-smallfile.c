#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>

#include "user.h"

#define NUMFILES	10000
#define NUMDIRS		100
#define FILESIZE	1024

#define NTEST		100000 /* No. of tests of gettimeofday() */

int num_files;
int nfiles_per_dir;
int timer_overhead;

void create_dirs(const char *topdir, int num_dirs);
void delete_dirs(const char *topdir, int num_dirs);
int create_files(const char *topdir, char *buf);
int read_files(const char *topdir, char *buf);
int unlink_files(const char *topdir, char *buf);

void usage(char *prog)
{
	fprintf(stderr, "Usage: %s [-n num_files] [-s spread] <working directory>\n", prog);
	exit(1);
}

int main(int argc, char **argv)
{
	int i, num_dirs;
	char ch, *buf, *topdir;
	extern char *optarg;
	extern int optind;

	int usec;
	float sec;
	float throughput;
	struct timeval before, after, dummy;

	/* Parse and test the arguments. */
	if (argc < 2) {
		usage(argv[0]);
	}

	num_files = NUMFILES;
	num_dirs = NUMDIRS;
	while ((ch = getopt(argc, argv, "n:s:")) != -1) {
		switch (ch) {
			case 'n':
				num_files = atoi(optarg);
				break;
			case 's':
				num_dirs = atoi(optarg);
				break;
			default:
				usage(argv[0]);
				exit(1);
		}
	}
	argc -= optind;
	argv += optind;

	topdir = argv[0];
	nfiles_per_dir = num_files/num_dirs;

	buf = (char *)malloc(FILESIZE);
	if (!buf) {
		fprintf(stderr, "ERROR: Failed to allocate buffer");
		exit(1);
	}

	/* Create the directories for the files to be spread amongst */
	create_dirs(topdir, num_dirs);

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
	printf ( "Running smallfile test on %s\n", topdir );
	printf ( "File Size = %d bytes\n", FILESIZE );
	printf ( "No. of files = %d\n", num_files );
	printf ( "No. of dirs (spread) = %d\n", num_dirs );
	printf ( "No. of files per dir = %d\n", nfiles_per_dir );
	printf ( "Test            Time(sec)       Files/sec\n" );
	printf ( "----            ---------       ---------\n" );

	usec = create_files(topdir, buf);
	sec = (float) usec / 1000000.0;
	throughput = ((float) num_files / sec);
	printf ( "create_files\t%7.3f\t\t%7.3f\n", sec, throughput );
	fflush ( stdout );


	usec = read_files(topdir, buf);
	sec = (float) usec / 1000000.0;
	throughput = ((float) num_files / sec);
	printf ( "read_files\t%7.3f\t\t%7.3f\n", sec, throughput );
	fflush ( stdout );

	usec = unlink_files(topdir, buf);
	sec = (float) usec / 1000000.0;
	throughput = ((float) num_files / sec);
	printf ( "unlink_files\t%7.3f\t\t%7.3f\n", sec, throughput );
	fflush ( stdout );

	delete_dirs(topdir, num_dirs);

	return 0;
}


void create_dirs(const char *topdir, int num_dirs)
{
	int i, ret;
	char dir[128];

	for (i = 0; i < num_dirs; i++) {
		snprintf(dir, 128, "%s/dir-%d", topdir, i);
		if ((ret = mkdir(dir, 0777)) != 0)
			die("mkdir %s failed %d\n", dir, ret);
	}
}


void delete_dirs(const char *topdir, int num_dirs)
{
	int i, ret;
	char dir[128];

	for (i = 0; i < num_dirs; i++) {
		snprintf(dir, 128, "%s/dir-%d", topdir, i);
		if ((ret = unlink(dir)) != 0)
			die("unlink %s failed %d\n", dir, ret);
	}
}


int create_files(const char *topdir, char *buf)
{
	int i, j, fd;
	ssize_t size;
	char filename[128];
	unsigned long time;
	struct timeval before, after;

	time = 0;
	gettimeofday ( &before, NULL );
	/* Create phase */
	for (i = 0, j = 0; i < num_files; i++) {
		snprintf(filename, 128, "%s/dir-%d/file-%d", topdir, j, i);
		fd = open(filename, O_WRONLY | O_CREAT | O_EXCL, S_IWUSR | S_IRUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
		if (fd == -1)
			die("open");

		size = write(fd, buf, FILESIZE);
		if (size == -1)
			die("write");

		if (close(fd) < 0)
			die("close");

		if ((i+1) % nfiles_per_dir == 0)
			j++;
	}
	gettimeofday ( &after, NULL );
	time = time + (after.tv_sec - before.tv_sec) * 1000000 +
		(after.tv_usec - before.tv_usec);
	time -= timer_overhead;
	return time;
}

int read_files(const char *topdir, char *buf)
{
	int i, j, fd;
	ssize_t size;
	char filename[128];
	unsigned long time;
	struct timeval before, after;

	time = 0;
	gettimeofday ( &before, NULL );
	/* Read phase */
	for (i = 0, j = 0; i < num_files; i++) {
		snprintf(filename, 128, "%s/dir-%d/file-%d", topdir, j, i);
		fd = open(filename, O_RDONLY);
		if (fd == -1)
			die("open");

		size = read(fd, buf, FILESIZE);
		if (size == -1)
			die("read");

		if (close(fd) < 0)
			die("close");

		if ((i+1) % nfiles_per_dir == 0)
			j++;
	}
	gettimeofday ( &after, NULL );
	time = time + (after.tv_sec - before.tv_sec) * 1000000 +
		(after.tv_usec - before.tv_usec);
	time -= timer_overhead;
	return time;
}

int unlink_files(const char *topdir, char *buf)
{
	int i, j, ret;
	char filename[128];
	unsigned long time;
	struct timeval before, after;

	time = 0;
	gettimeofday ( &before, NULL );
	/* Unlink phase */
	for (i = 0, j = 0; i < num_files; i++) {
		snprintf(filename, 128, "%s/dir-%d/file-%d", topdir, j, i);
		ret = unlink(filename);
		if (ret == -1)
			die("unlink");

		if ((i+1) % nfiles_per_dir == 0)
			j++;
	}
	gettimeofday ( &after, NULL );
	time = time + (after.tv_sec - before.tv_sec) * 1000000 +
		(after.tv_usec - before.tv_usec);
	time -= timer_overhead;
	return time;
}
