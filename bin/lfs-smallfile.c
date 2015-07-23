#define _GNU_SOURCE
#include <pthread.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>

#include "libutil.h"

#define NUMFILES	10000
#define NUMDIRS		100
#define FILESIZE	1024

#define NTEST		100000 /* No. of tests of gettimeofday() */

#define MAXCPUS		128

#define SYNC_NONE	0
#define SYNC_UNLINK	1
#define SYNC_CREATE_UNLINK 2
#define FSYNC_CREATE	3

int num_cpus;
int num_files;
int num_dirs;
int nfiles_per_dir;
int timer_overhead;

char *topdir, *buf;

pthread_t tid[MAXCPUS];
pthread_barrier_t bar;

int sync_when;
int per_cpu_dirs;

void *run_benchmark(void *arg);
void create_dirs(const char *topdir, int num_dirs);
void delete_dirs(const char *topdir, int num_dirs);
int  create_files(const char *topdir, char *buf, int cpu);
int  read_files(const char *topdir, char *buf, int cpu);
int  unlink_files(const char *topdir, char *buf, int cpu);
int  sync_files(void);

void usage(char *prog)
{
	fprintf(stderr, "Usage: %s [-n num_files] [-s spread] [-c cpus] [-p] "
		"[-y sync_when] <working directory>\n", prog);
	fprintf(stderr, "where, sync_when can be:\n"
		"%d - no sync\n"
		"%d - sync after unlink\n"
		"%d - sync after create and unlink\n"
		"%d - fsync during create\n",
		SYNC_NONE, SYNC_UNLINK, SYNC_CREATE_UNLINK, FSYNC_CREATE);
	fprintf(stderr, "and -p creates files under per-cpu sub-directories\n");
	exit(1);
}

int main(int argc, char **argv)
{
	uint64_t i;
	char ch;
	extern char *optarg;
	extern int optind;

	float sec;
	unsigned long usec;
	struct timeval before, after, dummy;

	/* Parse and test the arguments. */
	if (argc < 2) {
		usage(argv[0]);
	}

	/* Set the defaults */
	num_files = NUMFILES;
	num_dirs = NUMDIRS;
	num_cpus = 1;
	per_cpu_dirs = 0;
	sync_when = SYNC_UNLINK;

	while ((ch = getopt(argc, argv, "n:s:c:p:y:")) != -1) {
		switch (ch) {
			case 'n':
				num_files = atoi(optarg);
				break;
			case 's':
				num_dirs = atoi(optarg);
				break;
			case 'c': // Run on these many CPUs. (0 to num_cpus-1)
				num_cpus = atoi(optarg);
				if (num_cpus > MAXCPUS)
					num_cpus = MAXCPUS;
				break;
			case 'p':
				per_cpu_dirs = 1;
				break;
			case 'y': // When to call sync()
				sync_when = atoi(optarg);
				break;
			default:
				usage(argv[0]);
				exit(1);
		}
	}
	argc -= optind;
	argv += optind;

	topdir = argv[0];

	if (num_files < num_dirs)
		num_dirs = 1;

	nfiles_per_dir = num_files/num_dirs;

	buf = (char *)malloc(FILESIZE);
	if (!buf) {
		fprintf(stderr, "ERROR: Failed to allocate buffer");
		exit(1);
	}

	for (i = 0; i < FILESIZE; i++)
		buf[i] = 'a';

	/* Create the directories for the files to be spread amongst */
	create_dirs(topdir, num_dirs);

	if (sync_when != SYNC_NONE)
		sync();

	/* Compute the overhead of the gettimeofday() call */

	gettimeofday(&before, NULL);
	for (i = 0; i < NTEST; i++)
		gettimeofday(&dummy, NULL);
	gettimeofday(&after, NULL);

	timer_overhead = (after.tv_sec - before.tv_sec) * 1000000 +
			 (after.tv_usec - before.tv_usec);
	timer_overhead /= NTEST;

	pthread_barrier_init(&bar, 0, num_cpus);

	/* Time the overall benchmark */
	usec = 0;
	gettimeofday(&before, NULL);

	for (i = 0; i < num_cpus; i++) {
	  pthread_create(&tid[i], NULL, run_benchmark, (void *) i);
	}

	for (i = 0; i < num_cpus; i++) {
	  pthread_join(tid[i], NULL);
	}

	gettimeofday(&after, NULL);
	usec = (after.tv_sec - before.tv_sec) * 1000000 +
               (after.tv_usec - before.tv_usec);
	usec -= timer_overhead;

	sec = (float) usec / 1000000.0;
	printf("\nOverall benchmark (%s) : %7.3f sec\n",
              (sync_when == FSYNC_CREATE)?"no sleep used":"including sleep", sec);

	delete_dirs(topdir, num_dirs);

	if (sync_when != SYNC_NONE)
		sync();

	return 0;
}

void *run_benchmark(void *arg)
{
	int usec;
	float sec, total_sec;
	float throughput, avg_throughput;

	int cpu = (uintptr_t)arg;

	if (setaffinity(cpu) < 0) {
		printf("setaffinity failed for cpu %d\n", cpu);
		return 0;
	}

	pthread_barrier_wait(&bar);

	/*
	 * Now we just do the tests in sequence, printing the timing
	 * statistics as we go.
	 */
	total_sec = 0;

	printf ( "\n\n\n" );
	fflush ( stdout );
	printf ( "Running smallfile test on %s, on CPU %d\n", topdir, cpu );
	printf ( "File Size = %d bytes\n", FILESIZE );
	printf ( "No. of files = %d\n", num_files );
	printf ( "No. of dirs (spread) = %d\n", num_dirs );
	printf ( "No. of files per dir = %d\n", nfiles_per_dir );
	printf ( "Test            Time(sec)       Files/sec\n" );
	printf ( "----            ---------       ---------\n" );

	usec = create_files(topdir, buf, cpu);
	sec = (float) usec / 1000000.0;
	throughput = ((float) num_files / sec);
	printf ( "create_files\t%7.3f\t\t%7.3f\n", sec, throughput );
	fflush ( stdout );
	total_sec += sec;

	if (sync_when == SYNC_CREATE_UNLINK) {
		sleep(2);

		usec = sync_files();
		sec = (float) usec / 1000000.0;
		throughput = ((float) num_files / sec);
		printf ( "sync_files_1\t%7.3f\t\t%7.3f\n", sec, throughput );
		fflush ( stdout );
		total_sec += sec;
	}

	usec = read_files(topdir, buf, cpu);
	sec = (float) usec / 1000000.0;
	throughput = ((float) num_files / sec);
	printf ( "read_files\t%7.3f\t\t%7.3f\n", sec, throughput );
	fflush ( stdout );
	total_sec += sec;

	usec = unlink_files(topdir, buf, cpu);
	sec = (float) usec / 1000000.0;
	throughput = ((float) num_files / sec);
	printf ( "unlink_files\t%7.3f\t\t%7.3f\n", sec, throughput );
	fflush ( stdout );
	total_sec += sec;

	if (sync_when == SYNC_UNLINK || sync_when == SYNC_CREATE_UNLINK) {
		sleep(2);

		usec = sync_files();
		sec = (float) usec / 1000000.0;
		throughput = ((float) num_files / sec);
		printf ( "sync_files_2\t%7.3f\t\t%7.3f\n", sec, throughput );
		fflush ( stdout );
		total_sec += sec;
	}

	avg_throughput = ((float) num_files / total_sec);
	printf ( "thread %d total \t%7.3f\t\t%7.3f\n", cpu, total_sec, avg_throughput );
	fflush ( stdout );

	return 0;
}

void create_dirs(const char *topdir, int num_dirs)
{
	int i, j, ret;
	char dir[128], sub_dir[128];

	if (per_cpu_dirs) {
		for (j = 0; j < num_cpus; j++) {
			snprintf(sub_dir, 128, "%s/cpu-%d", topdir, j);
			if ((ret = mkdir(sub_dir, 0777)) != 0)
				die("mkdir %s failed %d\n", sub_dir, ret);

			for (i = 0; i < num_dirs; i++) {
				snprintf(dir, 128, "%s/cpu-%d/dir-%d", topdir, j, i);
				if ((ret = mkdir(dir, 0777)) != 0)
					die("mkdir %s failed %d\n", dir, ret);
			}
		}
	} else {
		for (i = 0; i < num_dirs; i++) {
			snprintf(dir, 128, "%s/dir-%d", topdir, i);
			if ((ret = mkdir(dir, 0777)) != 0)
				die("mkdir %s failed %d\n", dir, ret);
		}
	}
}


void delete_dirs(const char *topdir, int num_dirs)
{
	int i, j, ret;
	char dir[128], sub_dir[128];

	if (per_cpu_dirs) {
		for (j = 0; j < num_cpus; j++) {
			snprintf(sub_dir, 128, "%s/cpu-%d", topdir, j);

			for (i = 0; i < num_dirs; i++) {
				snprintf(dir, 128, "%s/cpu-%d/dir-%d", topdir, j, i);
				if ((ret = unlink(dir)) != 0)
					die("unlink %s failed %d\n", dir, ret);
			}

			if ((ret = unlink(sub_dir)) != 0)
				die("unlink %s failed %d\n", sub_dir, ret);
		}
	} else {
		for (i = 0; i < num_dirs; i++) {
			snprintf(dir, 128, "%s/dir-%d", topdir, i);
			if ((ret = unlink(dir)) != 0)
				die("unlink %s failed %d\n", dir, ret);
		}
	}
}


int create_files(const char *topdir, char *buf, int cpu)
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
		if (per_cpu_dirs)
			snprintf(filename, 128, "%s/cpu-%d/dir-%d/file-%d-%d",
				topdir, cpu, j, cpu, i);
		else
			snprintf(filename, 128, "%s/dir-%d/file-%d-%d",
				topdir, j, cpu, i);

		fd = open(filename, O_WRONLY | O_CREAT | O_EXCL,
			S_IWUSR | S_IRUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);

		if (fd == -1)
			die("open");

		size = write(fd, buf, FILESIZE);
		if (size == -1)
			die("write");

		if (sync_when == FSYNC_CREATE && fsync(fd) < 0)
			die("fsync");

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

int read_files(const char *topdir, char *buf, int cpu)
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
		if (per_cpu_dirs)
			snprintf(filename, 128, "%s/cpu-%d/dir-%d/file-%d-%d",
				topdir, cpu, j, cpu, i);
		else
			snprintf(filename, 128, "%s/dir-%d/file-%d-%d",
				topdir, j, cpu, i);

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

int unlink_files(const char *topdir, char *buf, int cpu)
{
	int i, j, ret;
	char filename[128];
	unsigned long time;
	struct timeval before, after;

	time = 0;
	gettimeofday ( &before, NULL );
	/* Unlink phase */
	for (i = 0, j = 0; i < num_files; i++) {
		if (per_cpu_dirs)
			snprintf(filename, 128, "%s/cpu-%d/dir-%d/file-%d-%d",
				topdir, cpu, j, cpu, i);
		else
			snprintf(filename, 128, "%s/dir-%d/file-%d-%d",
				topdir, j, cpu, i);

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

int sync_files(void)
{
	unsigned long time;
	struct timeval before, after;

	time = 0;
	gettimeofday ( &before, NULL );
	/* Sync everything */
	sync();
	gettimeofday ( &after, NULL );
	time = time + (after.tv_sec - before.tv_sec) * 1000000 +
		(after.tv_usec - before.tv_usec);
	time -= timer_overhead;
	return time;
}
