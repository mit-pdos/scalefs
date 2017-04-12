#define _GNU_SOURCE
#include <pthread.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/wait.h>

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

char *sync_options[] = {"SYNC_NONE", "SYNC_UNLINK", "SYNC_CREATE_UNLINK",
                        "FSYNC_CREATE"};

int num_cpus;
unsigned long num_files;
unsigned long num_dirs;
unsigned long nfiles_per_dir;
unsigned long timer_overhead;

char *topdir, *buf;

pthread_t tid[MAXCPUS];
pthread_barrier_t bar;

int sync_when, verbose;
int per_cpu_dirs, use_fork;

void *run_benchmark(void *arg);
void create_dirs(const char *topdir, unsigned long num_dirs);
void delete_dirs(const char *topdir, unsigned long num_dirs);
unsigned long  create_files(const char *topdir, char *buf, int cpu);
unsigned long  read_files(const char *topdir, char *buf, int cpu);
unsigned long  unlink_files(const char *topdir, char *buf, int cpu);
unsigned long  sync_files(void);

void usage(char *prog)
{
	fprintf(stderr, "Usage: %s [-n num_files] [-s spread] [-c cpus] [-p] [-f] "
		"[-y sync_when] <working directory>\n", prog);
	fprintf(stderr, "where, sync_when can be:\n"
		"%d - no sync\n"
		"%d - sync after unlink\n"
		"%d - sync after create and unlink\n"
		"%d - fsync during create\n\n",
		SYNC_NONE, SYNC_UNLINK, SYNC_CREATE_UNLINK, FSYNC_CREATE);
	fprintf(stderr, "-p creates files under per-cpu sub-directories\n");
	fprintf(stderr, "-f uses fork instead of pthreads\n");
	exit(1);
}

int main(int argc, char **argv)
{
	uint64_t i;
	char ch;
	extern char *optarg;
	extern int optind;

	float sec, throughput;
	unsigned long usec;
	struct timeval before, after, dummy;
	int forkret = 0;

	/* Parse and test the arguments. */
	if (argc < 2) {
		usage(argv[0]);
	}

	/* Set the defaults */
	num_files = NUMFILES;
	num_dirs = NUMDIRS;
	num_cpus = 1;
	per_cpu_dirs = 0;
	use_fork = 0;
	sync_when = SYNC_UNLINK;
	verbose = 0;

	while ((ch = getopt(argc, argv, "n:s:c:pfy:v")) != -1) {
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
			case 'f':
				use_fork = 1;
				break;
			case 'y': // When to call sync()
				sync_when = atoi(optarg);
				break;
			case 'v':
				verbose = 1;
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

	if (!use_fork)
		pthread_barrier_init(&bar, 0, num_cpus);

	// Print out benchmark parameters.
	printf("\n\n\n");
	fflush(stdout);
	printf("Running LFS-Smallfile test on %s, on %d CPUs\n", topdir, num_cpus);
	printf("File Size = %d bytes\n", FILESIZE);
	printf("No. of files = %ld\n", num_files);
	printf("No. of dirs (spread) = %ld\n", num_dirs);
	printf("No. of files per dir = %ld\n", nfiles_per_dir);
	printf("Directories are: %s\n", per_cpu_dirs ? "per-cpu" : "shared");
	printf("Sync/Fsync option: %s\n", sync_options[sync_when]);
	printf("Running %d parallel benchmark instance(s) using %s\n",
                num_cpus, use_fork ? "fork" : "pthreads");
	fflush(stdout);

	/* Time the overall benchmark */
	usec = 0;
	gettimeofday(&before, NULL);

	if (use_fork) {
		for (i = 0; i < num_cpus; i++) {
			forkret = fork();
			if (forkret == 0) {
				run_benchmark((void *) i);
				return 0; // Each child exits here.
			}
		}

		// Parent
		for (i = 0; i < num_cpus; i++)
			waitpid(-1, NULL, 0);

		// Fall-through to print out the statistics and
		// cleanup the benchmark.
	} else {
		for (i = 0; i < num_cpus; i++)
			pthread_create(&tid[i], NULL, run_benchmark, (void *) i);

		for (i = 0; i < num_cpus; i++)
			pthread_join(tid[i], NULL);
	}

	gettimeofday(&after, NULL);
	usec = (after.tv_sec - before.tv_sec) * 1000000 +
               (after.tv_usec - before.tv_usec);
	usec -= timer_overhead;

	sec = (float) usec / 1000000.0;
	throughput = ((float) (num_files * num_cpus) / sec);
	printf("\nOverall benchmark (%s) : %7.3f sec throughput : %7.3f files/sec\n",
		(sync_when == FSYNC_CREATE)?"no sleep used":"including sleep",
		sec, throughput);

	delete_dirs(topdir, num_dirs);

	if (sync_when != SYNC_NONE)
		sync();

	return 0;
}

void *run_benchmark(void *arg)
{
	unsigned long usec;
	float sec, total_sec;
	float throughput, avg_throughput;

	int cpu = (uintptr_t)arg;

	if (setaffinity(cpu) < 0) {
		printf("setaffinity failed for cpu %d\n", cpu);
		return 0;
	}

	if (!use_fork)
		pthread_barrier_wait(&bar);

	/*
	 * Now we just do the tests in sequence, printing the timing
	 * statistics as we go.
	 */
	total_sec = 0;

	if (verbose) {
		printf("Test            Time(sec)       Files/sec\n");
		printf("----            ---------       ---------\n");
	}

	usec = create_files(topdir, buf, cpu);

	if (verbose) {
		sec = (float) usec / 1000000.0;
		throughput = ((float) num_files / sec);
		printf("create_files\t%7.3f\t\t%7.3f\n", sec, throughput);
		fflush(stdout);
		total_sec += sec;
	}

	if (sync_when == SYNC_CREATE_UNLINK) {
		sleep(2);

		usec = sync_files();

		if (verbose) {
			sec = (float) usec / 1000000.0;
			throughput = ((float) num_files / sec);
			printf("sync_files_1\t%7.3f\t\t%7.3f\n", sec, throughput);
			fflush(stdout);
			total_sec += sec;
		}
	}

	usec = read_files(topdir, buf, cpu);

	if (verbose) {
		sec = (float) usec / 1000000.0;
		throughput = ((float) num_files / sec);
		printf("read_files\t%7.3f\t\t%7.3f\n", sec, throughput);
		fflush(stdout);
		total_sec += sec;
	}

	usec = unlink_files(topdir, buf, cpu);

	if (verbose) {
		sec = (float) usec / 1000000.0;
		throughput = ((float) num_files / sec);
		printf("unlink_files\t%7.3f\t\t%7.3f\n", sec, throughput);
		fflush(stdout);
		total_sec += sec;
	}

	if (sync_when == SYNC_UNLINK || sync_when == SYNC_CREATE_UNLINK) {
		sleep(2);

		usec = sync_files();

		if (verbose) {
			sec = (float) usec / 1000000.0;
			throughput = ((float) num_files / sec);
			printf("sync_files_2\t%7.3f\t\t%7.3f\n", sec, throughput);
			fflush(stdout);
			total_sec += sec;
		}
	}

	if (verbose) {
		avg_throughput = ((float) num_files / total_sec);
		printf("thread %d total \t%7.3f\t\t%7.3f\n", cpu, total_sec, avg_throughput);
		fflush(stdout);
	}

	return 0;
}

void create_dirs(const char *topdir, unsigned long num_dirs)
{
	int ret, fd;
	unsigned long i, j;
	char dir[128], sub_dir[128];

	if (per_cpu_dirs) {
		for (j = 0; j < num_cpus; j++) {
			setaffinity(j);
			snprintf(sub_dir, 128, "%s/cpu-%ld", topdir, j);
			if ((ret = mkdir(sub_dir, 0777)) != 0)
				die("mkdir %s failed %d\n", sub_dir, ret);

			fd = open(sub_dir, O_RDONLY|O_DIRECTORY);
			if (fd >= 0) {
				if ((ret = fsync(fd)) < 0)
					die("fsync %s failed %d\n", sub_dir, ret);
				close(fd);
			}

			fd = open(topdir, O_RDONLY|O_DIRECTORY);
			if (fd >= 0) {
				if ((ret = fsync(fd)) < 0)
					die("fsync %s failed %d\n", topdir, ret);
				close(fd);
			}

			for (i = 0; i < num_dirs; i++) {
				snprintf(dir, 128, "%s/cpu-%ld/dir-%ld", topdir, j, i);
				if ((ret = mkdir(dir, 0777)) != 0)
					die("mkdir %s failed %d\n", dir, ret);

				fd = open(dir, O_RDONLY|O_DIRECTORY);
				if (fd >= 0) {
					if ((ret = fsync(fd)) < 0)
						die("fsync %s failed %d\n", dir, ret);
					close(fd);
				}

			}

			fd = open(sub_dir, O_RDONLY|O_DIRECTORY);
			if (fd >= 0) {
				if ((ret = fsync(fd)) < 0)
					die("fsync %s failed %d\n", sub_dir, ret);
				close(fd);
			}
		}
	} else {
		for (i = 0; i < num_dirs; i++) {
			setaffinity(i % num_cpus);
			snprintf(dir, 128, "%s/dir-%ld", topdir, i);
			if ((ret = mkdir(dir, 0777)) != 0)
				die("mkdir %s failed %d\n", dir, ret);

			fd = open(dir, O_RDONLY|O_DIRECTORY);
			if (fd >= 0) {
				if ((ret = fsync(fd)) < 0)
					die("fsync %s failed %d\n", dir, ret);
				close(fd);
			}

			fd = open(topdir, O_RDONLY|O_DIRECTORY);
			if (fd >= 0) {
				if ((ret = fsync(fd)) < 0)
					die("fsync %s failed %d\n", topdir, ret);
				close(fd);
			}
		}
	}

	// Break CPU affinity.
	setaffinity(-1);
}


void delete_dirs(const char *topdir, unsigned long num_dirs)
{
	int ret, fd;
	unsigned long i, j;
	char dir[128], sub_dir[128];

	if (per_cpu_dirs) {
		for (j = 0; j < num_cpus; j++) {
			setaffinity(j);

			snprintf(sub_dir, 128, "%s/cpu-%ld", topdir, j);

			for (i = 0; i < num_dirs; i++) {
				snprintf(dir, 128, "%s/cpu-%ld/dir-%ld", topdir, j, i);

				fd = open(dir, O_RDONLY|O_DIRECTORY);
				if (fd >= 0) {
					if ((ret = fsync(fd)) < 0)
						die("fsync %s failed %d\n", dir, ret);
					close(fd);
				}

				if ((ret = rmdir(dir)) != 0)
					die("rmdir %s failed %d\n", dir, ret);
			}

			fd = open(sub_dir, O_RDONLY|O_DIRECTORY);
			if (fd >= 0) {
				if ((ret = fsync(fd)) < 0)
					die("fsync %s failed %d\n", sub_dir, ret);
				close(fd);
			}

			if ((ret = rmdir(sub_dir)) != 0)
				die("rmdir %s failed %d\n", sub_dir, ret);

			fd = open(topdir, O_RDONLY|O_DIRECTORY);
			if (fd >= 0) {
				if ((ret = fsync(fd)) < 0)
					die("fsync %s failed %d\n", topdir, ret);
				close(fd);
			}
		}
	} else {
		for (i = 0; i < num_dirs; i++) {
			setaffinity(i % num_cpus);
			snprintf(dir, 128, "%s/dir-%ld", topdir, i);

			fd = open(dir, O_RDONLY|O_DIRECTORY);
			if (fd >= 0) {
				if ((ret = fsync(fd)) < 0)
					die("fsync %s failed %d\n", dir, ret);
				close(fd);
			}

			if ((ret = rmdir(dir)) != 0)
				die("rmdir %s failed %d\n", dir, ret);

			fd = open(topdir, O_RDONLY|O_DIRECTORY);
			if (fd >= 0) {
				if ((ret = fsync(fd)) < 0)
					die("fsync %s failed %d\n", topdir, ret);
				close(fd);
			}
		}
	}

	// Break CPU affinity.
	setaffinity(-1);
}


unsigned long create_files(const char *topdir, char *buf, int cpu)
{
	int fd;
	unsigned long i, j;
	ssize_t size;
	char filename[128];
	unsigned long time;
	struct timeval before, after;

	time = 0;
	gettimeofday ( &before, NULL );
	/* Create phase */
	for (i = 0, j = 0; i < num_files; i++) {
		if (per_cpu_dirs)
			snprintf(filename, 128, "%s/cpu-%d/dir-%ld/file-%d-%ld",
				topdir, cpu, j, cpu, i);
		else
			snprintf(filename, 128, "%s/dir-%ld/file-%d-%ld",
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

unsigned long read_files(const char *topdir, char *buf, int cpu)
{
	int fd;
	unsigned long i, j;
	ssize_t size;
	char filename[128];
	unsigned long time;
	struct timeval before, after;

	time = 0;
	gettimeofday ( &before, NULL );
	/* Read phase */
	for (i = 0, j = 0; i < num_files; i++) {
		if (per_cpu_dirs)
			snprintf(filename, 128, "%s/cpu-%d/dir-%ld/file-%d-%ld",
				topdir, cpu, j, cpu, i);
		else
			snprintf(filename, 128, "%s/dir-%ld/file-%d-%ld",
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

unsigned long unlink_files(const char *topdir, char *buf, int cpu)
{
	int ret;
	unsigned long i, j;
	char filename[128];
	unsigned long time;
	struct timeval before, after;

	time = 0;
	gettimeofday ( &before, NULL );
	/* Unlink phase */
	for (i = 0, j = 0; i < num_files; i++) {
		if (per_cpu_dirs)
			snprintf(filename, 128, "%s/cpu-%d/dir-%ld/file-%d-%ld",
				topdir, cpu, j, cpu, i);
		else
			snprintf(filename, 128, "%s/dir-%ld/file-%d-%ld",
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

unsigned long sync_files(void)
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
