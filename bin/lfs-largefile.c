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

#define FILESIZE	1024*1024*100
#define IOSIZE		4096

#define NTEST		100000 /* No. of tests of gettimeofday() */

#define MAXCPUS		128

int num_cpus;
unsigned long timer_overhead;

char *topdir, *buf;
unsigned long file_size;

pthread_t tid[MAXCPUS];
pthread_barrier_t bar;

int verbose;
int per_cpu_dirs, use_fork;

void *run_createfile_benchmark(void *arg);
void *run_overwritefile_benchmark(void *arg);
void create_dirs(const char *topdir);
void delete_dirs(const char *topdir);
void create_largefile(const char *topdir, char *buf, int cpu);
void overwrite_largefile(const char *topdir, char *buf, int cpu);
void unlink_all_files(const char *topdir);

void usage(char *prog)
{
	fprintf(stderr, "Usage: %s [-c cpus] [-p] [-f] [-i size] <working directory>\n", prog);
	fprintf(stderr, "-p creates the largefiles under per-cpu sub-directories\n");
	fprintf(stderr, "-f uses fork instead of pthreads\n");
	fprintf(stderr, "-i creates a file of size MB (default 100)\n");
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
	file_size = FILESIZE;
	num_cpus = 1;
	per_cpu_dirs = 0;
	use_fork = 0;
	verbose = 0;

	while ((ch = getopt(argc, argv, "c:pfi:")) != -1) {
		switch (ch) {
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
			case 'i':
				file_size = atoi(optarg) * 1024 * 1024;
				break;
			default:
				usage(argv[0]);
				exit(1);
		}
	}
	argc -= optind;
	argv += optind;

	topdir = argv[0];

	buf = (char *)malloc(IOSIZE);
	if (!buf) {
		fprintf(stderr, "ERROR: Failed to allocate buffer");
		exit(1);
	}

	for (i = 0; i < IOSIZE; i++)
		buf[i] = (char) i;

	create_dirs(topdir);

	sleep(2);
	sync();

	/* Compute the overhead of the gettimeofday() call */

	gettimeofday(&before, NULL);
	for (i = 0; i < NTEST; i++)
		gettimeofday(&dummy, NULL);
	gettimeofday(&after, NULL);

	timer_overhead = (after.tv_sec - before.tv_sec) * 1000000 +
			 (after.tv_usec - before.tv_usec);
	timer_overhead /= NTEST;

	// Print out benchmark parameters.
	printf("\n\n\n");
	fflush(stdout);
	printf("Running LFS-Largefile test on %s, on %d CPUs\n", topdir, num_cpus);
	printf("File Size = %lu MB\n", file_size/(1024*1024));
	printf("Directories are: %s\n", per_cpu_dirs ? "per-cpu" : "shared");
	printf("Running %d parallel benchmark instance(s) using %s\n",
                num_cpus, use_fork ? "fork" : "pthreads");
	fflush(stdout);

	/* Time the creation of largefile */
	usec = 0;
	gettimeofday(&before, NULL);

	if (use_fork) {
		for (i = 0; i < num_cpus; i++) {
			forkret = fork();
			if (forkret == 0) {
				run_createfile_benchmark((void *) i);
				return 0; // Each child exits here.
			}
		}

		// Parent
		for (i = 0; i < num_cpus; i++)
			waitpid(-1, NULL, 0);

		// Fall-through to print out the statistics and
		// cleanup the benchmark.
	} else {
		pthread_barrier_init(&bar, 0, num_cpus);
		for (i = 0; i < num_cpus; i++)
			pthread_create(&tid[i], NULL, run_createfile_benchmark, (void *) i);

		for (i = 0; i < num_cpus; i++)
			pthread_join(tid[i], NULL);
	}

	gettimeofday(&after, NULL);
	usec = (after.tv_sec - before.tv_sec) * 1000000 +
               (after.tv_usec - before.tv_usec);
	usec -= timer_overhead;

	sec = (float) usec / 1000000.0;
	throughput = (((float)file_size / (1024*1024)) * num_cpus) / sec;
	printf("\nCreate-file benchmark : %7.3f sec throughput : %7.3f MB/sec\n",
		sec, throughput);

	/* Time the overwrite of largefile */

	for (i = 0; i < IOSIZE; i++)
		buf[i] = 'b';

	usec = 0;
	gettimeofday(&before, NULL);

	if (use_fork) {
		for (i = 0; i < num_cpus; i++) {
			forkret = fork();
			if (forkret == 0) {
				run_overwritefile_benchmark((void *) i);
				return 0; // Each child exits here.
			}
		}

		// Parent
		for (i = 0; i < num_cpus; i++)
			waitpid(-1, NULL, 0);

		// Fall-through to print out the statistics and
		// cleanup the benchmark.
	} else {
		pthread_barrier_init(&bar, 0, num_cpus);
		for (i = 0; i < num_cpus; i++)
			pthread_create(&tid[i], NULL, run_overwritefile_benchmark, (void *) i);

		for (i = 0; i < num_cpus; i++)
			pthread_join(tid[i], NULL);
	}

	gettimeofday(&after, NULL);
	usec = (after.tv_sec - before.tv_sec) * 1000000 +
               (after.tv_usec - before.tv_usec);
	usec -= timer_overhead;

	sec = (float) usec / 1000000.0;
	throughput = (((float)file_size / (1024*1024)) * num_cpus) / sec;
	printf("Overwrite-file benchmark : %7.3f sec throughput : %7.3f MB/sec\n",
		sec, throughput);

	unlink_all_files(topdir);
	delete_dirs(topdir);

	sleep(2);
	sync();

	return 0;
}

void *run_createfile_benchmark(void *arg)
{
	int cpu = (uintptr_t)arg;

	if (setaffinity(cpu) < 0) {
		printf("setaffinity failed for cpu %d\n", cpu);
		return 0;
	}

	if (!use_fork)
		pthread_barrier_wait(&bar);

	create_largefile(topdir, buf, cpu);
	return 0;
}

void *run_overwritefile_benchmark(void *arg)
{
	int cpu = (uintptr_t)arg;

	if (setaffinity(cpu) < 0) {
		printf("setaffinity failed for cpu %d\n", cpu);
		return 0;
	}

	if (!use_fork)
		pthread_barrier_wait(&bar);

	overwrite_largefile(topdir, buf, cpu);
	return 0;
}

void create_dirs(const char *topdir)
{
	int ret, fd;
	char dir[128], sub_dir[128];

	if (per_cpu_dirs) {
		for (int i = 0; i < num_cpus; i++) {
			setaffinity(i);
			snprintf(sub_dir, 128, "%s/cpu-%d", topdir, i);
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
		}
	} else {
		snprintf(dir, 128, "%s/dir", topdir);
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

	// Break CPU affinity.
	setaffinity(-1);
}


void delete_dirs(const char *topdir)
{
	int ret, fd;
	char dir[128], sub_dir[128];

	if (per_cpu_dirs) {
		for (int i = 0; i < num_cpus; i++) {
			setaffinity(i);

			snprintf(sub_dir, 128, "%s/cpu-%d", topdir, i);

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
		snprintf(dir, 128, "%s/dir", topdir);

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

	// Break CPU affinity.
	setaffinity(-1);
}


void create_largefile(const char *topdir, char *buf, int cpu)
{
	char filename[128];

	/* Create and fsync a large file. */
	if (per_cpu_dirs)
		snprintf(filename, 128, "%s/cpu-%d/largefile",
			topdir, cpu);
	else
		snprintf(filename, 128, "%s/dir/largefile-%d",
			topdir, cpu);

	int fd = open(filename, O_WRONLY | O_CREAT | O_EXCL,
			S_IWUSR | S_IRUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
	if (fd < 0)
		die("open");

	int count = file_size/IOSIZE;
	for (int i = 0; i < count; i++) {
		if (write(fd, buf, IOSIZE) != IOSIZE)
			die("write");
	}

	if (fsync(fd) < 0)
		die("fsync");

	close(fd);
}

void overwrite_largefile(const char *topdir, char *buf, int cpu)
{
	char filename[128];

	/* Create and fsync a large file. */
	if (per_cpu_dirs)
		snprintf(filename, 128, "%s/cpu-%d/largefile",
			topdir, cpu);
	else
		snprintf(filename, 128, "%s/dir/largefile-%d",
			topdir, cpu);

	int fd = open(filename, O_WRONLY);
	if (fd < 0)
		die("open");

	if (lseek(fd, 0, SEEK_SET) < 0)
		die("lseek");

	int count = file_size/IOSIZE;
	for (int i = 0; i < count; i++) {
		if (write(fd, buf, IOSIZE) != IOSIZE)
			die("write");
	}

	if (fsync(fd) < 0)
		die("fsync");

	close(fd);
}

void unlink_all_files(const char *topdir)
{
	char filename[128];

	for (int cpu = 0; cpu < num_cpus; cpu++) {
		setaffinity(cpu);
		if (per_cpu_dirs)
			snprintf(filename, 128, "%s/cpu-%d/largefile",
				topdir, cpu);
		else
			snprintf(filename, 128, "%s/dir/largefile-%d",
				topdir, cpu);

		if (unlink(filename) < 0)
			die("unlink");
	}

	setaffinity(-1);
}
