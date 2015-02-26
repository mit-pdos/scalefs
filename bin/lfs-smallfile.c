#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#define NUMFILES	10000
#define FILESIZE	1024

void usage(char *prog)
{
	fprintf(stderr, "Usage: %s <working directory>\n", prog);
	exit(1);
}

int main(int argc, char **argv)
{
	int i;
	int fd;
	int ret;
	char *buf;
	ssize_t size;
	char *dirname;
	char filename[128];

	if (argc != 2) {
		usage(argv[0]);
	}
	dirname = argv[1];

	buf = (char *)malloc(FILESIZE);
	if (!buf) {
		fprintf(stderr, "ERROR: Failed to allocate buffer");
		exit(1);
	}

	/* Create phase */
	for (i = 0; i < NUMFILES; i++) {
		snprintf(filename, FILESIZE, "%s/file-%d", dirname,  i);
		fd = open(filename, O_WRONLY | O_CREAT | O_EXCL, S_IWUSR | S_IRUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
		if (fd == -1) {
			perror("open");
			exit(1);
		}

		size = write(fd, buf, FILESIZE);
		if (size == -1) {
			perror("write");
			exit(1);
		}

		close(fd);
	}

	/* Read phase */
	for (i = 0; i < NUMFILES; i++) {
		snprintf(filename, FILESIZE, "%s/file-%d", dirname, i);
		fd = open(filename, O_RDONLY);
		if (fd == -1) {
			perror("open");
			exit(1);
		}

		size = read(fd, buf, FILESIZE);
		if (size == -1) {
			perror("read");
			exit(1);
		}

		close(fd);
	}

	/* Unlink phase */
	for (i = 0; i < NUMFILES; i++) {
		snprintf(filename, FILESIZE, "%s/file-%d", dirname, i);
		ret = unlink(filename);
		if (ret == -1) {
			perror("unlink");
			exit(1);
		}
	}
}
