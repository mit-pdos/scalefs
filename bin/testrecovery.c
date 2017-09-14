#define _GNU_SOURCE
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#if !defined(XV6_USER)
#include <sys/types.h>
#include <sys/stat.h>
#endif

#include "libutil.h"

#define MAXCPUS 80
#define NUM_ITERS 50

void usage(char *prog)
{
  fprintf(stderr, "Usage: %s [-c cpus] [-o] <working directory>\n", prog);
  fprintf(stderr, "-o overflow some of the per-core journals\n");
  exit(1);
}

int main(int argc, char **argv)
{
  char ch;
  extern char *optarg;
  extern int optind;

  char *topdir;
  int num_cpus, overflow_journal;

  /* Parse and test the arguments. */
  if (argc < 2)
    usage(argv[0]);

  /* Set the defaults */
  num_cpus = 1;
  overflow_journal = 0;

  while ((ch = getopt(argc, argv, "c:o")) != -1) {
    switch (ch) {
      case 'c':
        num_cpus = atoi(optarg);
        if (num_cpus > MAXCPUS)
          num_cpus = MAXCPUS;
        break;

      case 'o':
        overflow_journal = 1;
        break;

      default:
        usage(argv[0]);
        exit(1);
    }
  }

  argc -= optind;
  argv += optind;

  topdir = argv[0];

  char buf[128], dirname[128], filename[128];
  int fd, filenum = 0;

  setaffinity(0);
  snprintf(dirname, sizeof(dirname), "%s/testdir", topdir);
  if (mkdir(dirname, 0777) != 0)
    die("mkdir %s failed\n", dirname);

  sync();

  // Create files in a shared directory and fsync the files and the directory
  // from different CPUs, thus causing dependencies between the transactions
  // across a number of per-core journals. Filenames are numbered sequentially
  // to enable easy verification of the correctness of crash-recovery, by simple
  // inspection of the filesystem state with 'ls'. Further, each file contains
  // uniquely identifiable contents, which can be used to verify that the
  // filesystem uses appropriate write-barriers (disk-flush), especially when
  // using multiple disks.

  // When overflow_journal is true, the code randomizes the number of files
  // fsynced from each CPU, thus causing some of the per-core journals to get
  // filled faster than others, which in turn causes some of the dependent
  // transactions to get applied and deleted from their journals. This helps us
  // test more complex filesystem recovery scenarios involving dependent
  // transactions and partially applied per-core journals.

  // The following scenarios (after reboot) indicate incorrect recovery:
  // - The new directory is missing.
  // - Some of the newly created files in the new directory are missing
  //   (for correct recovery, the number of files after reboot must match
  //    the number indicated by the last filename that was persisted before
  //    the crash).
  // - Some of the new files don't have their expected file contents.

  if (!overflow_journal) {

    printf("Running %s without overflowing journals...\n", argv[0]);

    for (int cpu = 0; cpu < num_cpus; cpu++) {
      setaffinity(cpu);
      snprintf(filename, sizeof(filename), "%s/file%d.txt", dirname, filenum);
      memset(buf, 0, sizeof(buf));
      snprintf(buf, sizeof(buf), "filenum: %d cpu: %d\n", filenum, cpu);

      printf("Filename %s cpu %d ... ", filename, cpu);

      fd = open(filename, O_CREAT|O_WRONLY, 0666);
      if (fd < 0)
        die("open %s failed\n", filename);

      if (write(fd, buf, sizeof(buf)) != sizeof(buf))
        die("write %s failed\n", filename);

      if (fsync(fd) < 0)
        die("fsync %s failed\n", filename);
      close(fd);

      fd = open(dirname, O_RDONLY|O_DIRECTORY);
      if (fd >= 0) {
        if (fsync(fd) < 0)
          die("fsync %s failed\n", dirname);
        close(fd);
      }

      printf("done\n");
      filenum++;
    }

    printf("Running %s without overflowing journals -- complete\n", argv[0]);

  } else {

    printf("Running %s with journal overflow...\n", argv[0]);

    for (int k = 0; k < NUM_ITERS; k++) {

      printf("Iteration %d/%d starting...\n", k, NUM_ITERS);

      for (int cpu = 0; cpu < num_cpus; cpu++) {
        setaffinity(cpu);

        int iters = (unsigned int)rand() % 10;

        for (int i = 0; i < iters; i++) {
          snprintf(filename, sizeof(filename), "%s/file%d.txt", dirname, filenum);
          memset(buf, 0, sizeof(buf));
          snprintf(buf, sizeof(buf), "filenum: %d cpu: %d", filenum, cpu);

          printf("Filename %s cpu %d ... ", filename, cpu);

          fd = open(filename, O_CREAT|O_WRONLY, 0666);
          if (fd < 0)
            die("open %s failed\n", filename);

          if (write(fd, buf, sizeof(buf)) != sizeof(buf))
            die("write %s failed\n", filename);

          if (fsync(fd) < 0)
            die("fsync %s failed\n", filename);
          close(fd);

          fd = open(dirname, O_RDONLY|O_DIRECTORY);
          if (fd >= 0) {
            if (fsync(fd) < 0)
              die("fsync %s failed\n", dirname);
            close(fd);
          }

          printf("done\n");
          filenum++;
        }
      }

      printf("Iteration %d/%d complete\n", k, NUM_ITERS);
    }

    printf("Running %s with journal overflow -- complete\n", argv[0]);
  }

  return 0;
}
