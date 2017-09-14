#define _GNU_SOURCE
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "libutil.h"

#if !defined(XV6_USER)
#include <sys/types.h>
#include <sys/stat.h>
#endif

int main(int argc, char **argv)
{
  char dirA[128], dirB[128], dirC[128], dirD[128];

  snprintf(dirA, sizeof(dirA), "/dirA");
  printf("%s: creating %s\n", argv[0], dirA);
  if (mkdir(dirA, 0777) != 0)
    die("mkdir %s failed\n", dirA);

  snprintf(dirB, sizeof(dirB), "/dirA/dirB");
  printf("%s: creating %s\n", argv[0], dirB);
  if (mkdir(dirB, 0777) != 0)
    die("mkdir %s failed\n", dirB);

  snprintf(dirC, sizeof(dirC), "/dirA/dirB/dirC");
  printf("%s: creating %s\n", argv[0], dirC);
  if (mkdir(dirC, 0777) != 0)
    die("mkdir %s failed\n", dirC);

  snprintf(dirD, sizeof(dirD), "/dirA/dirB/dirC/dirD");
  printf("%s: creating %s\n", argv[0], dirD);
  if (mkdir(dirD, 0777) != 0)
    die("mkdir %s failed\n", dirD);

  // Create this directory structure on the disk.
  printf("%s: persisting the directory structure with a sync\n", argv[0]);
  sync();

  // Now perform the cross-directory directory renames in MemFS.
  printf("%s: renaming %s to %s\n", argv[0], dirC, "/dirC");
  if (rename(dirC, "/dirC") < 0)
    die("rename %s -> %s failed\n", dirC, "/dirC");

  printf("%s: renaming %s to %s\n", argv[0], dirB, "/dirC/dirD/dirB");
  if (rename(dirB, "/dirC/dirD/dirB") < 0)
    die("rename %s -> %s failed\n", dirB, "/dirC/dirD/dirB");

  // Now fsyncing dirD should not cause a loop on the disk.
  int fd = open("/dirC/dirD", O_RDONLY|O_DIRECTORY);
  if (fd < 0)
    die("open %s failed\n", "/dirC/dirD");

  printf("%s: fsyncing %s\n", argv[0], "/dirC/dirD");
  if (fsync(fd) < 0)
    die("fsync %s failed\n", "/dirC/dirD");

  close(fd);

  printf("%s: all done.\n", argv[0]);
  return 0;
}
