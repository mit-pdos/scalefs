#define _GNU_SOURCE
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "libutil.h"

#if !defined(XV6_SOURCE)
#include <sys/types.h>
#include <sys/stat.h>
#endif

#define NUM 1000

void rename_test(void)
{
  unsigned long i;
  int fd;
  char dir_path[128], file_path[128];
  char buf[512];

  for (i = 0; i < 512; i++)
    buf[i] = 'a';

  for (i = 0; i < NUM; i++) {
    printf("Creating directories and files: %lu\n", i);

    snprintf(dir_path, 128, "sub-dir-%lu", i);
    if (mkdir(dir_path, 0777) < 0)
      die("mkdir %s failed\n", dir_path);

    snprintf(file_path, 128, "%s/f%lu", dir_path, i);
    fd = open(file_path, O_RDWR | O_CREAT | O_EXCL,
              S_IWUSR|S_IRUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
    if (fd < 0)
      die("open file %s failed\n", file_path);

    if (write(fd, buf, 512) != 512)
      die("write failed\n");

    if (fsync(fd) < 0)
      die("fsync %s failed\n", file_path);

    close(fd);

    fd = open(dir_path, O_RDONLY | O_DIRECTORY);
    if (fd < 0)
      die("open %s failed\n", dir_path);

    if (fsync(fd) < 0)
      die("fsync %s failed\n", dir_path);

    close(fd);
  }

  sync();

  for (i = 0; i < NUM; i++) {
    char src_path[128], dst_path[128], src_dir[128], dst_dir[128];

    snprintf(src_dir, 128, "sub-dir-%lu", i);
    snprintf(dst_dir, 128, "sub-dir-%lu", (i + 1) % NUM);
    snprintf(src_path, 128, "%s/f%lu", src_dir, i);
    snprintf(dst_path, 128, "%s/f%lu", dst_dir, i);

    if (rename(src_path, dst_path) < 0)
      die("rename %s -> %s failed\n", src_path, dst_path);

    printf("rename %s to %s (%lu) succeeded\n", src_path, dst_path, i);
  }

  snprintf(file_path, 128, "sub-dir-0");
  fd = open("sub-dir-0", O_RDONLY, S_IRUSR|S_IRGRP|S_IROTH);

  if (fd < 0)
    die("open %s failed\n", file_path);

  if (fsync(fd) < 0)
    die("fsync %s failed\n", file_path);

  close(fd);
}


int main(void)
{
  rename_test();
  return 0;
}
