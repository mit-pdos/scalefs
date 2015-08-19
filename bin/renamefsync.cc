#include "user.h"
#include <fcntl.h>
#include <sys/stat.h>
#include "string.h"
#include "fs.h"
#include <stdio.h>

void fsync_run() {
  int fd_cwd, fd_dir1, fd_file1, fd_dir2;
  char buf[10];

  if ((fd_cwd = open(".", 0)) < 0)
    die("error: could not open .");

  if (mkdir("testdir1", 0777) < 0)
    die("error: mkdir testdir1 failed");
  if ((fd_dir1 = open("testdir1", 0)) < 0)
    die("error: could not open testdir1");

  fsync(fd_dir1);
  fsync(fd_cwd);

  if (mkdir("testdir2", 0777) < 0)
    die("error: mkdir testdir2 failed");
  if ((fd_dir2 = open("testdir2", 0)) < 0)
    die("error: could not open testdir2");

  fsync(fd_dir2);
  fsync(fd_cwd);

  if ((fd_file1 = open("testdir1/testfile1", O_RDWR | O_CREAT)) < 0)
    die("error: could not open testfile1");

  memset(buf, 'x', 10);
  if (write(fd_file1, buf, 10) != 10)
    die("error: write to testdir1/testfile1 failed");

  if (rename("testdir1/testfile1", "testdir2/testfile1") < 0)
      die("error: rename from testdir1/testfile1 to testdir2/testfile1 failed");

  fsync(fd_file1);
  fsync(fd_dir1);
  fsync(fd_dir2);

  close(fd_file1);
  close(fd_dir1);
  close(fd_dir2);
  close(fd_cwd);

  printf("renamefsync: success\n");
}

int main(int argc, char *argv[]) {
  fsync_run();
  return 0;
}
