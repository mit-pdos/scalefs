#include "user.h"
#include <fcntl.h>
#include <sys/stat.h>
#include "string.h"
#include "fs.h"

void sync_run() {
  int fd, fd1;
  char buf[4096];

  if (mkdir("testdir1", 0777) < 0)
    die("error: mkdir testdir1 failed");
  if ((fd = open("testdir1", 0)) < 0)
    die("error: could not open testdir1");
  close(fd);

  if (mkdir("testdir2", 0777) < 0)
    die("error: mkdir testdir2 failed");
  if ((fd = open("testdir2", 0)) < 0)
    die("error: could not open testdir2");
  close(fd);

  if ((fd = open("testfile1", O_RDWR | O_APPEND)) < 0)
    die("error: could not open testfile1");
  memset(buf, 'x', 4096);
  if (write(fd, buf, 4096) != 4096)
    die("error: write to testfile1 failed");

  if ((fd1 = open("testfile2", O_RDWR | O_CREAT)) < 0)
    die("error: could not open testfile2");
  memset(buf, 'y', 4096);
  if (write(fd1, buf, 4096) != 4096)
    die("error: write to testfile2 failed");

  if (rename("testfile2", "testdir1/testfile2") < 0) 
      die("error: rename failed");

  close(fd1);

  close(fd);

  if (mkdir("testdir1/testsubdir", 0777) < 0)
    die("error: mkdir failed");
  if ((fd = open("testdir1", 0)) < 0)
    die("error: could not open testdir1");
  close(fd);

  sync();
}

void sync_verify() {
  int fd, num_entries = 0;
  char namebuf[DIRSIZ+1];
  char *prev = nullptr;

  if ((fd = open("testdir1", 0)) < 0)
    die("check failed: could not open testdir1");
  while(readdir(fd, prev, namebuf) > 0) {
    prev = namebuf;
    num_entries++;
  }
  assert(num_entries == 4);
  close(fd);

  num_entries = 0;
  prev = nullptr;
  if ((fd = open("testdir2", 0)) < 0)
    die("check failed: could not open testdir2");
  while(readdir(fd, prev, namebuf) > 0) {
    prev = namebuf;
    num_entries++;
  }
  assert(num_entries == 2);
  close(fd);

  if ((fd = open("testdir1/testsubdir", 0)) < 0)
    die("check failed: could not open testdir1/testsubdir");
  close(fd);

  if ((fd = open("testdir1/testfile2", 0)) < 0)
    die("check failed: could not open testdir1/testfile2");
  close(fd);

  if ((fd = open("testfile1", 0)) < 0)
    die("check failed: could not open testfile1");
  close(fd);
}

int main(int argc, char *argv[]) {
  if (argc <= 1)
    sync_run();
  else
    sync_verify();
  return 0;
}
