#include "types.h"
#include "user.h"
#include "lib.h"
#include "amd64.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static FILE stdout_s{ .fd = 1 };
static FILE stderr_s{ .fd = 2 };
FILE *stdout = &stdout_s;
FILE *stderr = &stderr_s;

FILE*
fdopen(int fd, const char *mode)
{
  FILE *fp;

  if (mode[0] != 'r')
    return 0;
  fp = (FILE*)malloc(sizeof(*fp));
  if (fp == 0)
    return 0;
  if (fstat(fd, &fp->stat))
    return 0;
  fp->fd = fd;
  fp->off = 0;
  fp->poff = 0;
  fp->pfill = mode[1] == 'p';
  return fp;
}

int
fclose(FILE *fp)
{
  int r;

  r = close(fp->fd);
  free(fp);
  return r;
}

size_t
fread(void *ptr, size_t size, size_t nmemb, FILE *fp)
{
  ssize_t r;

  r = pread(fp->fd, ptr, size*nmemb, fp->off);
  if (r < 0) {
    fp->err = 1;
    return 0;
  } else if (r == 0) {
    fp->eof = 1;
    return 0;
  }
  fp->off += r;
  return r;
}


char*
fgets(char *buf, int max, FILE *fp)
{
  int cc;
  char *ptr;

  cc = fread(buf, max-1, 1, fp);
  if (cc < 1)
    return NULL;

  buf[cc] = '\0';

  int last = 0;
  if ((ptr = strchr(buf, '\n'))) {
    *(++ptr) = '\0';
    last = ptr - buf;
    fp->off -= (cc - last);
  }
  return buf;
}

int
fseek(FILE *fp, long offset, int whence)
{
  switch (whence) {
  case SEEK_SET:
    fp->off = offset;
    break;

  case SEEK_CUR:
    fp->off += offset;
    break;

  case SEEK_END:
    long end = lseek(fp->fd, 0L, SEEK_END);
    if (offset < 0 && end + offset < 0)
      return -1; // Attempt to seek before the beginning of the file.
  }

  return 0;
}

off_t
ftell(FILE *fp)
{
  return fp->off;
}

void
rewind(FILE *fp)
{
  fseek(fp, 0L, SEEK_SET);
  assert(ftell(fp) == 0);
  fp->eof = 0;
  fp->err = 0;
}

int
feof(FILE *fp)
{
  return fp->eof;
}

int
ferror(FILE *fp)
{
  return fp->err;
}

int
fflush(FILE* stream)
{
  // We don't implement buffering for writable files, so this does
  // nothing.
  return 0;
}
