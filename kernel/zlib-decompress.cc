#include "types.h"
#include "kernel.hh"
#include "zlib.h"

void *
zlib_alloc(void *opaque, unsigned count, unsigned nbytes)
{
  u64 *x = (u64 *) kmalloc(count * nbytes + sizeof(u64), (char *)opaque);
  *x = count * nbytes;
  return x+1;
}

void
zlib_free(void *opaque, void *p)
{
  u64 *x = (u64 *) p;
  kmfree(x-1, x[-1] + sizeof(u64));
}

int
zlib_decompress(unsigned char *src, u64 srclen,
                unsigned char *dst, u64 dstlen)
{
  z_stream stream;
  int err;

  stream.zalloc = zlib_alloc;
  stream.zfree  = zlib_free;
  stream.opaque = (void *)"zlib";

  stream.next_in = src;
  stream.avail_in = srclen;
  stream.next_out = dst;
  stream.avail_out = dstlen;

  err = inflateInit(&stream);
  if (err != Z_OK)
    panic("zlib: inflateInit() failed!\n");

  err = inflate(&stream, Z_FINISH);
  if (err != Z_STREAM_END) {
    inflateEnd(&stream);
    if (err == Z_NEED_DICT || (err == Z_BUF_ERROR && stream.avail_in == 0))
      panic("zlib: inflate() Z_DATA_ERROR!\n");
    panic("zlib: inflate() failed!\n");
  }

  if (stream.total_out != dstlen)
    panic("zlib: inflate() total_out != dstlen\n");

  inflateEnd(&stream);
  return 0;
}
