#include "types.h"
#include "kernel.hh"
#include "zlib.h"
#include "fs.h"

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

#define CHUNK BSIZE

int
zlib_decompress(unsigned char *src, u64 srclen, u64 dstlen,
                void (*copy_output)(const char *buf, u64 offset, u64 size))
{
  unsigned char out[CHUNK];
  z_stream stream;
  u64 have;
  int err;

  stream.zalloc = zlib_alloc;
  stream.zfree  = zlib_free;
  stream.opaque = (void *)"zlib";

  stream.next_in = src;
  stream.avail_in = srclen;
  stream.next_out = out;
  stream.avail_out = CHUNK;

  err = inflateInit(&stream);
  if (err != Z_OK)
    panic("%s: inflateInit() failed!\n", __func__);

  u64 i = 0;
  do {
    if (srclen == 0)
      break;
    stream.avail_in = srclen;
    stream.next_in = src;

    // Run inflate() on input until the output buffer is not full.
    do {
      stream.avail_out = CHUNK;
      stream.next_out = out;
      err = inflate(&stream, Z_NO_FLUSH);
      assert(err != Z_STREAM_ERROR); // state not clobbered

      switch (err) {
      case Z_NEED_DICT:
        err = Z_DATA_ERROR; // fall through
      case Z_DATA_ERROR:
      case Z_MEM_ERROR:
        inflateEnd(&stream);
        panic("%s: inflate() failed with error %d\n", __func__, err);
      }

      have = CHUNK - stream.avail_out;
      assert(have == CHUNK || have == 0);
      if (have) {
        copy_output((const char *)out, i*CHUNK, have);
        i++;
      }
    } while (stream.avail_out == 0);

    // done when inflate() says its done!
  } while (err != Z_STREAM_END);

  if (stream.total_out != dstlen)
    panic("%s: inflate() total_out != dstlen\n", __func__);

  assert(i == dstlen/CHUNK);
  inflateEnd(&stream);
  assert(err == Z_STREAM_END);
  return 0;
}
