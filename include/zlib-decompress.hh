#pragma once

#include "types.h"

int zlib_decompress(unsigned char *src, u64 srclen, u64 dstlen,
                    void (*copy_output)(const char *buf, u64 offset, u64 size));
