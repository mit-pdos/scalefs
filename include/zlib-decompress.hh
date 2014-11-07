#pragma once

#include "types.h"

int zlib_decompress(unsigned char *src, u64 srclen,
                    unsigned char *dst, u64 dstlen);
