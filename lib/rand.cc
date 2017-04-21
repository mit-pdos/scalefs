#include <stdlib.h>
#include "amd64.h"

void
srand(unsigned int seed)
{
}

int
rand(void)
{
  int t = rdtsc();
  return (t >= 0) ? t : 0 - t;
}
