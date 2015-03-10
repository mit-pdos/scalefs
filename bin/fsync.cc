#include <unistd.h>
#include <fcntl.h>

#include "libutil.h"

int
main(int argc, char *argv[])
{
  int fd, i;

  if(argc <= 1)
    return 0;

  for(i = 1; i < argc; i++){
    if((fd = open(argv[i], 0)) < 0){
      die("fsync: cannot open %s", argv[i]);
    }
    fsync(fd);
    close(fd);
  }
  return 0;
}
