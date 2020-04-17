#pragma once

#include <stdio.h>
#include <sys/resource.h>
#include <sys/time.h>

#include "debug.hh"

static void
displayRusageRUMaxrss()
{
  struct rusage r;
  if (getrusage(RUSAGE_SELF, &r) != 0) ERR;
  printf("maxrss:\t%ld kB\n", r.ru_maxrss);
}

