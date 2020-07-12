/**
 * @file memory.hh
 */

#pragma once

#include <stdio.h>
#include <sys/resource.h>
#include <sys/time.h>

#include <iostream>

static void displayRusageRUMaxrss() {
  struct rusage r;
  if (getrusage(RUSAGE_SELF, &r) != 0) {
    std::cout << __FILE__ << " : " << __LINE__ << " : fatal error."
              << std::endl;
    std::abort();
  }
  printf("maxrss:\t%ld kB\n", r.ru_maxrss);
}
