#include <config.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "check_impl.h"

#include <stasis/common.h>

uint64_t myrandom(uint64_t x) {
  double xx = x;
  double r = random();
  double max = ((uint64_t)RAND_MAX)+1;
  max /= xx;
  return (uint64_t)((r/max));
}


void setup (void) {
  remove("logfile.txt");
  remove("storefile.txt");
}

void teardown(void) {
#ifdef LONG_TEST
  system("echo *.txt | grep -v '*' | xargs -n1 -r ls -lh --full-time");
#endif
  setup();
}