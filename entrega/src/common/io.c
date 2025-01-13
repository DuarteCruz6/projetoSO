#include "io.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "src/common/constants.h"

int read_all(int fd, void *buffer, size_t size, int *intr) {
  if (intr != NULL && *intr) {
    return -1;
  }
  size_t bytes_read = 0;
  while (bytes_read < size) {
    ssize_t result = read(fd, buffer + bytes_read, size - bytes_read);
    if (result == -1) {
      if (errno == EINTR) {
        if (intr != NULL) {
          *intr = 1;
          if (bytes_read == 0) {
            return -1;
          }
        }
        continue;
      }
      perror("Failed to read from pipe");
      return -1;
    } else if (result == 0) {
      return 0;
    }
    bytes_read += (size_t)result;
  }
  return 1;
}

int write_all(int fd, const void *buffer, size_t size) {
  size_t bytes_written = 0;
  while (bytes_written < size) {
    ssize_t result = write(fd, buffer + bytes_written, size - bytes_written);
    if (result == -1) {
      if (errno == EINTR) {
        // error for broken PIPE (error associated with writting to the closed
        // PIPE)
        continue;
      }
      perror("Failed to write to pipe");
      return -1;
    }
    bytes_written += (size_t)result;
  }
  return 1;
}

static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

void delay(unsigned int time_ms) {
  struct timespec delay = delay_to_timespec(time_ms);
  nanosleep(&delay, NULL);
}

void write_str(int fd, const char *str) {
  size_t len = strlen(str);
  const char *ptr = str;

  while (len > 0) {
    ssize_t written = write(fd, ptr, len);

    if (written < 0) {
      perror("Error writing string");
      break;
    }

    ptr += written;
    len -= (size_t)written;
  }
}

//adiciona \0 até a string tar de tamanho length
void pad_string(char *message,const char *str, int length) {
  for(size_t i=0; i< (size_t) length; i++){
    if(i<strlen(str)){
      message[i] = str[i];
    }else{
      message[i] = '\0';
    }
  }
}