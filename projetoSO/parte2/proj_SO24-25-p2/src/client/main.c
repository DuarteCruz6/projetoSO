#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"

char *server_pipe_path= NULL;

struct ThreadPrincipalData {
  const char *req_pipe_path;
  const char *resp_pipe_path;
  const char *notif_pipe_path;
};

struct ThreadSecundariaData {
  const char *notif_pipe_path;
};

void pad_string(char *str, size_t length) {
  size_t current_length = strlen(str);
  if (current_length < length) {
    memset(str + current_length, ' ', length - current_length);
    str[length - 1] = '\0'; // Assegurar que termina com \0
  }
}

//thread principal: le os comandos e gere o envio de pedidos para o servidor e recebe as respostas do server
static void *thread_principal_work(void *arguments){
  struct ThreadPrincipalData *thread_data = (struct ThreadPrincipalData *)arguments;
  char req_pipe[40];
  strcpy(req_pipe, thread_data->req_pipe_path);

  char resp_pipe[40];
  strcpy(resp_pipe, thread_data->resp_pipe_path);

  char notif_pipe[40];
  strcpy(notif_pipe, thread_data->notif_pipe_path);

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;

  while (1) {
    switch (get_next(STDIN_FILENO)) {
    case CMD_DISCONNECT:
      if (kvs_disconnect(req_pipe, resp_pipe, notif_pipe) != 0) {
        fprintf(stderr, "Failed to disconnect to the server\n");
        pthread_exit(NULL);
        return NULL;
      }
      // TODO: end notifications thread
      printf("Disconnected from server\n");
      pthread_exit(NULL);
      return NULL;

    case CMD_SUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_subscribe(req_pipe, resp_pipe, keys[0])) {
        fprintf(stderr, "Command subscribe failed\n");
      }

      break;

    case CMD_UNSUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_unsubscribe(req_pipe, resp_pipe, keys[0])) {
        fprintf(stderr, "Command subscribe failed\n");
      }

      break;

    case CMD_DELAY:
      if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay_ms > 0) {
        printf("Waiting...\n");
        delay(delay_ms);
      }
      break;

    case CMD_INVALID:
      fprintf(stderr, "Invalid command. See HELP for usage\n");
      break;

    case CMD_EMPTY:
      break;

    case EOC:
      // input should end in a disconnect, or it will loop here forever
      break;
    }
  }
}

//thread secundaria: recebe as notificacoes e imprime o resultado para o stdout
void *thread_secundaria_work(void *arguments){
  struct ThreadSecundariaData *thread_data = (struct ThreadSecundariaData *)arguments;
  char notif_pipe[40];
  strcpy(notif_pipe, thread_data->notif_pipe_path);

  int pipe_notif = open(notif_pipe, O_RDONLY);
  if (pipe_notif == -1) {
    fprintf(stderr, "Erro ao abrir a pipe de notificacoes");
    return NULL;
  }
  while(1){
  char buffer[256];
  int success = read_all(pipe_notif, buffer, 256, NULL);
  
  if (success == 1) {
    buffer[256] = '\0'; // Assegurar que o buffer é uma string válida
    write_str(STDOUT_FILENO,buffer);
  } else if (success == 0) {
    // EOF, caso o escritor feche a pipe
    return NULL;
  } else {
    close(pipe_notif);
    fprintf(stderr, "Erro ao ler a pipe de notificacoes");
    return NULL;
  }
  close(pipe_notif);
  return NULL;
  }
  
}

//criar as 2 threads por cliente:
    //principal: le os comandos e gere o envio de pedidos para o servidor e recebe as respostas do server
    //secundaria: recebe as notificacoes e imprime o resultado para o stdout
void create_threads(const char *req_pipe_path, const char *resp_pipe_path, const char *notif_pipe_path){
  
  //principal
  pthread_t *thread_principal = malloc(sizeof(pthread_t));
  pthread_t *thread_secundaria = malloc(sizeof(pthread_t));
  if (thread_principal == NULL) {
    fprintf(stderr, "Failed to allocate memory for thread\n");
    return;
  }
  if (thread_secundaria == NULL) {
    fprintf(stderr, "Failed to allocate memory for thread\n");
    return;
  }
  struct ThreadPrincipalData threadPrincipal_data= {req_pipe_path, resp_pipe_path, notif_pipe_path};
  struct ThreadSecundariaData threadSecundaria_data = {notif_pipe_path};

  //principal
  if (pthread_create(&thread_principal[0], NULL, thread_principal_work, (void *)&threadPrincipal_data)!=0) {
    fprintf(stderr, "Failed to create thread %d\n", 1);
    free(thread_principal);
    return;
  }

  //secundaria
  if (pthread_create(&thread_secundaria[0], NULL, thread_secundaria_work, (void *)&threadSecundaria_data)!=0) {
    fprintf(stderr, "Failed to create thread %d\n", 1);
    free(thread_secundaria);
    return;
  }

  //espera pela principal
  if (pthread_join(thread_principal[0], NULL) != 0) {
    fprintf(stderr, "Failed to join thread gestora\n");
    free(thread_principal);
    return;
  }
  
  //espera pela secundaria
  if (pthread_join(thread_secundaria[0], NULL) != 0) {
    fprintf(stderr, "Failed to join thread\n");
    free(thread_secundaria);
    return;
  }
  

  free(thread_principal);
  free(thread_secundaria);
}


int main(int argc, char *argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n",
            argv[0]);
    return 1;
  }

  char req_pipe_path[256] = "/tmp/req";
  char resp_pipe_path[256] = "/tmp/resp";
  char notif_pipe_path[256] = "/tmp/notif";

  //adicionar id do cliente ao nome dos pipes
  strncat(req_pipe_path, argv[1], strlen(argv[2]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[2]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[2]) * sizeof(char));

  char req_pipe[MAX_PIPE_PATH_LENGTH], resp_pipe[MAX_PIPE_PATH_LENGTH], notif_pipe[MAX_PIPE_PATH_LENGTH];

  strcpy(req_pipe, req_pipe_path);
  strcpy(resp_pipe, resp_pipe_path);
  strcpy(notif_pipe, notif_pipe_path);

  // Preencher até 40 caracteres
  pad_string(req_pipe, sizeof(req_pipe));
  pad_string(resp_pipe, sizeof(resp_pipe));
  pad_string(notif_pipe, sizeof(notif_pipe));

  // TODO open pipes

  if (kvs_connect(req_pipe, resp_pipe, notif_pipe, server_pipe_path)==1){
    return 1;
  }
  create_threads(req_pipe, resp_pipe, notif_pipe);

}