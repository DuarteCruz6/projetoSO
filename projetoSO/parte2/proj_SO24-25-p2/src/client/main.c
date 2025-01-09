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
char req_pipe[MAX_PIPE_PATH_LENGTH], resp_pipe[MAX_PIPE_PATH_LENGTH], notif_pipe[MAX_PIPE_PATH_LENGTH];

struct ThreadPrincipal {
};

struct ThreadSecundaria {
};

void pad_string(char *str, size_t length) {
  size_t current_length = strlen(str);
  if (current_length < length) {
    memset(str + current_length, ' ', length - current_length);
    str[length - 1] = '\0'; // Assegurar que termina com \0
  }
}

//thread principal: le os comandos e gere o envio de pedidos para o servidor e recebe as respostas do server
void thread_principal_work(){
  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;
  while (1) {
    switch (get_next(STDIN_FILENO)) {
    case CMD_DISCONNECT:
      if (kvs_disconnect() != 0) {
        fprintf(stderr, "Failed to disconnect to the server\n");
        pthread_exit(NULL);
        return 1;
      }
      // TODO: end notifications thread
      printf("Disconnected from server\n");
      pthread_exit(NULL);
      return 0;

    case CMD_SUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_subscribe(keys[0])) {
        fprintf(stderr, "Command subscribe failed\n");
      }

      break;

    case CMD_UNSUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_unsubscribe(keys[0])) {
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
void thread_secundaria_work(){
  int pipe_notif = open(notif_pipe, O_RDONLY);
    if (pipe_notif == -1) {
      fprintf(stderr, "Erro ao abrir a pipe de notificacoes");
      return;
    }
    char buffer[256];
    while (1) { // Loop infinito para ler notificações
      ssize_t bytes_read = read(pipe_notif, buffer, sizeof(buffer) - 1);
      if (bytes_read > 0) {
        buffer[bytes_read] = '\0'; // Assegurar que o buffer é uma string válida
        printf("Notificação recebida: %s\n", buffer);
      } else if (bytes_read == 0) {
        // EOF, caso o escritor feche a pipe
        break;
      } else {
        fprintf(stderr, "Erro ao ler a pipe de notificacoes");
      }
    }
}

//criar as 2 threads por cliente:
    //principal: le os comandos e gere o envio de pedidos para o servidor e recebe as respostas do server
    //secundaria: recebe as notificacoes e imprime o resultado para o stdout
void create_threads(){
  pthread_t *threads = malloc(2 * sizeof(pthread_t));
  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  //principal
  if (pthread_create(&threads[0], NULL, thread_principal_work, NULL)!=0) {
    fprintf(stderr, "Failed to create thread %d\n", 1);
    free(threads);
    return;
  }

  //secundaria
  if (pthread_create(&threads[1], NULL, thread_secundaria_work, NULL)!=0) {
    fprintf(stderr, "Failed to create thread %d\n", 1);
    free(threads);
    return;
  }
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

  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  strcpy(req_pipe, req_pipe_path);
  strcpy(resp_pipe, resp_pipe_path);
  strcpy(notif_pipe, notif_pipe_path);

  // Preencher até 40 caracteres
  pad_string(req_pipe, sizeof(req_pipe));
  pad_string(resp_pipe, sizeof(resp_pipe));
  pad_string(notif_pipe, sizeof(notif_pipe));

  // TODO open pipes

  if (kvs_connect(req_pipe_path, resp_pipe_path, notif_pipe_path, server_pipe_path)==1){
    return 1;
  }
  create_threads();

}