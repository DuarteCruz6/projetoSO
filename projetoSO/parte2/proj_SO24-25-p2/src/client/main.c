#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>

#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"

char *server_pipe_path= NULL;
bool deuDisconnect = false; //flag para saber se deu disconnect ou nao

struct ThreadPrincipalData {
  int req_pipe;
  int resp_pipe;
  int notif_pipe;
};

struct ThreadSecundariaData {
  int notif_pipe;
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

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;

  int req_pipe = thread_data->req_pipe;
  int resp_pipe = thread_data->resp_pipe;
  int notif_pipe = thread_data->notif_pipe;

  while (!getSinalSeguranca()) {
    switch (get_next(STDIN_FILENO)) {
    case CMD_DISCONNECT:
      if (kvs_disconnect(req_pipe, resp_pipe, notif_pipe) != 0) {
        write_str(STDERR_FILENO, "Failed to disconnect to the server\n");
        return NULL;
      }
      //pthread_cancel(thread_data->thread_secundaria); //cancelar a thread secundaria
      printf("Disconnected from server\n");
      deuDisconnect = true;
      return NULL;

    case CMD_SUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_subscribe(req_pipe, resp_pipe, keys[0])) {
        write_str(STDERR_FILENO, "Command subscribe failed\n");
      }

      break;

    case CMD_UNSUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_unsubscribe(req_pipe, resp_pipe, keys[0])) {
        write_str(STDERR_FILENO, "Command subscribe failed\n");
      }

      break;

    case CMD_DELAY:
      if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay_ms > 0) {
        printf("Waiting...\n");
        delay(delay_ms);
      }
      break;

    case CMD_INVALID:
      write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
      break;

    case CMD_EMPTY:
      break;

    case EOC:
      // input should end in a disconnect, or it will loop here forever
      break;
    }
  }
  return NULL;
}

//thread secundaria: recebe as notificacoes e imprime o resultado para o stdout
void *thread_secundaria_work(void *arguments){
  printf("comecou a secundaria\n");
  struct ThreadSecundariaData *thread_data = (struct ThreadSecundariaData *)arguments;
  //char notif_pipe[40];
  //strcpy(notif_pipe, thread_data->notif_pipe_path);
  printf(" vai abrir o pipe notif\n");
  //int pipe_notif = open(notif_pipe, O_RDONLY | O_NONBLOCK);
  int pipe_notif = thread_data->notif_pipe;
  printf("abriu o pipe notif\n");
  //if (pipe_notif == -1) {
  //  write_str(STDERR_FILENO, "Erro ao abrir a pipe de notificacoes");
  //  return NULL;
  //}
  while(!deuDisconnect && !getSinalSeguranca()){ //trabalha até dar disconnect
    printf("while da secundaria\n");
    char buffer[256];
    printf("a ler pipe notif\n");
    int success = read_all(pipe_notif, buffer, 256, NULL);

    if (success == 1) {
      printf("sucesso = 1\n");
      buffer[256] = '\0'; // Assegurar que o buffer é uma string válida
      write_str(STDOUT_FILENO,buffer);
    } else if (success == 0) {
      printf("sucesso = 0\n");
      // EOF, caso o escritor feche a pipe
      return NULL;
    } else {
      printf("sucesso = -1\n");
      close(pipe_notif);
      write_str(STDERR_FILENO, "Erro ao ler a pipe de notificacoes");
      return NULL;
    }
    printf("a seguir vai fechar ??? isto deve tar mal\n");
    //close(pipe_notif);
    printf("vai dar return NULL dentro do while, ta mal de certeza\n");
    //return NULL;
  }
  return NULL;
}

//criar as 2 threads por cliente:
    //principal: le os comandos e gere o envio de pedidos para o servidor e recebe as respostas do server
    //secundaria: recebe as notificacoes e imprime o resultado para o stdout
void create_threads(const char *req_pipe_path, const char *resp_pipe_path, const char *notif_pipe_path){
  
  pthread_t *thread_principal = malloc(sizeof(pthread_t));
  pthread_t *thread_secundaria = malloc(sizeof(pthread_t));
  if (thread_principal == NULL) {
    write_str(STDERR_FILENO, "Failed to allocate memory for thread\n");
    return;
  }
  if (thread_secundaria == NULL) {
    write_str(STDERR_FILENO, "Failed to allocate memory for thread\n");
    return;
  }

  int req_pipe = open(req_pipe_path, O_WRONLY | O_NONBLOCK);
  int resp_pipe = open(resp_pipe_path, O_RDONLY);
  int notif_pipe = open(notif_pipe_path, O_RDONLY | O_NONBLOCK);

  struct ThreadPrincipalData threadPrincipal_data= {req_pipe, resp_pipe, notif_pipe};
  struct ThreadSecundariaData threadSecundaria_data = {notif_pipe};

  //principal
  if (pthread_create(&thread_principal[0], NULL, thread_principal_work, (void *)&threadPrincipal_data)!=0) {
    write_str(STDERR_FILENO, "Failed to create client main thread");
    free(thread_principal);
    return;
  }

  //secundaria
  if (pthread_create(&thread_secundaria[0], NULL, thread_secundaria_work, (void *)&threadSecundaria_data)!=0) {
    write_str(STDERR_FILENO, "Failed to create client second thread \n");
    free(thread_secundaria);
    return;
  }

  //espera pela principal
  printf("À espera que a principal acabe\n");
  if (pthread_join(thread_principal[0], NULL) != 0) {
    write_str(STDERR_FILENO, "Failed to join thread gestora\n");
    free(thread_principal);
    return;
  }
  printf("À espera que a secundaria acabe\n");
  //espera pela secundaria
  if (pthread_join(thread_secundaria[0], NULL) != 0) {
    write_str(STDERR_FILENO, "Failed to join thread\n");
    free(thread_secundaria);
    return;
  }
  
  printf("vai dar free as threads\n");
  free(thread_principal);
  printf("vai dar free a thread secundaria\n");
  free(thread_secundaria);
  printf("deu free as threads\n");
  return;
}


int main(int argc, char *argv[]) {
  if (argc < 3) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO,argv[0]);
    write_str(STDERR_FILENO, " <client_unique_id> <register_pipe_path>\n");
    return 1;
  }

  //char req_pipe_path[256] = "tmp/req_";
  //char resp_pipe_path[256] = "tmp/resp_";
  //char notif_pipe_path[256] = "tmp/notif_";
  char req_pipe_path[256] = "../common/tmp/req_";
  char resp_pipe_path[256] = "../common/tmp/resp_";
  char notif_pipe_path[256] = "../common/tmp/notif_";

  //adicionar id do cliente ao nome dos pipes
  strncat(req_pipe_path, argv[1], strlen(argv[1]));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]));

  if(strlen(req_pipe_path)>40 || strlen(resp_pipe_path)>40 || strlen(notif_pipe_path)>40){
    write_str(STDERR_FILENO,"Too big of a lenght for a pipe\n");
    return 1;
  }

  char req_pipe[MAX_PIPE_PATH_LENGTH], resp_pipe[MAX_PIPE_PATH_LENGTH], notif_pipe[MAX_PIPE_PATH_LENGTH];

  strcpy(req_pipe, req_pipe_path);
  strcpy(resp_pipe, resp_pipe_path);
  strcpy(notif_pipe, notif_pipe_path);

  // Preencher até 40 caracteres
  pad_string(req_pipe, sizeof(req_pipe));
  pad_string(resp_pipe, sizeof(resp_pipe));
  pad_string(notif_pipe, sizeof(notif_pipe));

  server_pipe_path = argv[2];


  if (kvs_connect(req_pipe, resp_pipe, notif_pipe, server_pipe_path)==1){
    return 1;
  }
  create_threads(req_pipe, resp_pipe, notif_pipe);
  printf("xau\n");
  return 0;
}