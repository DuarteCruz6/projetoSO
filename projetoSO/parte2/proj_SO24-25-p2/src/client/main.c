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

char *server_pipe_path= NULL; //caminho para o server pipe
bool deuDisconnect = false; //flag para saber se deu disconnect ou nao

//dados da thread principal
struct ThreadPrincipalData {
  const char *req_pipe_path; //caminho para o pipe de request
  const char *resp_pipe_path; //caminho para o pipe de response
  const char *notif_pipe_path; //caminho para o pipe de notificacoes
  pthread_t *thread_secundaria; //ponteiro para a thread secundaria que lê as notificacoes
};

//dados da thread secundaria
struct ThreadSecundariaData {
  const char *notif_pipe_path; //caminho para o pipe de notificacoes
};

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

  while (!getSinalSeguranca()) {
    //nao foi lancado nenhum sigusr1
    switch (get_next(STDIN_FILENO)) {
    case CMD_DISCONNECT:
      //era disconnect
      if (kvs_disconnect(req_pipe, resp_pipe, notif_pipe) != 0) {
        if(!getSinalSeguranca()){
          write_str(STDERR_FILENO, "Failed to disconnect to the server\n");
        }else{
          write_str(STDERR_FILENO, "Pipe fechado pelo servidor\n");
          return NULL;
        }
        return NULL;
      }
      pthread_cancel(*(thread_data->thread_secundaria)); //cancela a thread secundaria
      deuDisconnect = true;
      return NULL;

    case CMD_SUBSCRIBE:
      //era subscribe
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_subscribe(keys[0])==1){
        if(!getSinalSeguranca()){
          write_str(STDERR_FILENO, "Command subscribe failed\n");
        }else{
          write_str(STDERR_FILENO, "Pipe fechado pelo servidor\n");
          return NULL;
        }
      }
      break;

    case CMD_UNSUBSCRIBE:
      //era unsubscribe
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_unsubscribe(keys[0])==1) {
        if(!getSinalSeguranca()){
          write_str(STDERR_FILENO, "Command unsubscribe failed\n");
        }else{
          write_str(STDERR_FILENO, "Pipe fechado pelo servidor\n");
          return NULL;
        }
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
  struct ThreadSecundariaData *thread_data = (struct ThreadSecundariaData *)arguments;
  char notif_pipe[41];
  strcpy(notif_pipe, thread_data->notif_pipe_path);
  int pipe_notif = open(notif_pipe, O_RDONLY); //abre o pipe das notificacoes em modo de leitura
  if (pipe_notif == -1) {
    write_str(STDERR_FILENO, "Erro ao abrir a pipe de notificacoes\n");
    return NULL;
  }
  while(!deuDisconnect && !getSinalSeguranca()){ //trabalha até dar disconnect ou haver um sigusr1
    char notif[83];
    int success = read_all(pipe_notif, notif, 82, NULL); //le o pipe de notifs
    notif[82]= '\0';
    char chave[41];
    memcpy(chave, &notif[0],40);
    chave[40] = '\0';
    char newValue[41];
    memcpy(newValue, &notif[41],40);
    newValue[40] = '\0';
    size_t tamanho = (size_t) snprintf(NULL,0,"(%s,%s)",chave,newValue);
    char *output = malloc(tamanho + 1);
    //cria a mensagem para o output q mostra q houve notificacao
    snprintf(output, tamanho + 1, "(%s,%s)", chave, newValue);

    if (success == 1) {
      write_str(STDOUT_FILENO,output);
      write_str(STDOUT_FILENO,"\n");
      free(output);
    } else if(success == -1){
      close(pipe_notif);
      write_str(STDERR_FILENO, "Erro ao ler a pipe de notificacoes\n");
      free(output);
      return NULL;
    } else{
      //o pipe foi fechado
      free(output);
      return NULL;
    }
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
  struct ThreadPrincipalData threadPrincipal_data= {req_pipe_path, resp_pipe_path, notif_pipe_path,&thread_secundaria[0]};
  struct ThreadSecundariaData threadSecundaria_data = {notif_pipe_path};

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
  if (pthread_join(thread_principal[0], NULL) != 0) {
    write_str(STDERR_FILENO, "Failed to join thread gestora\n");
    free(thread_principal);
    return;
  }

  //espera pela secundaria
  if (pthread_join(thread_secundaria[0], NULL) != 0) {
    write_str(STDERR_FILENO, "Failed to join thread\n");
    free(thread_secundaria);
    return;
  }
  
  free(thread_principal);
  free(thread_secundaria);
  return;
}


int main(int argc, char *argv[]) {
  if (argc < 3) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO,argv[0]);
    write_str(STDERR_FILENO, " <client_unique_id> <register_pipe_path>\n");
    return 1;
  }

  char req_pipe_path[256] = "/tmp/req";
  char resp_pipe_path[256] = "/tmp/resp";
  char notif_pipe_path[256] = "/tmp/notif";

  //adicionar id do cliente ao nome dos pipes
  strncat(req_pipe_path, argv[1], strlen(argv[1]));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]));



  server_pipe_path = argv[2];
  //conecta ao server
  if (kvs_connect(req_pipe_path, resp_pipe_path, notif_pipe_path, server_pipe_path)==1){
    return 1;
  }
  //cria as threads
  create_threads(req_pipe_path, resp_pipe_path, notif_pipe_path);

  //apaga os seus pipes
  unlink(req_pipe_path);
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);
  return 0;
}