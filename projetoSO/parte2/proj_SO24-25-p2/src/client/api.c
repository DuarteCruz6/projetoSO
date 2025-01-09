#include "api.h"
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#include "src/common/constants.h"
#include "src/common/protocol.h"

//manda request
void createMessage(const char *req_pipe_path, char *message){
  int pipe_req = open(req_pipe_path, O_WRONLY);
  if (write(pipe_req, message, strlen(message) + 1) == -1) { // +1 para incluir o '\0'
    fprintf(stderr, "Error writing to pipe request");
    return;
  }
}

//recebe a resposta do pipe
int getResponse(const char *resp_pipe_path){
  // abrir pipe de response para leitura
  int pipe_resp = open(resp_pipe_path, O_RDONLY);
  if (pipe_resp == -1) {
      fprintf(stderr, "Error reading pipe response");
      return 1;
  }

  // Ler a mensagem do pipe (bloqueante)
  char buffer[2];
  ssize_t bytes_read = read(pipe_resp, buffer, sizeof(buffer));
  if (bytes_read == -1) {
      fprintf(stderr, "Error reading pipe response");
      return 1;
  }
  int code, result;
  sprintf(buffer, "%d %d", code, result);

  char* operations[4]={"connect","disconnect","subscribe","unsubscribe"};
  printf("Server returned %d for operation: %s",result,operations[result-1]);
  return result; 
}

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *notif_pipe_path, char const *server_pipe_path) {
  // create pipes and connect
  if (mkfifo(req_pipe_path, 0666) == -1) {
    fprintf(stderr, "Failed to create request pipe\n");
    return 1;
  }
  if (mkfifo(resp_pipe_path, 0666) == -1) {
    fprintf(stderr, "Failed to create response pipe\n");
    return 1;
  }
  if (mkfifo(notif_pipe_path, 0666) == -1) {
    fprintf(stderr, "Failed to create notification pipe\n");
    return 1;
  }

  char message[128];
  //construir mensagem
  snprintf(message, 128, "%d %s %s %s", OP_CODE_CONNECT ,req_pipe_path, resp_pipe_path, notif_pipe_path);

  createMessage(server_pipe_path,message);
  
  int response = getResponse(resp_pipe_path);
  if(response!=0){
    fprintf(stderr, "Failed to connect the client\n");
    return 1;
  }
  return 0;
}

int kvs_disconnect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *notif_pipe_path) {
  // close pipes and unlink pipe files
  createMessage(req_pipe_path,OP_CODE_DISCONNECT);
  int response = getResponse(resp_pipe_path);
  if(response!=0){
    fprintf(stderr, "Failed to disconnect the client\n");
    return 1;
  }
  int req_pipe = open(req_pipe_path, O_WRONLY);
  int resp_pipe = open(resp_pipe_path, O_RDONLY);
  int notif_pipe = open(notif_pipe_path, O_RDONLY);
  close(req_pipe);
  close(resp_pipe);
  close(notif_pipe);

  return 0;
}

int kvs_subscribe(char const *req_pipe_path, char const *resp_pipe_path, const char *key) {
  // send subscribe message to request pipe and wait for response in response
  // pipe
  char message[44];
  //construir mensagem
  snprintf(message, 44, "%d %s", OP_CODE_SUBSCRIBE ,key);
  createMessage(req_pipe_path,message);
  int response = getResponse(resp_pipe_path);
  if(response!=0){
    fprintf(stderr, "Failed to subscribe the client\n");
    return 1;
  }

  return 0;
}

int kvs_unsubscribe(char const *req_pipe_path, char const *resp_pipe_path, const char *key) {
  // send unsubscribe message to request pipe and wait for response in response
  // pipe
  char message[44];
  //construir mensagem
  snprintf(message, 44, "%d %s", OP_CODE_UNSUBSCRIBE ,key);
  createMessage(req_pipe_path,message);
  int response = getResponse(resp_pipe_path);
  if(response!=0){
    fprintf(stderr, "Failed to unsubscribe the client\n");
    return 1;
  }
  return 0;
}
