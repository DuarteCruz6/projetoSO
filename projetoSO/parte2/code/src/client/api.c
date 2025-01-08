#include "api.h"

#include "src/common/constants.h"
#include "src/common/protocol.h"

int server_pipe;
char req_pipe_simples[40], resp_pipe_simples[40], notif_pipe_simples[40];
int req_pipe, resp_pipe, notif_pipe

void pad_string(char *str, size_t length) {
  size_t current_length = strlen(str);
  if (current_length < length) {
    memset(str+current_length,"\0",length-current_length);
    str[length-1] = '\0'; // Assegurar que termina com \0
  }
}

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *notif_pipe_path, char const *server_pipe_path) {

  // create request, response and notification pipes and connect to server pipe
  if (mkfifo(req_pipe_path, 0666) == -1) {
    fprintf(stderr, "Error creating the request pipe\n");
    return 1;
  }
  if (mkfifo(resp_pipe_path, 0666) == -1) {
    fprintf(stderr, "Error creating the response pipe\n");
    return 1;
  }
  if (mkfifo(notif_pipe_path, 0666) == -1) {
    fprintf(stderr, "Error creating the notification pipe\n");
    return 1;
  } 

  server_pipe = open(server_pipe_path, O_WRONLY);
  if (fd == -1) {
      fprintf(stderr, "Error connecting to the server pipe\n");
      return 1;
  }
  strcpy(req_pipe_simples,req_pipe_path);
  strcpy(resp_pipe_simples,resp_pipe_path);
  strcpy(notif_pipe_simples,notif_pipe_path);

  // Preencher até 40 caracteres
  pad_string(req_pipe_simples, sizeof(req_pipe_simples));
  pad_string(resp_pipe_simples, sizeof(resp_pipe_simples));
  pad_string(notif_pipe_simples, sizeof(notif_pipe_simples));

  // fazer pedido ao server atraveś do pipe
  char message[128];
  snprintf(message, sizeof(message), "1 %s %s %s", req_pipe_simples, resp_pipe_simples, notif_pipe_simples);
  write_message_to_server_pipe(message)

  //esperar pela resposta

  return 0;
}

int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  write_message_to_server_pipe("2")

  //esperar pela resposta do server

  //fechar os pipes
  close(req_pipe);
  close(resp_pipe);
  close(notif_pipe);

  return 0;
}

int kvs_subscribe(const char *key) {
  // send subscribe message to request pipe and wait for response in response
  // pipe
  return 0;
}

int kvs_unsubscribe(const char *key) {
  // send unsubscribe message to request pipe and wait for response in response
  // pipe
  return 0;
}

int write_message_to_server_pipe(const char *message){
  //write message to server pipe
  write(server_pipe, message, sizeof(char) * (strlen(message)+1)); // Enviar mensagem
}
