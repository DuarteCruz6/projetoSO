#include "api.h"
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>

#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "src/common/io.h"


int sinalSeguranca = 0; //flag para saber se occoreu um SIGUSR1, 0->falso, 1->verdadeiro

int pipe_req; //variavel global para guardar o descritor do pipe request
int pipe_resp; //variavel global para guardar o descritor do pipe response

//retorna o sinal de seguranca (0->false, 1->true)
int getSinalSeguranca(){
  return sinalSeguranca; 
}

//muda o sinal de seguranca quando houve um sigusr1
void mudarSinalSeguranca(){
  if(sinalSeguranca){
    sinalSeguranca=0;
  }else{
    sinalSeguranca=1;
  }
}

//manda request atraves do pipe request
int createMessage(char *message, int size){
  int success = write_all(pipe_req,message,(size_t) size);
  if(success!=1){
    write_str(STDERR_FILENO, "Error writing to pipe request");
    return 1;
  }
  return 0;
}

//recebe a resposta atraves do pipe response
int getResponse(){
  char buffer[3];
  int success = read_all(pipe_resp, buffer, 2, NULL);
  buffer[2]='\0';
  if (success != 1) {
    write_str(STDERR_FILENO, "Error reading pipe response\n");
    mudarSinalSeguranca(); //houve um sigusr1
    return 1;
  }
  
  int code = buffer[0] - '0'; 
  int result = buffer[1] - '0';

  char* operations[4]={"connect","disconnect","subscribe","unsubscribe"};
  char string[256];
  snprintf(string, sizeof(string), "Server returned %d for operation: %s", result, operations[code-1]);
  write_str(STDOUT_FILENO,string);
  write_str(STDOUT_FILENO," \n");
  return result; 
}

//conecta o cliente ao servidor
int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *notif_pipe_path, char const *server_pipe_path) {
  // create pipes and connect
  if (mkfifo(req_pipe_path, 0777) == -1) {
    write_str(STDERR_FILENO, "Failed to create request pipe\n");
    return 1;
  }
  if (mkfifo(resp_pipe_path, 0777) == -1) {
    write_str(STDERR_FILENO, "Failed to create response pipe\n");
    return 1;
  }
  if (mkfifo(notif_pipe_path, 0777) == -1) {
    write_str(STDERR_FILENO, "Failed to create notification pipe\n");
    return 1;
  }

  char message[122];
  //construir mensagem para pedir connect
  message[0] = (char) ('0' + OP_CODE_CONNECT);
  pad_string(&message[1], req_pipe_path, 40);
  pad_string(&message[41], resp_pipe_path, 40);
  pad_string(&message[81], notif_pipe_path, 40);
  message[121] = '\0';
  //escreve o pedido no server pipe
  int server_pipe = open(server_pipe_path, O_WRONLY);
  int success = write_all(server_pipe,message,121);
  if(success!=1){
    write_str(STDERR_FILENO, "Erro ao escrever no FIFO do server\n");
    return 1;
  }

  //abre o pipe de resposta para obter resposta
  pipe_resp = open(resp_pipe_path, O_RDONLY);
  int response = getResponse(resp_pipe_path);
  if(response!=0){
    write_str(STDERR_FILENO, "Failed to connect the client\n");
    return 1;
  }
  pipe_req = open(req_pipe_path, O_WRONLY);
  return 0;
}

//desconecta o cliente do server
int kvs_disconnect() {
  // close pipes and unlink pipe files
  char code[2];
  sprintf(code, "%d", OP_CODE_DISCONNECT);
  if(createMessage(code,1)==1){
    return 1;
  }
  int response = getResponse();
  if(response!=0){
    write_str(STDERR_FILENO, "Failed to disconnect the client\n");
    return 1;
  }
  return 0;
}

//subscreve o cliente à chave
int kvs_subscribe(const char *key) {
  // send subscribe message to request pipe and wait for response in response
  // pipe
  char message[43];
  message[0] = (char) ('0' + OP_CODE_SUBSCRIBE);
  pad_string(&message[1], key, 41);
  message[42] = '\0';

  if(createMessage(message,42)==1){
    return 1;
  }
  int response = getResponse();
  if(response!=0){
    write_str(STDERR_FILENO, "Failed to subscribe the client\n");
    return 1;
  }

  return 0;
}

//tira o sub do cliente da chave
int kvs_unsubscribe(const char *key) {
  // send unsubscribe message to request pipe and wait for response in response
  // pipe
  //construir mensagem
  char message[43];
  message[0] = (char) ('0' + OP_CODE_UNSUBSCRIBE);
  pad_string(&message[1], key, 41);
  message[42] = '\0';

  if(createMessage(message,42)==1){
    return 1;
  }
  int response = getResponse();
  if(response!=0){
    write_str(STDERR_FILENO, "Failed to unsubscribe the client\n");
    return 1;
  }
  return 0;
}
