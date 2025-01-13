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

int pipe_req;
int pipe_resp;

void pad_string(char *message,const char *str, int length) {
  for(size_t i=0; i< (size_t) length; i++){
    if(i<strlen(str)){
      message[i] = str[i];
    }else{
      message[i] = '\0';
    }
  }
}

int getSinalSeguranca(){
  return sinalSeguranca;
}

void mudarSinalSeguranca(){
  printf("sigusr1\n");
  if(sinalSeguranca){
    sinalSeguranca=0;
  }else{
    sinalSeguranca=1;
  }
}

//manda request
int createMessage(char *message, int size){
  printf("vai abrir o pipe para pedir _%s_\n",message);
  if (pipe_req == -1 && errno == EPIPE ) {
    mudarSinalSeguranca();
    return 1;
  } else if (pipe_req == -1){
    perror("Error reading pipe response, error: \n");
    return 1;
  }
  int success = write_all(pipe_req,message,(size_t) size);
  if(success!=1){
    write_str(STDERR_FILENO, "Error writing to pipe request");
    return 1;
  }
  printf("ja pediu algo, com sucesso %d\n",success);
  return 0;
}

//recebe a resposta do pipe
int getResponse(){
  // Ler a mensagem do pipe (bloqueante)
  char buffer[3];
  printf("vai ler a msg agora \n");
  int success = read_all(pipe_resp, buffer, 2, NULL);
  buffer[2]='\0';
  if (success != 1) {
    write_str(STDERR_FILENO, "Error reading pipe response\n");
    mudarSinalSeguranca();
    return 1;
  }
  
  printf("leu a msg agora _%s_\n",buffer);
  
  int code = buffer[0] - '0';
  int result = buffer[1] - '0';
  printf("scanneou\n");
  printf("code: %d\n",code);
  printf("result: %d\n",result);

  char* operations[4]={"connect","disconnect","subscribe","unsubscribe"};
  char string[256];
  snprintf(string, sizeof(string), "Server returned %d for operation: %s", result, operations[code-1]);
  write_str(STDOUT_FILENO,string);
  write_str(STDOUT_FILENO," \n");
  return result; 
}

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *notif_pipe_path, char const *server_pipe_path) {
  // create pipes and connect
  if (mkfifo(req_pipe_path, 0777) == -1) {
    printf("path: %s\n",req_pipe_path);
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
  printf("conectou a todos os pipes do cliente, agora vai mandar msg para o do server\n");
  printf("pipe do server: %s\n",server_pipe_path);

  char message[122];
  //construir mensagem
  message[0] = (char) ('0' + OP_CODE_CONNECT);
  pad_string(&message[1], req_pipe_path, 40);
  pad_string(&message[41], resp_pipe_path, 40);
  pad_string(&message[81], notif_pipe_path, 40);
  message[121] = '\0';
  printf("len message: %ld\n",strlen(message));
  int server_pipe = open(server_pipe_path, O_WRONLY);

  
  printf("ja criou a mensagem %s\n", message);

  printf("sem stor\n");
  int success = write_all(server_pipe,message,121);
  if(success!=1){
    perror("Erro ao escrever no FIFO do server");
    return 1;
  }

  printf("agora vai receber a mensagem\n");
  pipe_resp = open(resp_pipe_path, O_RDONLY);
  int response = getResponse(resp_pipe_path);
  pipe_req = open(req_pipe_path, O_WRONLY);
  if(response!=0){
    write_str(STDERR_FILENO, "Failed to connect the client\n");
    return 1;
  }
  return 0;
}

int kvs_disconnect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *notif_pipe_path) {
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

  // Apagar os pipes
  if(unlinkPipes(req_pipe_path)!=0){
    write_str(STDERR_FILENO, "Failed to close request pipe\n");
    return 1;
  }
  if(unlinkPipes(resp_pipe_path)!=0){
    write_str(STDERR_FILENO, "Failed to close response pipe\n");
    return 1;
  }
  if(unlinkPipes(notif_pipe_path)!=0){
    write_str(STDERR_FILENO, "Failed to close notification pipe\n");
    return 1;
  }
  return 0;
}

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
