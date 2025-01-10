#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdbool.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>

#include "constants.h"
#include "io.h"
#include "operations.h"
#include "parser.h"
#include "kvs.h"
#include "src/common/constants.h"
#include "src/common/io.h"
#include "src/common/sinalSIGUSR1.h"

struct SharedData {
  DIR *dir;
  char *dir_name;
  pthread_mutex_t directory_mutex;
};

typedef struct User{
  Cliente* cliente; //ponteiro para a estrutura cliente para user os pipes
  bool usedFlag;  //flag para saber se uma thread ja o esta a usar ou nao
  struct User* nextUser; //ponteiro para o proximo user
}User;

typedef struct BufferUserConsumer {
  User* headUser; //ponteiro para a cabeca da linked list de users
  pthread_mutex_t buffer_mutex; //read and write block para ter a certeza q 2 threads n vao ao mesmo tempo
}BufferUserConsumer; 

int numClientes=0;
sem_t semaforoBuffer; //semaforo para o buffer -> +1 quando ha inicio de sessao de um cliente, -1 quando uma thread vai buscar um cliente
sigset_t sinalSeguranca; //sinal SIGUSR1

BufferUserConsumer* bufferThreads;//buffer utilizador - consumidor


pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0; // Number of active backups
size_t max_backups;        // Maximum allowed simultaneous backups
size_t max_threads;        // Maximum allowed simultaneous threads
char *jobs_directory = NULL;
char *nome_fifo = NULL;
int server_fifo;

int filter_job_files(const struct dirent *entry) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot != NULL && strcmp(dot, ".job") == 0) {
    return 1; // Keep this file (it has the .job extension)
  }
  return 0;
}

static int entry_files(const char *dir, struct dirent *entry, char *in_path,
                       char *out_path) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 ||
      strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    write_str(STDERR_FILENO,dir);
    write_str(STDERR_FILENO, "/");
    write_str(STDERR_FILENO, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char *filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
    case CMD_WRITE:
      num_pairs =
          parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_write(num_pairs, keys, values)) {
        write_str(STDERR_FILENO, "Failed to write pair\n");
      }
      break;

    case CMD_READ:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_read(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to read pair\n");
      }
      break;

    case CMD_DELETE:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_delete(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to delete pair\n");
      }
      break;

    case CMD_SHOW:
      kvs_show(out_fd);
      break;

    case CMD_WAIT:
      if (parse_wait(in_fd, &delay, NULL) == -1) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay > 0) {
        printf("Waiting %d seconds\n", delay / 1000);
        kvs_wait(delay);
      }
      break;

    case CMD_BACKUP:
      pthread_mutex_lock(&n_current_backups_lock);
      if (active_backups >= max_backups) {
        wait(NULL);
      } else {
        active_backups++;
      }
      pthread_mutex_unlock(&n_current_backups_lock);
      int aux = kvs_backup(++file_backups, filename, jobs_directory);

      if (aux < 0) {
        write_str(STDERR_FILENO, "Failed to do backup\n");
      } else if (aux == 1) {
        return 1;
      }
      break;

    case CMD_INVALID:
      write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
      break;

    case CMD_HELP:
      write_str(STDOUT_FILENO,
                "Available commands:\n"
                "  WRITE [(key,value)(key2,value2),...]\n"
                "  READ [key,key2,...]\n"
                "  DELETE [key,key2,...]\n"
                "  SHOW\n"
                "  WAIT <delay_ms>\n"
                "  BACKUP\n" // Not implemented
                "  HELP\n");

      break;

    case CMD_EMPTY:
      break;

    case EOC:
      printf("EOF\n");
      return 0;
    }
  }
}

// frees arguments
static void *get_file(void *arguments) {
  struct SharedData *thread_data = (struct SharedData *)arguments;
  DIR *dir = thread_data->dir;
  char *dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    write_str(STDERR_FILENO, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent *entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      write_str(STDERR_FILENO, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        write_str(STDERR_FILENO, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      write_str(STDERR_FILENO, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    write_str(STDERR_FILENO, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

void iniciar_sessao(char *message){
  char first_char = message[0];
  int code = atoi(&first_char);
  char pipe_req[41], pipe_resp[41], pipe_notif[41];
  strncpy(pipe_req, &message[1], 40); // Copiar os primeiros 40 caracteres após o número
  pipe_req[40] = '\0';             // Adicionar terminador nulo
  strncpy(pipe_resp, &message[41], 40); // Copiar os próximos 40 caracteres
  pipe_resp[40] = '\0';              // Adicionar terminador nulo
  strncpy(pipe_notif, &message[81], 40); // Copiar os últimos 40 caracteres
  pipe_notif[40] = '\0';  

  int response_pipe = open(pipe_resp, O_WRONLY);
  if (response_pipe == -1) {
    write_str(STDERR_FILENO,"Erro ao abrir o pipe de response");
    return;
  }
  if(code==1){
    Cliente *new_cliente = malloc(sizeof(Cliente));
    if (new_cliente == NULL) {
      write_str(STDERR_FILENO, "Erro ao alocar memória para novo cliente\n");
      char response[2] = "1";
      if (write_all(response_pipe, response, 1) == -1) {
        write_str(STDERR_FILENO,"Erro ao enviar pedido de subscrição");
      }
      return;
    }

    // Inicializa os campos da estrutura
    numClientes++;
    new_cliente->id = numClientes;
    strcpy(new_cliente->req_pipe_path, pipe_req);
    strcpy(new_cliente->resp_pipe_path, pipe_resp);
    strcpy(new_cliente->notif_pipe_path, pipe_notif);
    new_cliente->num_subscricoes=0;
    User *new_user = malloc(sizeof(User));
    new_user->cliente = new_cliente;
    new_user->usedFlag = false;
    new_user ->nextUser = NULL;
    pthread_mutex_lock(&bufferThreads->buffer_mutex); //da lock ao buffer pois vamos escrever nele
    if(bufferThreads->headUser==NULL){
      //buffer tava vazio
      bufferThreads->headUser = new_user;
    }else{
      User *user_atual = bufferThreads->headUser; //vai ao primeiro user
      new_user->nextUser = user_atual; 
      bufferThreads->headUser = new_user; //adiciona o novo user ao inicio do buffer
    }
    pthread_mutex_unlock(&bufferThreads->buffer_mutex);
    sem_post(&semaforoBuffer); //aumentar 1 no semaforo pois adicionamos o cliente

    //manda que deu sucesso
    char response[2] = "0";
    if (write_all(response_pipe, response, 1) == -1) {
      write_str(STDERR_FILENO,"Erro ao enviar pedido de subscrição");
      return;
    }
    return;
  }
  
  printf("Erro ao iniciar sessao de novo cliente.\n");
  return;
}



int subscribeClient(Cliente *cliente, char *message){
  if(!sinalSegurancaLancado){
    char key[41];
    int code;
    sscanf(message,"%d%s",&code, key);

    if (addSubscriber(cliente, key)==0){
      //a key existe e deu certo
      return 0;
    }
    return 1;
  }
  return 1;
}

int unsubscribeClient(Cliente *cliente, char *message){
  if(!sinalSegurancaLancado){
    char key[41];
    int code;
    sscanf(message,"%d%s",&code, key);

    if (removeSubscriber(cliente, key)==0){
      //a subscricao existia e deu certo
      return 0;
    }
    return 1;
  }
  return 1;
}

void removeClientFromBuffer(Cliente *cliente){
  pthread_mutex_lock(&bufferThreads->buffer_mutex); //bloquear o buffer pois vamos altera-lo
  User *user_atual = bufferThreads->headUser;
  if(user_atual->cliente->id == cliente->id){
    //este user era a cabeca da lista
    bufferThreads->headUser = user_atual->nextUser;
    free(user_atual);
    return;
  }else{
    //este user esta no meio da lista
    User* prev_user= NULL;
    while(user_atual->cliente->id != cliente->id && user_atual->nextUser!=NULL){
      //ainda nao encontramos o que queriamos entao passamos para o proximo
      prev_user=user_atual;
      user_atual=user_atual->nextUser;
    }
    //encontramos o que queriamos
    prev_user->nextUser = user_atual->nextUser; //muda a ligacao antigo->atual->futuro para antigo->futuro
    free(user_atual);
    return;
  }

}

// Função para tratar SIGUSR1
void sinalDetetado() {
  //tem de eliminar todas as subscricoes de todos os clientes e encerrar os seus pipes
  sinalSegurancaLancado = 0; //mete como true
  User *userAtual = bufferThreads->headUser;
  while (userAtual!=NULL){
    Cliente* cliente = userAtual->cliente;
    disconnectClient(cliente); //remove as suas subscricoes
    removeClientFromBuffer(cliente); //remover do buffer
    // Apagar os pipes do cliente
    if (unlinkPipes(cliente->req_pipe_path)!=0){
      write_str(STDERR_FILENO, "Failed to close request pipe\n");
      return;
    }
    if (unlinkPipes(cliente->resp_pipe_path)!=0){
      write_str(STDERR_FILENO, "Failed to close response pipe\n");
      return;
    }
    if (unlinkPipes(cliente->notif_pipe_path)!=0){
      write_str(STDERR_FILENO, "Failed to close notification pipe\n");
      return;
    }
    free(cliente);
    userAtual=userAtual->nextUser;
  }
  sinalSegurancaLancado = 1; //volta a meter como false
  return;
}

void *readServerPipe(){
  // Desbloquear SIGUSR1 apenas nesta thread
  pthread_sigmask(SIG_UNBLOCK, &sinalSeguranca, NULL);
  // Registar o manipulador de sinal
  signal(SIGUSR1, sinalDetetado);

  //ler FIFO
  int erro=0;
  char message[128];
  while(1){
    int success = read_all(server_fifo,&message, 128, &erro);
    if (success > 1){
      int code = message[0];
      if (code==1){
        iniciar_sessao(message);
      }else{
        write_str(STDERR_FILENO, "Erro ao ler do pipe do server\n");
        return NULL;
      }
    } else if (success == 0) {
      // EOF: O pipe foi fechado
      return NULL;
    } else {
      // Erro ao ler
      write_str(STDERR_FILENO, "Erro ao ler do pipe do server\n");
      return NULL;
    }
  }
  return NULL;
}

int sendOperationResult(int code, int result, Cliente* cliente){
  if(!sinalSegurancaLancado){
    //escreve se a operacao deu certo (0) ou errado (1)
    char response[3];
    snprintf(response,3,"%d%d", code, result);
    int response_pipe = open(cliente->resp_pipe_path, O_WRONLY);
    if(response_pipe==-1){
      //erro a abrir o pipe de respostas
      return 1;
    }
    int success = write_all(response_pipe, response, 3);
    if(success==1){
      close(response_pipe);
      return 0;
    }else{
      write_str(STDERR_FILENO, "Erro ao escrever no pipe de response\n");
      return 1;
    }
  }
  return 1;
}

//so acaba quando o client der disconnect ou houver o sinal SIGSUR1
int manageClient(Cliente *cliente){
  char message[43];
  while(!sinalSegurancaLancado){ //trabalha enquanto o sinal SIGUSR1 nao for detetado
    int request_pipe = open(cliente->req_pipe_path, O_RDONLY);
    if(request_pipe==-1){
      return 1;
    }
    int success = read_all(request_pipe,&message, 43, NULL);
    close(request_pipe);
    if (success > 0){
      int code = message[0];
      int result;

      if (code==2){
        //disconnect
        result = disconnectClient(cliente);
        if (result==0){
          //tirar do buffer
          removeClientFromBuffer(cliente);
          if(sendOperationResult(code,result,cliente)==1){
            //erro a mandar mensagem para o cliente
            return 1;
          }
          break;
        }

      }else if (code==3){
        //subscribe
        result = subscribeClient(cliente, message);
      }else if (code==4){
        //unsubscribe
        result = unsubscribeClient(cliente, message);
      }else{
        //leu um codigo inesperado
        return 1;
      }
      if(sendOperationResult(code,result,cliente)==1){
        //erro a mandar mensagem para o cliente
        return 1;
      }
    }else{
      //nao leu nada pois houve erro
      return 1;
    }
  }
  return 0;
}

//retira o primeiro cliente que nao tem thread associada e retorna-o
Cliente* getClientForThread(){
  User* user_atual = bufferThreads->headUser;
  while (user_atual->usedFlag){
    user_atual = user_atual->nextUser; //passa ate encontrar um cliente cuja usedFlag seja falsa
  }
  user_atual->usedFlag=true; //mete a flag do user como true pois vai ser usado
  return user_atual->cliente; //retorna o cliente que vai ser usado
}

//quando o manage client acaba significa q o client deu disconnect, portanto vai buscar outro client
//so acaba quando o server morre (??)
void *readClientPipe(){
  while(1){
    sem_wait(&semaforoBuffer); //tirar 1 ao semaforo
    Cliente *cliente = getClientForThread();
    if(manageClient(cliente)==1){
      //deu erro a ler cliente
      return NULL;
    }
  }
}

static void dispatch_threads(DIR *dir) {
  pthread_t *threads = malloc(max_threads * sizeof(pthread_t));
  pthread_t *threads_gestoras = malloc(MAX_SESSION_COUNT * sizeof(pthread_t));

  //bloqueia o sinal SIGUSR1 em todas as threads
  sigemptyset(&sinalSeguranca);
  sigaddset(&sinalSeguranca, SIGUSR1);
  pthread_sigmask(SIG_BLOCK, &sinalSeguranca, NULL);

  if (threads == NULL) {
    write_str(STDERR_FILENO, "Failed to allocate memory for threads\n");
    return;
  }

  //threads dos .job
  struct SharedData thread_data = {dir, jobs_directory,
                                   PTHREAD_MUTEX_INITIALIZER};

  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void *)&thread_data) !=
        0) {
      write_str(STDERR_FILENO, "Failed to create thread");
      write_uint(STDERR_FILENO,(int) i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  

  //cria S threads ler do pipe de registo de cada cliente
  for (size_t thread_gestora = 0; thread_gestora < MAX_SESSION_COUNT; thread_gestora++) {
    pthread_mutex_lock(&bufferThreads->buffer_mutex); //lock pois vamos ler do buffer        
    pthread_mutex_unlock(&bufferThreads->buffer_mutex); //unlock do buffer
    if (pthread_create(&threads_gestoras[thread_gestora], NULL, readClientPipe,NULL) !=
        0) {
      write_str(STDERR_FILENO, "Failed to create thread gestora");
      write_uint(STDERR_FILENO, (int) thread_gestora);
      free(threads_gestoras);
      return;
    }
  }

  //inicia sessão dos clientes
  readServerPipe();

  for(unsigned int thread_gestora = 0; thread_gestora < MAX_SESSION_COUNT; thread_gestora++){
    if (pthread_join(threads_gestoras[thread_gestora], NULL) != 0) {
      write_str(STDERR_FILENO, "Failed to join thread gestora ");
      write_uint(STDERR_FILENO, (int) thread_gestora);
      free(threads_gestoras);
      return;
    }
  }

  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      write_str(STDERR_FILENO, "Failed to join thread");
      write_uint(STDERR_FILENO, (int) i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    write_str(STDERR_FILENO, "Failed to destroy directory_mutex\n");
  }

  free(threads);
}

int main(int argc, char **argv) {
  if (argc < 5) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
    write_str(STDERR_FILENO, " <max_threads>");
    write_str(STDERR_FILENO, " <max_backups> \n");
    write_str(STDERR_FILENO, " <nome_FIFO_de_registo> \n");
    return 1;
  }

  jobs_directory = argv[1];
  nome_fifo = argv[4];


  char *endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

  if (*endptr != '\0') {
    write_str(STDERR_FILENO, "Invalid max_proc value\n");
    return 1;
  }

  max_threads = strtoul(argv[2], &endptr, 10);

  if (*endptr != '\0') {
    write_str(STDERR_FILENO, "Invalid max_threads value\n");
    return 1;
  }

  if (max_backups <= 0) {
    write_str(STDERR_FILENO, "Invalid number of backups\n");
    return 0;
  }

  if (max_threads <= 0) {
    write_str(STDERR_FILENO, "Invalid number of threads\n");
    return 0;
  }

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  DIR *dir = opendir(argv[1]);
  if (dir == NULL) {
    write_str(STDERR_FILENO, "Failed to open directory: ");
    write_str(STDERR_FILENO, argv[1]);
    return 0;
  }

  //criar FIFO
  char fifo_path[256] = "/tmp/req";
  strcat(fifo_path,nome_fifo);
  if (mkfifo(nome_fifo, 0777) == -1) {
      write_str(STDERR_FILENO, "Failed to create FIFO: ");
      write_str(STDERR_FILENO, argv[4]);
      return 0;
  }

  server_fifo = open(nome_fifo, O_RDWR);
  if (server_fifo == -1) {
    write_str(STDERR_FILENO, "Failed to open fifo: ");
    write_str(STDERR_FILENO, nome_fifo);
    return 0;
  }

  sem_init(&semaforoBuffer, 0, MAX_SESSION_COUNT); //inicializar semaforo a 0 e vai até S
  bufferThreads = (BufferUserConsumer*)malloc(sizeof(BufferUserConsumer));
  bufferThreads->headUser = NULL;
  pthread_mutex_init(&bufferThreads->buffer_mutex, NULL);

  dispatch_threads(dir);

  if (closedir(dir) == -1) {
    write_str(STDERR_FILENO, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  
  kvs_terminate();
  close(server_fifo);
  pthread_mutex_destroy(&bufferThreads->buffer_mutex);
  sem_destroy(&semaforoBuffer);
  free(bufferThreads);

  return 0;
}
