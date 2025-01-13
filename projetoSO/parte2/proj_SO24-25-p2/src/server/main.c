#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdint.h>
#include <sys/stat.h>
#include <stdbool.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>
#include <errno.h>

#include "constants.h"
#include "io.h"
#include "operations.h"
#include "parser.h"
#include "kvs.h"
#include "src/common/constants.h"
#include "src/common/io.h"

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

int numClientes=0; //para saber o numero de clientes
sem_t semaforoBuffer; //semaforo para o buffer -> +1 quando ha inicio de sessao de um cliente, -1 quando uma thread vai buscar um cliente
sigset_t sinalSeguranca; //sinal SIGUSR1

BufferUserConsumer* bufferThreads;//buffer utilizador - consumidor
pthread_t *threads_gestoras; 
char fifo_path[256] = "/tmp/";


pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0; // Number of active backups
size_t max_backups;        // Maximum allowed simultaneous backups
size_t max_threads;        // Maximum allowed simultaneous threads
char *jobs_directory = NULL;
char *nome_fifo = NULL;
int server_fifo; //descritor do server pipe

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
  return NULL;
}

//recebe um novo ciente e adiciona-o à estrutura e ao buffer
int novoCliente(char *message){
  char first_char = message[0];
  int code = atoi(&first_char);
  char pipe_req[41], pipe_resp[41], pipe_notif[41];
  memcpy(pipe_req, &message[1], 40); // Copiar os primeiros 40 caracteres após o número
  pipe_req[40] = '\0';             // Adicionar terminador nulo
  memcpy(pipe_resp, &message[41], 40); // Copiar os próximos 40 caracteres
  pipe_resp[40] = '\0';              // Adicionar terminador nulo
  memcpy(pipe_notif, &message[81], 40); // Copiar os últimos 40 caracteres
  pipe_notif[40] = '\0';  

  if(code==1){
    Cliente *new_cliente = malloc(sizeof(Cliente));
    if (new_cliente == NULL) {
      write_str(STDERR_FILENO, "Erro ao alocar memória para novo cliente\n");
      return 1;
    }

    // Inicializa os campos da estrutura cliente
    numClientes++;
    new_cliente->id = numClientes;
    new_cliente->num_subscricoes=0;
    new_cliente->head_subscricoes = NULL;
    new_cliente ->usado = 0;
    strcpy(new_cliente->req_pipe_path, pipe_req);
    strcpy(new_cliente->resp_pipe_path, pipe_resp);
    strcpy(new_cliente->notif_pipe_path, pipe_notif);
    
    //inicializa os campos da estrutura user
    User *new_user = malloc(sizeof(User));
    new_user->cliente = new_cliente;
    new_user->usedFlag = false;
    new_user ->nextUser = NULL;

    pthread_mutex_lock(&bufferThreads->buffer_mutex); //da lock ao buffer pois vamos escrever nele
    if(bufferThreads->headUser==NULL){
      //buffer tava vazio, ent este fica a head
      bufferThreads->headUser = new_user;
    }else{
      //buffer ja tinha clientes
      User *user_atual = bufferThreads->headUser; //vai ao primeiro user
      new_user->nextUser = user_atual; 
      bufferThreads->headUser = new_user; //adiciona o novo user ao inicio do buffer
    }
    pthread_mutex_unlock(&bufferThreads->buffer_mutex); //da unlock ao buffer
    sem_post(&semaforoBuffer); //aumentar 1 no semaforo pois adicionamos o cliente
    return 0;
  }
  write_str(STDERR_FILENO, "Erro ao iniciar novo cliente\n");
  return 1;
}

//comando SUBSCRIBE
int subscribeClient(Cliente *cliente, char *key){
  if(!getSinalSeguranca()){
    if (addSubscriber(cliente, key)==0){
      //a key existe e deu certo
      return 0;
    }
    return 1;
  }
  return 1;
}

//COMANDO UNSUBSCRIBE
int unsubscribeClient(Cliente *cliente, char *key){
  if(!getSinalSeguranca()){
    if (removeSubscriber(cliente, key)==0){
      //a subscricao existia e deu certo
      return 0;
    }
    return 1;
  }
  return 1;
}

//para remover um cliente do buffer quando há um sigusr1 ou este da disconnect
void removeClientFromBuffer(Cliente *cliente){
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

//manda o code+result para o pipe response do user
int sendOperationResult(int code, int result, Cliente* cliente){
  if(!getSinalSeguranca()){
    //escreve se a operacao deu certo (0) ou errado (1)
    char response[3];
    snprintf(response,3,"%d%d", code, result);
    int success = write_all(cliente->resp_pipe, response, 2);
    if(success==1){
      return 0;
    }else{
      write_str(STDERR_FILENO, "Erro ao escrever no pipe de response\n");
      return 1;
    }
  }
  return 1;
}

// Função para tratar SIGUSR1
void sinalDetetado() {
  //tem de eliminar todas as subscricoes de todos os clientes e encerrar os seus pipes
  mudarSinalSeguranca(); //mete como true
  User *userAtual = bufferThreads->headUser;
  while (userAtual!=NULL){
    Cliente* cliente = userAtual->cliente;
    if(cliente->usado){
      disconnectClient(cliente); //remove as suas subscricoes
      pthread_mutex_lock(&bufferThreads->buffer_mutex); //bloquear o buffer pois vamos altera-lo
      removeClientFromBuffer(cliente); //remover do buffer
      pthread_mutex_unlock(&bufferThreads->buffer_mutex); //desbloquear o buffer 

      //fechar os pipes do cliente e mete no cliente a flag de que houve um sigusr1
      close(cliente->req_pipe);
      close(cliente->resp_pipe);
      close(cliente->notif_pipe);
      cliente->flag_sigusr1 = 1;
    }
    userAtual=userAtual->nextUser; //passa para o proximo user
  }
  return;
}

//thread que lê o pipe do server
void *readServerPipe(){
  //desbloquear SIGUSR1 apenas nesta thread
  pthread_sigmask(SIG_UNBLOCK, &sinalSeguranca, NULL);
  //registar o manipulador de sinal
  signal(SIGUSR1, sinalDetetado);

  //ler FIFO
  int erro=0;
  char message[122];
  server_fifo = open(fifo_path, O_RDONLY); //so queremos em modo leitura

  if (server_fifo == -1) {
    write_str(STDERR_FILENO, "Failed to open fifo: ");
    write_str(STDERR_FILENO, nome_fifo);
    write_str(STDERR_FILENO, "\n");
    return 0;
  }

  //verifica o 4º argumento do read_all -> quando fica = 1 é para terminar o programa
  while(erro==0){
    int success = read_all(server_fifo,&message, 121, &erro);
    message[121] = '\0';
    if(erro==1){
      //houve um erro
      if(getSinalSeguranca()){
        //foi sigusr1, entao nao se termina o programa
        mudarSinalSeguranca(); //volta a meter como false
        erro=0;
      }else{
        //nao foi sigusr1, entao é para terminar o programa
        return NULL;
      }
    
    }else if (success == 1){
      //recebeu uma mensagem
      int code = atoi(message);
      if (code==1){
        //inicia sessao a um novo cliente, adicionando-o ao buffer
        if(novoCliente(message)==1){
          //novoCliente deu errado
          return NULL;
        }
      }else{
        //o codigo inserido era != 1
        write_str(STDERR_FILENO, "Codigo != 1\n");
        return NULL;
      }
    } else if (success<0 ){
      // Erro ao ler
      write_str(STDERR_FILENO, "Erro ao ler do pipe do server\n");
      break;
    }
  }
  return NULL;
}

//abre os pipes de um cliente que uma thread gestora foi buscar ao buffer
int iniciarSessaoCliente(Cliente *cliente){
  cliente ->resp_pipe = open(cliente->resp_pipe_path, O_WRONLY); //abre a de response no modo de escrita
  if (cliente ->resp_pipe == -1) {
    write_str(STDERR_FILENO,"Erro ao abrir o pipe de response: ");
    write_str(STDERR_FILENO,cliente->resp_pipe_path);
    write_str(STDERR_FILENO,"\n");
    return 1;
  }
  //manda que deu sucesso
  char response[3] = "10";
  int success = write_all(cliente ->resp_pipe, response, 2);
  if(success!=1){
    write_str(STDERR_FILENO, "Erro ao escrever no pipe de response\n");
    return 1;
  }
  return 0;
}

//so acaba quando o client der disconnect ou houver o sinal SIGSUR1
int manageClient(Cliente *cliente){
  //vai buscar um cliente ao buffer
  if(iniciarSessaoCliente(cliente)==1){
    return 1;
  }
  char message[2];
  cliente -> req_pipe = open(cliente->req_pipe_path, O_RDONLY); //abre o pipe de request do cliente em modo leitura
  while(!getSinalSeguranca()){ //trabalha enquanto o sinal SIGUSR1 nao for detetado
    //le o codigo que o user quer fazer
    int successCode = read_all(cliente -> req_pipe,&message, 1, NULL);
    message[1] = '\0';
    if (successCode == 1){
      //leu bem
      int code = message[0]- '0';
      int result;
      if(cliente->flag_sigusr1){
        //houve um sigusr1
        return 1;
      }
      if (code==2){
        //disconnect
        result = disconnectClient(cliente);
        if (result==0){
          //tirar do buffer
          pthread_mutex_lock(&bufferThreads->buffer_mutex); //bloquear o buffer pois vamos altera-lo
          removeClientFromBuffer(cliente);
          pthread_mutex_unlock(&bufferThreads->buffer_mutex); //desbloquear o buffer
          if(sendOperationResult(code,result,cliente)==1){
            //erro a mandar mensagem para o cliente
            return 1;
          }   
          break;
        }

      }else if (code==3){
        //subscribe
        char key[42];
        int success = read_all(cliente -> req_pipe,&key, 41, NULL); //lê a chave
        if (success==-1){
          //nao leu bem
          return 1;
        }
        key[41] = '\0';
        result = subscribeClient(cliente, key);

      }else if (code==4){
        //unsubscribe
        char key[42];
        int success = read_all(cliente -> req_pipe,&key, 41, NULL); //lê a chave
        if (success!=1){
          return 1;
        }
        key[41] = '\0';
        result = unsubscribeClient(cliente, key);

      }else{
        //leu um codigo inesperado
        return 1;
      }

      if(sendOperationResult(code,result,cliente)==1){
        //erro a mandar mensagem para o cliente
        return 1;
      }

    }else if (successCode == -1){
      //nao leu nada pois houve erro
      return 1;
    }
  }
  return 0;
}

//retira o primeiro cliente que nao tem thread associada e retorna-o
Cliente* getClientForThread(){
  User* user_atual = bufferThreads->headUser;
  if(user_atual!=NULL){
    while (user_atual->nextUser!=NULL){
      if(!user_atual->usedFlag){
        //nao esta a ser usado
        return user_atual->cliente;
      }
      user_atual = user_atual->nextUser; //passa ate encontrar um cliente cuja usedFlag seja falsa ou entao ate nao haver mais nenhum cliente disponivel
    }
    //ja esta no ultimo user
    if(user_atual->usedFlag){
      //ja esta usado
      return NULL;
    }
    //nao esta usado
    user_atual->usedFlag=true; //mete a flag do user como true pois vai ser usado
    return user_atual->cliente; //retorna o cliente que vai ser usado
  }
  return NULL;
}

//quando o manage client acaba significa q o client deu disconnect, portanto vai buscar outro client
void *readClientPipe() {
  while(1){
    sem_wait(&semaforoBuffer); //tirar 1 ao semaforo
    pthread_mutex_lock(&bufferThreads->buffer_mutex); //bloquear mutex pq vai buscar um cliente ao buffer
    Cliente *cliente = getClientForThread();
    pthread_mutex_unlock(&bufferThreads->buffer_mutex); //desbloquear mutex 
    if(cliente!=NULL){
      cliente->usado = 1;
      if(manageClient(cliente)==1){
        //deu erro a ler cliente
        free(cliente);
      }
    }
  }
}

//cria a thread que lê o server pipe, as S threads gestoras e as threads dos .job
static void dispatch_threads(DIR *dir) {
  pthread_t *threads = malloc(max_threads * sizeof(pthread_t));

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

  //cria S threads gestoras, que vao buscando clientes e tratando deles
  for (size_t thread_gestora = 0; thread_gestora < MAX_SESSION_COUNT; thread_gestora++) {
    if (pthread_create(&threads_gestoras[thread_gestora], NULL, readClientPipe,NULL) !=
        0) {
      write_str(STDERR_FILENO, "Failed to create thread gestora");
      write_uint(STDERR_FILENO, (int) thread_gestora);
      free(threads_gestoras);
      return;
    }
  }

  //inicia sessão dos clientes, lendo o server pipe
  pthread_t thread_inicioSessao;
  if (pthread_create(&thread_inicioSessao, NULL, readServerPipe,NULL) !=
      0) {
    write_str(STDERR_FILENO, "Failed to create thread inicioSessao");
    return;
  }

  //espera que as threads dos .jobs acabem
  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      write_str(STDERR_FILENO, "Failed to join thread");
      write_uint(STDERR_FILENO, (int) i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  //espera que as threads gestoras acabem
  for(unsigned int thread_gestora = 0; thread_gestora < MAX_SESSION_COUNT; thread_gestora++){
    if (pthread_join(threads_gestoras[thread_gestora], NULL) != 0) {
      write_str(STDERR_FILENO, "Failed to join thread gestora ");
      write_uint(STDERR_FILENO, (int) thread_gestora);
      free(threads_gestoras);
      return;
    }
  }

  //espera que a thread principal que lê o server pipe acabe
  if (pthread_join(thread_inicioSessao, NULL) != 0) {
      write_str(STDERR_FILENO, "Failed to join thread inicioSessao ");
      return;
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

  //criar FIFO po server
  strcat(fifo_path,nome_fifo);
  if (mkfifo(fifo_path, 0777) == -1) {
      write_str(STDERR_FILENO, "Failed to create FIFO: ");
      write_str(STDERR_FILENO, fifo_path);
      write_str(STDERR_FILENO, "\n");
      return 0;
  }


  sem_init(&semaforoBuffer, 0, MAX_SESSION_COUNT); //inicializar semaforo a 0 e vai até S
  
  //cria o buffer para as threads gestoras
  bufferThreads = (BufferUserConsumer*)malloc(sizeof(BufferUserConsumer));
  bufferThreads->headUser = NULL;
  pthread_mutex_init(&bufferThreads->buffer_mutex, NULL);

  //cria as threads todas e espera q acabem
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
  close(server_fifo); //fecha o pipe do server
  pthread_mutex_destroy(&bufferThreads->buffer_mutex); //destroi o lock do buffer
  free(bufferThreads);
  sem_destroy(&semaforoBuffer); //destroy o semaforo

  return 0;
}
