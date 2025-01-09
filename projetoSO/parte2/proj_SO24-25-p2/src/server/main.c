#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "constants.h"
#include "io.h"
#include "operations.h"
#include "parser.h"
#include "pthread.h"
#include "kvs.h"
#include "src/common/constants.h"

struct SharedData {
  DIR *dir;
  char *dir_name;
  pthread_mutex_t directory_mutex;
};

struct SharedDataGestoras {
  Cliente* cliente;
};

Cliente* listaClientes[40] = {NULL};
int numClientes=0;

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
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
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
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent *entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
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
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

void iniciar_sessao(char *message){
  int code;
  char pipe_req[40], pipe_resp[40], pipe_notif[40];
  if (sscanf(message, "%d %s %s %s", &code, pipe_req, pipe_resp, pipe_notif) == 4) {
    int response_pipe = open(pipe_resp, O_WRONLY);
    if (response_pipe == -1) {
      fprintf(stderr,"Erro ao abrir o pipe de response");
      return;
    }
    if(code==1){
      Cliente *new_cliente = malloc(sizeof(Cliente));
      if (new_cliente == NULL) {
        fprintf(stderr, "Erro ao alocar memória para novo cliente\n");
        if (write(response_pipe, "1", 2) == -1) {
          fprintf(stderr,"Erro ao enviar pedido de subscrição");
        }
        return;
      }

      // Inicializa os campos da estrutura
      strcpy(new_cliente->req_pipe_path, pipe_req);
      strcpy(new_cliente->resp_pipe_path, pipe_resp);
      strcpy(new_cliente->notif_pipe_path, pipe_notif);
      listaClientes[numClientes] = new_cliente;
      numClientes++;

      //manda que deu sucesso
      if (write(response_pipe, "0", 2) == -1) {
        fprintf(stderr,"Erro ao enviar pedido de subscrição");
        return;
      }
      return;
    }
  }
  printf("Erro ao iniciar sessao de novo cliente.\n");
  return;
}

int subscribeClient(Cliente *cliente, char *message){
  char key[41];
  int code;
  sscanf(message,"%d %s",&code, key);

  if (addSubscriber(cliente, key)==0){
    //a key existe e deu certo
    return 0;
  }
  return 1;
}

int unsubscribeClient(Cliente *cliente, char *message){
  char key[41];
  int code;
  sscanf(message,"%d %s",&code, key);

  if (removeSubscriber(cliente, key)==0){
    //a subscricao existia e deu certo
    return 0;
  }
  return 1;
}

int disconnectClient(Cliente *cliente){
  Subscriptions *currentSubscr = cliente->subscricoes;
  Subscriptions *prevSubscr = NULL;
  while (currentSubscr != NULL) {
    KeyNode *keyNode = currentSubscr->key;
    Subscribers *currentSub = keyNode->subscribers;
    Subscribers *prevSub = NULL;

    // Remoção na lista de subscribers associada à chave
    while (currentSub != NULL) {
      if (strcmp(currentSub->cliente->resp_pipe_path, cliente->resp_pipe_path) == 0 &&
        strcmp(currentSub->cliente->notif_pipe_path, cliente->notif_pipe_path) == 0 &&
        strcmp(currentSub->cliente->req_pipe_path, cliente->req_pipe_path) == 0) {
        if (prevSub == NULL) {
          keyNode->subscribers = currentSub->next; // Remove caso seja o primeiro elemento
        } else {
          prevSub->next = currentSub->next; // Remove para os outros casos
        }
        free(currentSub); // Liberta a memória do subscritor
        break; // Terminamos a remoção para este subscritor
      }
      prevSub = currentSub;
      currentSub = currentSub->next;
    }
    // Próxima associação na lista de subscrições
    Subscriptions *nextSubscr = currentSubscr->next;
    free(currentSubscr); // Libera a memória da associação
    currentSubscr = nextSubscr;
  }

  cliente->subscricoes = NULL; // Após desconectar, limpar a lista de subscrições

  return 0; // Sucesso
}

void *readServerPipe(){
  //ler FIFO
  char message[128];
  while (1) {
    ssize_t bytes_read = read(server_fifo, &message, sizeof(message));
    if (bytes_read > 0){
      int code = message[0];
      if (code==1){
        iniciar_sessao(message);
      }else{
        
      }
      
    } else if (bytes_read == 0) {
      // EOF: O pipe foi fechado
      break;
    } else {
      // Erro ao ler
      fprintf(stderr, "Erro ao ler do pipe de requests\n");
      break;
    }
   
  }
  return;
}

void *readClientPipe(void *arguments){
  char message[43];
  Cliente *cliente = (Cliente *)arguments;
  int request_pipe = open(cliente->req_pipe_path, O_RDONLY);
  int response_pipe = open(cliente->resp_pipe_path, O_WRONLY);
  while (1) {
    ssize_t bytes_read = read(request_pipe, &message, sizeof(message));
    if (bytes_read > 0){
      int code = message[0];
      int result;
      if (code==2){
        //disconnect
        result = disconnectClient(cliente);

      }else if (code==3){
        //subscribe
        result = subscribeClient(cliente, message);
      
      }else if (code==4){
        //unsubscribe
        result = unsubscribeClient(cliente, message);

      }else{
        //erro
      }

      //escreve se a operacao deu certo (0) ou errado (1)
      char response[4];
      snprintf(response,4,"%d %d", code, result);
      write(response_pipe, response, 2);
      
    } else if (bytes_read == 0) {
      // EOF: O pipe foi fechado
      break;
    } else {
      // Erro ao ler
      fprintf(stderr, "Erro ao ler do pipe de requests\n");
      break;
    }
   
  }
}


static void dispatch_threads(DIR *dir) {
  pthread_t *threads = malloc(max_threads * sizeof(pthread_t));
  pthread_t *threads_gestoras = malloc(MAX_SESSION_COUNT * sizeof(pthread_t));

  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory,
                                   PTHREAD_MUTEX_INITIALIZER};

  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void *)&thread_data) !=
        0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  

  // ler do pipe de registo de cada cliente
  for (size_t thread_gestora = 0; thread_gestora < MAX_SESSION_COUNT; thread_gestora++) {
    struct SharedDataGestoras threadGestoras_data = {listaClientes[thread_gestora]};
    if (pthread_create(&threads_gestoras[thread_gestora], NULL, readClientPipe,(void *)&threadGestoras_data) !=
        0) {
      fprintf(stderr, "Failed to create thread gestora %zu\n", thread_gestora);
      free(threads_gestoras);
      return;
    }
  }

  //inicia sessão dos clientes
  readServerPipe();

  for(unsigned int thread_gestora = 0; thread_gestora < MAX_SESSION_COUNT; thread_gestora++){
    if (pthread_join(threads_gestoras[thread_gestora], NULL) != 0) {
      fprintf(stderr, "Failed to join thread gestora %u\n", thread_gestora);
      free(threads_gestoras);
      return;
    }
  }

  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
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
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  max_threads = strtoul(argv[2], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
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
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  //criar FIFO
  if (mkfifo(nome_fifo, 0666) == -1) {
      fprintf(stderr, "Failed to create FIFO: %s\n", argv[4]);
      exit(EXIT_FAILURE);
  }
  server_fifo = open(nome_fifo, O_RDONLY);
  if (server_fifo == -1) {
    fprintf(stderr, "Failed to open fifo: %s\n", nome_fifo);
    return 0;
  }

  dispatch_threads(dir);

  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  kvs_terminate();

  return 0;
}
