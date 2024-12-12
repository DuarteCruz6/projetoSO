#include <limits.h>
#include <stdio.h>
#include <unistd.h> 
#include <sys/wait.h>  
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"

// Variáveis globais para controle de threads
int MAX_BACKUPS=0;
int MAX_THREADS=0;
long unsigned MAX_PATH_NAME_SIZE = 0;

typedef struct {
    char *job_input_path;
    int num_thread;
    pthread_rwlock_t* mutex_active_threads;
    pthread_rwlock_t* mutex_active_backups;
    int* active_threads;
} thread_args;


// Função para processar o file .job
// O parâmetro input_path é o caminho para o file de entrada .job
// O parâmetro output_path é o caminho para o file de saída .out

void process_job_file(const char *input_path, const char *output_path, int* backups_ativos, 
  pthread_rwlock_t* mutex_activeBackups) {

  //pid_t backupsConcorrentes[num_backups_concorrentes];
  int id_backup=1;
  // Abrir o file .job em modo leitura
  int fd_in = open(input_path, O_RDONLY);
  if (fd_in < 0) {
    fprintf(stderr, "Error opening input file _%s_\n",input_path);  // Se falhar, print erro
    return;
  }

  // Criar ou abrir o file .out em modo escrita
  int fd_out = open(output_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (fd_out < 0) {
    fprintf(stderr, "Error opening output file\n");  // Se falhar, print erro
    close(fd_in);
    return;
  }

  // Processar os comandos do file .job
  while (1) {
    // Obtém o próximo comando do file .job
    enum Command cmd = get_next(fd_in);
    

    // Dependendo do comando, executa a ação correspondente
    switch (cmd) {
      case CMD_WRITE: {
        // Declara as variáveis para armazenar as chaves e valores
        char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE];
        char values[MAX_WRITE_SIZE][MAX_STRING_SIZE];
        
        // Lê as chaves e valores do file
        size_t num_pairs = parse_write(fd_in, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          fprintf(stderr, "Invalid WRITE command. See HELP for usage\n");
          continue;
        }
        // Chama a função de escrita no KVS, passando os pares chave-valor
        if (kvs_write(fd_out, num_pairs, keys, values)) {
          fprintf(stderr, "Failed to write pair\n");
        }
        break;
      }

      case CMD_READ: {
        // Declara a variável para armazenar as chaves
        char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE];
        
        // Lê as chaves para o comando READ
        size_t num_pairs = parse_read_delete(fd_in, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid READ command. See HELP for usage\n");
          continue;
        }

        // Lê os valores do KVS
        //printf("1 ler %s numero pares %ld chaves %s \n",output_path,num_pairs,keys[0]);
        if (kvs_read(fd_out, num_pairs, keys)) {
          fprintf(stderr, "Failed to read pair\n");
        }
        //printf("2 ler %s numero pares %ld chaves %s \n",output_path,num_pairs,keys[0]);
        break;
      }

      case CMD_DELETE: {
        // Declara a variável para armazenar as chaves
        char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE];
        
        // Lê as chaves para o comando DELETE
        size_t num_pairs = parse_read_delete(fd_in, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid DELETE command. See HELP for usage\n");
          continue;
        }

        // Deleta os pares do KVS
        if (kvs_delete(fd_out, num_pairs, keys)) {
          fprintf(stderr, "Failed to delete pair\n");
        }
        break;
      }

      case CMD_SHOW:
        // Exibe o estado atual do KVS
        kvs_show(fd_out);
        break;

      case CMD_WAIT: {
        unsigned int delay;
        
        // Lê o tempo de delay para o comando WAIT
        if (parse_wait(fd_in, &delay, NULL) == -1) {
          fprintf(stderr, "Invalid WAIT command. See HELP for usage\n");
          continue;
        }
        
        // Espera o tempo especificado
        kvs_wait(delay);
        break;
      }

      case CMD_EMPTY:
        // Comando vazio, nada a fazer
        break;

      case EOC:
        // Fim, termina a função
        //printf(":\n");
       // printf("a acabar o input %s com o output %s\n",input_path,output_path);
        //printf(":\n");
        // Fechar os files após o processamento
        close(fd_in);
        close(fd_out);
        return;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_BACKUP:
        pthread_rwlock_rdlock(mutex_activeBackups);
        if ((*backups_ativos) >= MAX_BACKUPS) {
            //tem de esperar que um backup acabe, pois ja ta a acontecer o numero maximo de backups
            
            pid_t terminated_pid = waitpid(-1, NULL, 0); // Espera por qualquer processo filho
            if (terminated_pid > 0) {  
                pthread_rwlock_unlock(mutex_activeBackups);
                pthread_rwlock_wrlock(mutex_activeBackups);
                (*backups_ativos)--; //remove um ao numero de processos filhos a acontecer
                pthread_rwlock_unlock(mutex_activeBackups);
            } else if (terminated_pid == -1) {
              fprintf(stderr, "Error waiting for child process\n");
            }
        }
        pthread_rwlock_unlock(mutex_activeBackups);

        pid_t pid = fork();

        if (pid<0){
          fprintf(stderr, "Error creating child process\n");
          break;

        }else if (pid==0){
          //processo filho
          
          char backup_path[MAX_JOB_FILE_NAME_SIZE+16];
          strncpy(backup_path, output_path, strlen(output_path)-4); //cria o backup_path igual a output_path mas sem o .out
          backup_path[strlen(output_path) - 4] = '\0';
          
          char backup_id[20];

          sprintf(backup_id, "-%d.bck", id_backup); //backup_id = "-{id}.bck"

          strcat(backup_path,backup_id); //adiciona o backup_id ao backup_path 
         
          int fd_backup = open(backup_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
          
          //cria o ficheiro de backup no backup_path
          if (fd_backup < 0) {
            
            fprintf(stderr, "Error opening backup file\n");
            close(fd_in);
            _exit(1);
          }
          
          kvs_backup(fd_backup); //cria o backup no ficheiro
          
          
          close(fd_backup); //fecha o ficheiro de backup
          
          _exit(0);
        }else{
          //processo pai pode continuar
          pthread_rwlock_wrlock(mutex_activeBackups);
          (*backups_ativos)++;     //adiciona um ao numero de backups a decorrer
          pthread_rwlock_unlock(mutex_activeBackups);
          id_backup++;  //adiciona um ao id de processos filhos
        }
         
        break;
      case CMD_HELP:
        // Exibe a ajuda de todos os comandos
        printf( 
            "Available commands:\n"
            "  WRITE [(key,value)(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n"
            "  HELP\n"
        );
        break;

      default:
        // Comando desconhecido
        fprintf(stderr, "ERROR: Unknown command\n");
        break;
    }
  }
  //printf("a acabar o input %s\n",input_path);
  // Fechar os files após o processamento
  close(fd_in);
  close(fd_out);
  free(backups_ativos);
}

void *thread_work(void *arguments){
  thread_args args = *((thread_args*)arguments);  // Converter o argumento void* para thread_args*

  char *job_input_path = (char *)malloc(MAX_PATH_NAME_SIZE * sizeof(char));
  strcpy(job_input_path, args.job_input_path);

  //criar os ficheiros .out
  char job_output_path[MAX_PATH_NAME_SIZE];
  // Substituir extensão .job por .out
  strncpy(job_output_path, job_input_path, MAX_PATH_NAME_SIZE);
  char *ext = strrchr(job_output_path, '.');  // Encontrar a última ocorrência de '.'
  if (ext != NULL) {
    strcpy(ext, ".out");  // Substituir .job por .out
  } else {
    strncat(job_output_path, ".out", MAX_PATH_NAME_SIZE - strlen(job_output_path) - 1);  // Garantir que .out seja adicionado
  }

  //processar os .job
  //int num_thread = args.num_thread;
  //printf("thread %d work\n", num_thread);
  process_job_file(job_input_path,job_output_path,args.active_threads,args.mutex_active_backups);
   // Espera que todos os processos filhos terminem
    for (int i = 0; i < MAX_BACKUPS; ++i) {
        wait(NULL); // Espera qualquer processo filho terminar
    }
  
  pthread_rwlock_wrlock(args.mutex_active_threads);
  args.active_threads--;
  pthread_rwlock_unlock(args.mutex_active_threads);

  free(args.job_input_path);
  free(job_input_path);
  free(arguments);
  return NULL;
}

void wait_for_threads(int thread_count,pthread_t *lista_threads){
  // Esperar por todas as threads restantes
  for (int i = 0; i < thread_count; i++) {
      pthread_join(lista_threads[i], NULL);

  }
}

// Função de comparação para qsort
int compare_files(const void *a, const void *b) {
    const char *file_a = *(const char **)a;
    const char *file_b = *(const char **)b;
    return strcmp(file_a, file_b); // Ordenação alfabética
}

// Função para ordenar a lista de ficheiros
void order_files(char **lista_ficheiros, int num_files) {
    if (num_files > 1) { // Apenas ordena se existir mais de um ficheiro
        qsort(lista_ficheiros, (size_t)num_files, sizeof(char *), compare_files);
    }
}

void get_path(const char *directory, char **lista_ficheiros){
  // Abrir o diretório
  DIR *dir = opendir(directory);
  if (dir == NULL) {
    perror("Error opening directory");
    return;
  }

  struct dirent *entry;

  // Contar o número de ficheiros .job no diretório
  int num_files = 0;
  while ((entry = readdir(dir)) != NULL) {
      if (strstr(entry->d_name, ".job") != NULL) {
          // Construir caminhos para os files de entrada
          lista_ficheiros = realloc(lista_ficheiros, (size_t)(num_files + 1) * sizeof(char*));
          char *job_input_path = malloc(MAX_PATH_NAME_SIZE * sizeof(char));
          snprintf(job_input_path, MAX_PATH_NAME_SIZE, "%s/%s", directory, entry->d_name);
          lista_ficheiros[num_files]=job_input_path;
          num_files++;  // Contar os ficheiros .job
      }
  }
  //ordena a lista de files por ordem alfabetica
  if(num_files>1){
    order_files(lista_ficheiros, (size_t) num_files);
  }
}

void create_threads(const char *directory) {
  DIR *dir = opendir(directory);
  char **lista_ficheiros = malloc(0 * sizeof(char*));
  get_path(directory,lista_ficheiros);

  pthread_t *lista_threads = malloc((size_t)MAX_THREADS * sizeof(pthread_t));

  int thread_count = 0;

  int* backups_a_decorrer = malloc(sizeof(int));
  int* active_threads = malloc(sizeof(int));

  pthread_rwlock_t *mutex_backups_a_decorrer=malloc(sizeof(pthread_rwlock_t));
  pthread_rwlock_init(mutex_backups_a_decorrer,NULL);
  pthread_rwlock_t *mutex_threads_a_decorrer=malloc(sizeof(pthread_rwlock_t));
  pthread_rwlock_init(mutex_threads_a_decorrer,NULL);

  // Iterar pelos arquivos do diretório
  
    for(int i=0;i<sizeof(lista_ficheiros);i++){

      char job_input_path[MAX_PATH_NAME_SIZE];

      snprintf(job_input_path, MAX_PATH_NAME_SIZE, "%s", lista_ficheiros[i]);
      
      // Criar argumentos para a thread
      thread_args *args_thread = malloc(sizeof(thread_args));

      args_thread->job_input_path= strdup(job_input_path);

      args_thread->num_thread = thread_count;
      args_thread->active_threads=backups_a_decorrer;

      args_thread->mutex_active_backups= mutex_backups_a_decorrer;
      args_thread->mutex_active_threads= mutex_threads_a_decorrer;

      // Esperar caso o número de threads ativas atinja o limite
      pthread_rwlock_rdlock(args_thread->mutex_active_threads);
      while ((*active_threads) >= MAX_THREADS) {
          
      }
      pthread_rwlock_unlock(args_thread->mutex_active_threads);
      // Criar nova thread
      if (pthread_create(&lista_threads[thread_count], NULL, thread_work, (void*)args_thread) != 0) {
          fprintf(stderr, "Falha ao criar thread para o arquivo %s\n", job_input_path);
          free(args_thread->job_input_path);
          free(args_thread);
      } else {
          pthread_rwlock_wrlock(args_thread->mutex_active_threads);
          (*active_threads)++;
          pthread_rwlock_unlock(args_thread->mutex_active_threads);
          thread_count++;
      }
    
      //free(args_thread);
    }
  
  //free(args_thread);

  // Esperar que todas as threads terminem
  wait_for_threads(thread_count,lista_threads);

  free(lista_threads);
  for (int i = 0; i < sizeof(lista_ficheiros); i++) {
      free(lista_ficheiros[i]);  // Libertar cada caminho alocado
  }
  free(lista_ficheiros);  // Libertar a lista de caminhos
  pthread_rwlock_destroy(mutex_backups_a_decorrer);
  pthread_rwlock_destroy(mutex_threads_a_decorrer);
  free(mutex_backups_a_decorrer);
  free(mutex_threads_a_decorrer);

  free(backups_a_decorrer);
  free(active_threads);
  // Fechar o diretório após iteração
  closedir(dir);
}

void create_files(char *directory){
  // Inicializar KVS
  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return;
  }

  create_threads(directory);

}


int main(int argc, char *argv[]) {
  // Verificar número de argumentos
  if (argc < 4) {
    fprintf(stderr, "Usage: %s <directory> <MAX_BACKUPS>\n", argv[0]);
    return 1;
  }

  char *directory = argv[1];
  MAX_BACKUPS = atoi(argv[2]);
  MAX_THREADS = atoi(argv[3]);
  MAX_PATH_NAME_SIZE = (long unsigned)pathconf(".", _PC_PATH_MAX);

  // Validar valor de MAX_BACKUPS
  if (MAX_BACKUPS <= 0) {
    fprintf(stderr, "Invalid value for MAX_BACKUPS\n");
    return 1;
  }

  // Validar valor de MAX_THREADS
  if (MAX_THREADS <= 0) {
    fprintf(stderr, "Invalid value for MAX_THREADS\n");
    return 1;
  }

  create_files(directory);
  kvs_clear();
  // Finalizar KVS
  kvs_terminate();
  return 0;
}