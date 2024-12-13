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

//variáveis globais para estabelecer limites
int MAX_BACKUPS=0;
int MAX_THREADS=0;
long unsigned MAX_PATH_NAME_SIZE = 0;

//estrutura dos argumentos das threads
typedef struct {
    char *job_input_path; //o caminho da diretoria do .job
    int num_thread; //o id da thread
    pthread_rwlock_t* mutex_active_threads; //mutex do tipo write/read para o número de threads a acontecer ao mesmo tempo
    pthread_rwlock_t* mutex_active_backups; //mutex do tipo write/read para o número de backups a acontecer ao mesmo tempo
    int* active_threads;  //ponteiro para o número de threads a acontecer ao mesmo tempo
} thread_args;


///funcao para processar um .job
///@param input_path caminho do .job
///@param output_path caminho do .out
///@param backups_ativos ponteiro para o número de backups a acontecer ao mesmo tempo
///@param mutex_activeBackups mutex do tipo write/read para o número de backups a acontecer ao mesmo tempo

void process_job_file(const char *input_path, const char *output_path, int* backups_ativos, 
  pthread_rwlock_t* mutex_activeBackups) {

  int id_backup=1; //id do backup

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
        if (kvs_read(fd_out, num_pairs, keys)) {
          fprintf(stderr, "Failed to read pair\n");
        }
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
        // Fechar os files após o processamento
        close(fd_in);
        close(fd_out);
        return;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_BACKUP:
        pthread_rwlock_rdlock(mutex_activeBackups); //da lock do tipo read ao mutex do numero de backups a acontecer ao mesmo tempo
                                                    //de forma a que nenhuma thread possa mudar o valor da variável backups_ativos
                                                    //mas de forma a permitir que leiam o valor da variável 

        if ((*backups_ativos) >= MAX_BACKUPS) {
            //tem de esperar que um backup acabe, pois ja ta a acontecer o numero maximo de backups
            pid_t terminated_pid = waitpid(-1, NULL, 0); // Espera por qualquer processo filho

            if (terminated_pid > 0) {  
                //tem de alterar o valor da variavel backups_ativos
                pthread_rwlock_unlock(mutex_activeBackups); //da unlock ao mutex para podermos bloquear com o tipo write lock
                pthread_rwlock_wrlock(mutex_activeBackups); //da lock do tipo write lock ao mutex de forma a que nenhuma outra thread
                                                            //possa escrever/ler esta variavel ate darmos unlock
                (*backups_ativos)--; //remove um ao numero de processos filhos a acontecer
                pthread_rwlock_unlock(mutex_activeBackups); //da unlock pois ja mexemos no valor da variavel

            } else if (terminated_pid == -1) {
              //deu erro
              fprintf(stderr, "Error waiting for child process\n");
            }
        }
        pthread_rwlock_unlock(mutex_activeBackups); //da unlock (só é necessario caso o if nao tenha sido true)

        pid_t pid = fork(); //cria um processo filho

        if (pid<0){
          fprintf(stderr, "Error creating child process\n");
          break;

        }else if (pid==0){
          //estamos no processo filho
          
          char backup_path[MAX_JOB_FILE_NAME_SIZE+16];
          strncpy(backup_path, output_path, strlen(output_path)-4); //cria o backup_path igual a output_path mas sem o .out
          backup_path[strlen(output_path) - 4] = '\0';
          
          char backup_id[20];

          sprintf(backup_id, "-%d.bck", id_backup); //backup_id = "-{id}.bck"

          strcat(backup_path,backup_id); //adiciona o backup_id ao backup_path 
         
          int fd_backup = open(backup_path, O_WRONLY | O_CREAT | O_TRUNC, 0666); //cria ou abre o ficheiro
          
          //cria o ficheiro de backup no backup_path
          if (fd_backup < 0) {
            fprintf(stderr, "Error opening backup file\n");
            close(fd_in);
            _exit(1);
          }
          
          kvs_backup(fd_backup); //faz o backup no ficheiro
          
          close(fd_backup); //fecha o ficheiro de backup
          
          _exit(0); //fecha o processo filho

        }else{
          //processo pai pode continuar

          pthread_rwlock_wrlock(mutex_activeBackups); //lock do tipo write ao numero de backups ativos, pois vamos mexer na variavel
                                                      //entao nao podemos deixar que outras threads leiam/mudem o valor da variavel
          (*backups_ativos)++;     //adiciona um ao numero de backups a decorrer
          pthread_rwlock_unlock(mutex_activeBackups); //unlock ao mutex pois ja mexemos no valor da variavel
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
  // Fechar os files após o processamento
  close(fd_in);
  close(fd_out);
  free(backups_ativos);
}

void *thread_work(void *arguments){
  //funcao das threads
  thread_args args = *((thread_args*)arguments);  // Converter o argumento void* para thread_args*

  char *job_input_path = (char *)malloc(MAX_PATH_NAME_SIZE * sizeof(char)); //caminho da diretoria
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
  process_job_file(job_input_path,job_output_path,args.active_threads,args.mutex_active_backups);
  // Espera que todos os processos filhos terminem
 
  for (int i = 0; i < MAX_BACKUPS; ++i) {
      wait(NULL); // Espera qualquer processo filho terminar
  }
    
  pthread_rwlock_wrlock(args.mutex_active_threads); //da lock do tipo write ao mutex do numero de threads ativas pq vamos
                                                    //mexer na variavel entao nao queremos que nenhuma outra thread 
                                                    //leia/escreva nela
  args.active_threads--;  //tiramos 1 ao numero de threads ativas (este argumento é um ponteiro)
  pthread_rwlock_unlock(args.mutex_active_threads); //da unlock ao mutex

  //frees a coisas que usaram memoria dinamica
  free(args.job_input_path);  
  free(job_input_path);
  free(arguments);
  return NULL;
}

void wait_for_threads(int thread_count,pthread_t *lista_threads){
  //esperar que todas as threads acabem
  for (int i = 0; i<thread_count; i++) {
      pthread_join(lista_threads[i], NULL);
  }
}

int compare_files(const void *a, const void *b) {
  //função para ordenar os files por ordem alfabetica
  const char *file_a = *(const char **)a;
  const char *file_b = *(const char **)b;
  return strcmp(file_a, file_b); // Ordenação alfabética
}

void order_files(char **lista_ficheiros, int num_files) {
  //ordenar os files por ordem alfabetica
  if (num_files > 1) { //apenas ordena se existir mais de um ficheiro
      qsort(lista_ficheiros, (size_t)num_files, sizeof(char *), compare_files);
  }
}



void create_threads(const char *directory) {
  //cria as threads
  char **lista_ficheiros = malloc(0 * sizeof(char*)); //lista com as diretorias dos .job
  //abrir o diretório
  DIR *dir = opendir(directory);
  if (dir == NULL) {
    perror("Error opening directory");
    return;
  }

  struct dirent *entry;

  //contar o número de ficheiros .job no diretório
  int num_files = 0;
  while ((entry = readdir(dir)) != NULL) {
      if (strstr(entry->d_name, ".job") != NULL) {
          //construir caminhos para os files de entrada
          lista_ficheiros = realloc(lista_ficheiros, (size_t)(num_files + 1) * sizeof(char*));
          char *job_input_path = malloc(MAX_PATH_NAME_SIZE * sizeof(char));
          snprintf(job_input_path, MAX_PATH_NAME_SIZE, "%s/%s", directory, entry->d_name);
          lista_ficheiros[num_files]=job_input_path;  //adiciona a diretoria do .job à lista
          num_files++;  //contar os ficheiros .job
      }
  }

  order_files(lista_ficheiros, (size_t) num_files); //ordena a lista de files por ordem alfabetica
  
  pthread_t *lista_threads = malloc((size_t)num_files * sizeof(pthread_t)); 

  int thread_count = 0; //numero de threads totais

  int* backups_a_decorrer = malloc(sizeof(int));
  int* active_threads = malloc(sizeof(int));

  //cria os locks do tipo read and write para o numero de backups e threads ativos 
  //pois nao queremos que varias threads mudem o valor destas variaveis ao mesmo tempo
  //nem que leiam o valor desta variavel ao mesmo tempo que outra thread muda o seu valor
  pthread_rwlock_t *mutex_backups_a_decorrer=malloc(sizeof(pthread_rwlock_t));
  pthread_rwlock_init(mutex_backups_a_decorrer,NULL); //inicializar o mutex
  pthread_rwlock_t *mutex_threads_a_decorrer=malloc(sizeof(pthread_rwlock_t));
  pthread_rwlock_init(mutex_threads_a_decorrer,NULL); //inicializar o mutex

  //iterar pelos arquivos do diretório
    for(int i=0;i<num_files;i++){
      char job_input_path[MAX_PATH_NAME_SIZE];

      snprintf(job_input_path, MAX_PATH_NAME_SIZE, "%s", lista_ficheiros[i]);

      //criar argumentos para a thread
      thread_args *args_thread = malloc(sizeof(thread_args));
      args_thread->job_input_path= strdup(job_input_path); //guarda a diretoria do .job
      args_thread->num_thread = thread_count; //guarda o numero total de threads 
      args_thread->active_threads=backups_a_decorrer; //guarda um ponteiro para o numero de backups a decorrer ao mesmo tempo
      args_thread->mutex_active_backups= mutex_backups_a_decorrer;  //guarda o mutex para a variavel do numero de backups ao mesmo tempo
      args_thread->mutex_active_threads= mutex_threads_a_decorrer;  //guarda o mutex para a variavel do numero de threads ao mesmo tempo

      while(1) {
        pthread_rwlock_rdlock(mutex_threads_a_decorrer);  //da lock do tipo read ao numero de threads a acontecer pois nao queremos que nenhuma
                                                          //thread mude o valor desta variavel, mas queremos que leiam
        if (*active_threads < MAX_THREADS) {
          //podemos criar uma nova thread pois ainda n atingimos o maximo de threads ao mesmo tempo
          pthread_rwlock_unlock(mutex_threads_a_decorrer);  //da unlock pois precisamos de dar lock do tipo write
          pthread_rwlock_wrlock(mutex_threads_a_decorrer);  //da lock do tipo write pq vamos mudar o valor da variavel
          (*active_threads)++;                              //aumentamos o valor da quantidade de threads a acontecer ao msm tempo
          pthread_rwlock_unlock(mutex_threads_a_decorrer);  //damos unlock pq ja mudamos o valor da variavel
          break;                                            //sai do while loop
        }
        //precisamos de esperar que uma thread acabe porque atingimos o maximo de threads a acontecer ao mesmo tempo
        pthread_rwlock_unlock(mutex_threads_a_decorrer);   //damos unlock ao numero de threads a acontecer pois pode ser precisa noutra thread
        //we can add a wait function if we want
      }
      pthread_rwlock_wrlock(args_thread->mutex_active_threads); //da lock do tipo write porque vamos mudar o valor da variavel
      (*active_threads)++;                                      //aumentamos o numero de threads ativas
      pthread_rwlock_unlock(args_thread->mutex_active_threads); //damos unlock pois ja alteramos o valor
      pthread_create(&lista_threads[thread_count], NULL, thread_work, args_thread); //criamos uma thread que vai fazer a funcao thread_work cujos argumentos
                                                                                    //sao args_thread
      thread_count++; //aumentamos o numero total de threads
    }
  
  wait_for_threads(thread_count,lista_threads); //esperar que todas as threads terminem

  //destruimos os locks dos mutex's porque ja nao vamos precisar mais
  pthread_rwlock_destroy(mutex_backups_a_decorrer);
  pthread_rwlock_destroy(mutex_threads_a_decorrer);
  //dar free a tudo que usou memoria dinamica
  free(mutex_backups_a_decorrer);
  free(mutex_threads_a_decorrer);
  free(lista_threads); 
  for (int i = 0; i < num_files; i++) {
      free(lista_ficheiros[i]);  // Libertar cada caminho alocado
  }
  free(lista_ficheiros);  // Libertar a lista de caminhos
  free(backups_a_decorrer);
  free(active_threads);
  //fechar o diretório após iteração
  closedir(dir);
}

void create_files(char *directory){
  //inicializar KVS
  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return;
  }
  create_threads(directory); //criar as threads
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