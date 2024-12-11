/*

FILE: main.c

Uso do programa: 
abra o diretório onde está a raiz do projeto;
make -> para compilar;
./kvs <directory> <max_backups> -> para rodar;

ex:
./kvs tests-public/jobs 1

Descrição:
Este file contém a função principal do sistema. Ele é responsável por inicializar o sistema KVS (Key-Value Store),
processar files de comandos .job, e gerar os respectivos files de saída .out. A função 'main' é responsável por 
abrir o diretório onde os files .job estão localizados, processá-los um a um, executando os comandos especificados
em cada file e garantindo que o estado do KVS seja reinicializado entre cada file.

Funcionalidades:
- Inicialização do KVS.
- Leitura e execução de comandos dos files .job (como WRITE, READ, DELETE, SHOW, etc.).
- Criação de files .out com os resultados da execução dos comandos.
- Processamento de múltiplos files .job de forma sequencial.
- Gerenciamento de erros, com mensagens de erro escritas no file de saída .out.

Funções:
- 'process_job_file': Processa um file .job específico, executando os comandos contidos nele e escrevendo os resultados 
  no file de saída.
- 'main': Função principal, que inicializa o KVS, lê files .job de um diretório e os processa, gerando os files .out 
  correspondentes.

Autores:
- Davi Rocha
- Duarte Cruz

Data de Finalização:
- 08/12/2024 - Parte 1

*/



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


// Função para processar o file .job
// O parâmetro input_path é o caminho para o file de entrada .job
// O parâmetro output_path é o caminho para o file de saída .out

void do_backup(int fd_out){
  //faz o backup do kvs para o ficheiro
  kvs_backup(fd_out);
}

void process_job_file(const char *input_path, const char *output_path, const int num_backups_concorrentes) {

  //pid_t backupsConcorrentes[num_backups_concorrentes];
  int backupsDecorrer=0;
  int id_backup=1;
  // Abrir o file .job em modo leitura
  int fd_in = open(input_path, O_RDONLY);
  if (fd_in < 0) {
    fprintf(stderr, "Error opening input file");  // Se falhar, print erro
    return;
  }

  // Criar ou abrir o file .out em modo escrita
  int fd_out = open(output_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (fd_out < 0) {
    fprintf(stderr, "Error output input file");  // Se falhar, print erro
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
        return;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_BACKUP:
        
        if (backupsDecorrer >= num_backups_concorrentes) {
            //tem de esperar que um backup acabe, pois ja ta a acontecer o numero maximo de backups
            pid_t terminated_pid = wait(NULL);
            if (terminated_pid > 0) {  
                backupsDecorrer--; //remove um ao numero de processos filhos a acontecer
            } else if (terminated_pid == -1) {
              fprintf(stderr, "Error waiting for child process\n");
            }
        }

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
            exit(1);
          }
          do_backup(fd_backup); //cria o backup no ficheiro
          close(fd_backup); //fecha o ficheiro de backup
          exit(0);
        }else{
          //processo pai pode continuar
          backupsDecorrer++;     //adiciona um ao numero de backups a decorrer
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
            "  BACKUP\n" // Not implemented (yet)
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
}

typedef struct {
    char *job_input_path;
    int num_backups;
} thread_args;

void *thread_work(void *arguments){
  thread_args* args = (thread_args*) arguments;

  long unsigned max_path_name_size = (long unsigned)pathconf(".", _PC_PATH_MAX);

  char job_input_path[max_path_name_size];
  strcpy(job_input_path, args->job_input_path);

  //criar os ficheiros .out
  char job_output_path[max_path_name_size];
  // Substituir extensão .job por .out
  strncpy(job_output_path, job_input_path, max_path_name_size);
  char *ext = strrchr(job_output_path, '.');  // Encontrar a última ocorrência de '.'
  if (ext != NULL) {
    strcpy(ext, ".out");  // Substituir .job por .out
  } else {
    strncat(job_output_path, ".out", max_path_name_size - strlen(job_output_path) - 1);  // Garantir que .out seja adicionado
  }

  //processar os .job
  int num_backups = args->num_backups;
  process_job_file(job_input_path,job_output_path,num_backups);
}

void wait_for_threads(int max_threads,pthread_t lista_threads[]){
  //wait for each thread to complete
  for (int i = 0; i < max_threads; i++) {
    if(pthread_join(lista_threads[i], NULL)!=0){
      fprintf(stderr, "Failed to end thread\n");
    }
  }
}

void create_threads(int max_threads,pthread_t lista_threads[], const char *input_path,int num_backups){
  //create all threads one by one
  int result_code;
  for (int i=0; i < max_threads; i++) {
    printf("In main: Creating thread %d.\n", i);
    thread_args* args = (thread_args*) malloc(sizeof(thread_args));
    args->job_input_path = strdup(input_path);  // Copia o caminho do arquivo
    args->num_backups = num_backups;
    if(pthread_create(&lista_threads[i], NULL, thread_work, args)!=0){
      fprintf(stderr, "Failed to create thread\n");
    }
  }
  wait_for_threads(max_threads,lista_threads);
}

void do_backup(int fd_out){
  //faz o backup do kvs para o ficheiro
  kvs_backup(fd_out);
}

void create_files(char *directory, int max_backups, int max_threads){
  // Inicializar KVS
  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

  pthread_t lista_threads[max_threads];
  
  // Abrir o diretório
  DIR *dir = opendir(directory);
  if (dir == NULL) {
    perror("Error opening directory");
    kvs_terminate();
    return 1;
  }

  struct dirent *entry;
  while ((entry = readdir(dir)) != NULL) {
    // Verificar a extensão .job
    if (strstr(entry->d_name, ".job") != NULL) {
      // Construir caminhos para os files de entrada e saída
      long unsigned max_path_name_size = (long unsigned)pathconf(".", _PC_PATH_MAX);
      char job_input_path[max_path_name_size];
      snprintf(job_input_path, max_path_name_size, "%s/%s", directory, entry->d_name);

      // Limpar o KVS para o próximo file
      kvs_clear();
      create_threads(max_threads,lista_threads,job_input_path,max_backups);
    }
  }

  // Fechar o diretório
  closedir(dir);

  // Finalizar KVS
  kvs_terminate();
}


int main(int argc, char *argv[]) {
  // Verificar número de argumentos
  if (argc < 4) {
    fprintf(stderr, "Usage: %s <directory> <max_backups>\n", argv[0]);
    return 1;
  }

  char *directory = argv[1];
  int max_backups = atoi(argv[2]);
  int max_threads = atoi(argv[3]);

  // Validar valor de max_backups
  if (max_backups <= 0) {
    fprintf(stderr, "Invalid value for max_backups\n");
    return 1;
  }

  // Validar valor de max_threads
  if (max_threads <= 0) {
    fprintf(stderr, "Invalid value for max_threads\n");
    return 1;
  }

  create_files(directory,max_backups,max_threads);
  return 0;
}