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
#include <pthread.h>
#include <fcntl.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"

#define MAX_QUEUE_SIZE 1024

// Estruturas para sincronização e fila de trabalho
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t queue_cond = PTHREAD_COND_INITIALIZER;

char *job_queue[MAX_QUEUE_SIZE];
int queue_start = 0, queue_end = 0, queue_count = 0;
int max_threads;

// Função para adicionar um ficheiro à fila de trabalho
void enqueue_job(const char *job_path) {
    pthread_mutex_lock(&queue_mutex);
    if (queue_count < MAX_QUEUE_SIZE) {
        job_queue[queue_end] = strdup(job_path);
        queue_end = (queue_end + 1) % MAX_QUEUE_SIZE;
        queue_count++;
        pthread_cond_signal(&queue_cond);
    }
    pthread_mutex_unlock(&queue_mutex);
}

// Função para retirar um ficheiro da fila de trabalho
char *dequeue_job() {
    pthread_mutex_lock(&queue_mutex);
    while (queue_count == 0) {
        pthread_cond_wait(&queue_cond, &queue_mutex);
    }
    char *job_path = job_queue[queue_start];
    queue_start = (queue_start + 1) % MAX_QUEUE_SIZE;
    queue_count--;
    pthread_mutex_unlock(&queue_mutex);
    return job_path;
}

// Função executada por cada thread
void *worker_thread(void *arg) {
    while (1) {
        char *job_path = dequeue_job();
        if (job_path == NULL) {
            break; // Sinal de terminação
        }

        // Criar caminho para o ficheiro de saída
        char output_path[PATH_MAX];
        strncpy(output_path, job_path, PATH_MAX);
        char *ext = strrchr(output_path, '.');
        if (ext != NULL) {
            strcpy(ext, ".out");
        } else {
            strncat(output_path, ".out", PATH_MAX - strlen(output_path) - 1);
        }

        // Limpar o KVS para o próximo ficheiro
        kvs_clear();

        // Processar o ficheiro
        process_job_file(job_path, output_path, max_threads);

        free(job_path);
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <directory> <max_backups> <max_threads>\n", argv[0]);
        return 1;
    }

    char *directory = argv[1];
    int max_backups = atoi(argv[2]);
    max_threads = atoi(argv[3]);

    if (max_threads <= 0 || max_backups <= 0) {
        fprintf(stderr, "Invalid value for max_backups or max_threads\n");
        return 1;
    }

    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        return 1;
    }

    DIR *dir = opendir(directory);
    if (dir == NULL) {
        perror("Error opening directory");
        kvs_terminate();
        return 1;
    }

    // Criar threads
    pthread_t threads[max_threads];
    for (int i = 0; i < max_threads; i++) {
        pthread_create(&threads[i], NULL, worker_thread, NULL);
    }

    // Adicionar ficheiros .job à fila
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strstr(entry->d_name, ".job") != NULL) {
            char job_path[PATH_MAX];
            snprintf(job_path, PATH_MAX, "%s/%s", directory, entry->d_name);
            enqueue_job(job_path);
        }
    }

    closedir(dir);

    // Adicionar sinais de terminação para as threads
    for (int i = 0; i < max_threads; i++) {
        enqueue_job(NULL);
    }

    // Esperar que as threads terminem
    for (int i = 0; i < max_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    kvs_terminate();
    return 0;
}
