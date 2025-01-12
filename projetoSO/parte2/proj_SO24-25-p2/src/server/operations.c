#include "operations.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "constants.h"
#include "io.h"
#include "src/common/io.h"
#include "kvs.h"

static struct HashTable *kvs_table = NULL;
int sinalSegurancaLancado=0; //flag para saber se houve um sinal SIGUSR1 lancado ou nao (0-false 1-true)

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
  if (kvs_table != NULL) {
    write_str(STDERR_FILENO, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    write_str(STDERR_FILENO, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  kvs_table = NULL;
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE],
              char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    write_str(STDERR_FILENO, "KVS state must be initialized\n");
    return 1;
  }

  pthread_rwlock_wrlock(&kvs_table->tablelock);

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      write_str(STDERR_FILENO, "Failed to write key pair (");
      write_str(STDERR_FILENO, keys[i]);
      write_str(STDERR_FILENO, ",");
      write_str(STDERR_FILENO, values[i]);
      write_str(STDERR_FILENO, ")\n");
    }
  }

  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    write_str(STDERR_FILENO, "KVS state must be initialized\n");
    return 1;
  }

  pthread_rwlock_rdlock(&kvs_table->tablelock);

  write_str(fd, "[");
  for (size_t i = 0; i < num_pairs; i++) {
    char *result = read_pair(kvs_table, keys[i]);
    char aux[MAX_STRING_SIZE];
    if (result == NULL) {
      snprintf(aux, MAX_STRING_SIZE, "(%s,KVSERROR)", keys[i]);
    } else {
      snprintf(aux, MAX_STRING_SIZE, "(%s,%s)", keys[i], result);
    }
    write_str(fd, aux);
    free(result);
  }
  write_str(fd, "]\n");

  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    write_str(STDERR_FILENO, "KVS state must be initialized\n");
    return 1;
  }

  pthread_rwlock_wrlock(&kvs_table->tablelock);

  int aux = 0;
  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        write_str(fd, "[");
        aux = 1;
      }
      char str[MAX_STRING_SIZE];
      snprintf(str, MAX_STRING_SIZE, "(%s,KVSMISSING)", keys[i]);
      write_str(fd, str);
    }
  }
  if (aux) {
    write_str(fd, "]\n");
  }

  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

void kvs_show(int fd) {
  if (kvs_table == NULL) {
    write_str(STDERR_FILENO, "KVS state must be initialized\n");
    return;
  }

  pthread_rwlock_rdlock(&kvs_table->tablelock);
  char aux[MAX_STRING_SIZE];

  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i]; // Get the next list head
    while (keyNode != NULL) {
      snprintf(aux, MAX_STRING_SIZE, "(%s, %s)\n", keyNode->key,
               keyNode->value);
      write_str(fd, aux);
      keyNode = keyNode->next; // Move to the next node of the list
    }
  }

  pthread_rwlock_unlock(&kvs_table->tablelock);
}

int kvs_backup(size_t num_backup, char *job_filename, char *directory) {
  pid_t pid;
  char bck_name[50];
  snprintf(bck_name, sizeof(bck_name), "%s/%s-%ld.bck", directory,
           strtok(job_filename, "."), num_backup);

  pthread_rwlock_rdlock(&kvs_table->tablelock);
  pid = fork();
  pthread_rwlock_unlock(&kvs_table->tablelock);
  if (pid == 0) {
    // functions used here have to be async signal safe, since this
    // fork happens in a multi thread context (see man fork)
    int fd = open(bck_name, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    for (int i = 0; i < TABLE_SIZE; i++) {
      KeyNode *keyNode = kvs_table->table[i]; // Get the next list head
      while (keyNode != NULL) {
        char aux[MAX_STRING_SIZE];
        aux[0] = '(';
        size_t num_bytes_copied = 1; // the "("
        // the - 1 are all to leave space for the '/0'
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied, keyNode->key,
                                        MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied, ", ",
                                        MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied, keyNode->value,
                                        MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied, ")\n",
                                        MAX_STRING_SIZE - num_bytes_copied - 1);
        aux[num_bytes_copied] = '\0';
        write_str(fd, aux);
        keyNode = keyNode->next; // Move to the next node of the list
      }
    }
    exit(1);
  } else if (pid < 0) {
    return -1;
  }
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}

void mudarSinalSeguranca(){
  if(sinalSegurancaLancado){
    sinalSegurancaLancado = 0; //mete como false
  }else{
    sinalSegurancaLancado = 1; //mete como true
  }
  return;
}

int getSinalSeguranca(){
  return sinalSegurancaLancado;
}


int addSubscriber(Cliente *cliente, char *key){
  if(!getSinalSeguranca()){
    if (kvs_table == NULL) {
      write_str(STDERR_FILENO, "KVS state must be initialized\n");
      return 1;
    }
    pthread_rwlock_rdlock(&kvs_table->tablelock);
    if(addSubscription(kvs_table,cliente, key)!=0){
      printf("funcao addSubscription deu errado\n");
      pthread_rwlock_unlock(&kvs_table->tablelock);
      return 1;
    }
    printf("funcao addSubscription deu certo\n");
    pthread_rwlock_unlock(&kvs_table->tablelock);
    return 0;
  }
  return 1;
}

int removeSubscriber(Cliente *cliente, char *key){
  if(!getSinalSeguranca()){
    if (kvs_table == NULL) {
      write_str(STDERR_FILENO, "KVS state must be initialized\n");
      return 1;
    }
    pthread_rwlock_rdlock(&kvs_table->tablelock);
    if(removeSubscription(cliente, key)!=0){
      pthread_rwlock_unlock(&kvs_table->tablelock);
      return 1;
    }
    pthread_rwlock_unlock(&kvs_table->tablelock);
    return 0;
  }
  return 1;
}

int disconnectClient(Cliente *cliente){
  if(!getSinalSeguranca()){
    printf("esta no disconnectClient \n");
    Subscriptions *subscricao_atual = cliente->head_subscricoes;
    if(cliente->head_subscricoes == NULL){
      printf("head é null\n");
    }else{
      printf("head nao é null\n");
    }
    if(cliente->head_subscricoes->next == NULL){
      printf("head next é null");
    }else{
      printf("head next nao é null\n");
    }
    //remover todas as suas subscricoes 
    while (subscricao_atual!=NULL){
      printf("ta inscrito nalgo \n");
      KeyNode *par = subscricao_atual->par;
      char *key = par->key;
      if(removeSubscription(cliente, key)==1){
        printf("deu erro no removeSubscription \n");
        return 1;
      }
      printf("removeu subscricao no cliente \n");
      subscricao_atual = subscricao_atual->next;
    }
    return 0;
  }
  return 1;
}