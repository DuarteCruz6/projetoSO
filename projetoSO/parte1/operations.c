#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

#include "kvs.h"
#include "constants.h"

// A tabela de hash onde as chaves e valores são armazenados
static struct HashTable* kvs_table = NULL;

/// Converte o tempo de delay em milissegundos para um formato de timespec.
/// @param delay_ms O tempo de atraso em milissegundos.
/// @return Uma estrutura timespec representando o atraso.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

///ordena a lista de chaves e valores. util para o write.
///caso values seja null, entao apenas ordenas as chaves. util para o read e delete
/// @param keys lista de chaves da hashtable
/// @param values lista de valores da hashtable
/// @param size tamanho da lista
/// @return vazio
void order_keys_values(char keys[][MAX_STRING_SIZE],char values[][MAX_STRING_SIZE],size_t size){
  for (size_t i = 0; i < size - 1; i++) {
    for (size_t j = 0; j < size - i - 1; j++) {
      if (strcmp(keys[j], keys[j + 1]) > 0) {
        // Troca as strings
        char temp[MAX_STRING_SIZE];
        strcpy(temp, keys[j]);
        strcpy(keys[j], keys[j+1]);
        strcpy(keys[j + 1], temp);

        if(values){
          char temp_value[MAX_STRING_SIZE];
          strcpy(temp_value, values[j]);
          strcpy(values[j], values[j+1]);
          strcpy(values[j+1], temp_value);
        }
      }
    }
  }
}

/// Inicializa o estado do KVS (Key-Value Store).
/// A função cria uma nova tabela de hash para armazenar os pares chave-valor.
/// @return 0 se o KVS foi inicializado com sucesso, 1 caso contrário (se já foi inicializado).
int kvs_init() {
  if (kvs_table != NULL) {
    // Se o KVS já foi inicializado, retorna um erro
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();  // Cria a tabela de hash
  return kvs_table == NULL;  // Retorna 1 se a criação da tabela falhou
}

/// Finaliza o estado do KVS.
/// Libera os recursos alocados para a tabela de hash.
/// @return 0 se o KVS foi finalizado com sucesso, 1 se o KVS não foi inicializado.
int kvs_terminate() {
  if (kvs_table == NULL) {
    // Se o KVS não foi inicializado, retorna erro
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);  // Libera a tabela de hash
  return 0;
}

/// Escreve pares chave-valor no KVS.
/// Se a chave já existe, seu valor é atualizado.
/// @param fd_out O descritor do file para onde a saída será escrita.
/// @param num_pairs O número de pares chave-valor a serem escritos.
/// @param keys O array de chaves.
/// @param values O array de valores.
/// @return 0 se os pares foram escritos com sucesso, 1 caso contrário.
int kvs_write(int fd_out, size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  
  if (kvs_table == NULL) {
    // Se o KVS não foi inicializado, retorna erro
    dprintf(fd_out, "KVS state must be initialized\n");
    return 1;
  }
  order_keys_values(keys,values,num_pairs);
  for (size_t i = 0; i < num_pairs; i++) {
    // Tenta escrever cada par chave-valor na tabela
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      dprintf(fd_out, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
      return 1;
    }
  }
  return 0;
}

/// Lê os valores associados a um conjunto de chaves no KVS.
/// @param fd_out O descritor do file para onde a saída será escrita.
/// @param num_pairs O número de chaves a serem lidas.
/// @param keys O array de chaves.
/// @return 0 se as chaves foram lidas com sucesso, 1 caso contrário.
int kvs_read(int fd_out, size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    // Se o KVS não foi inicializado, retorna erro
    return 1;
  }
  order_keys_values(keys,NULL,num_pairs);
  dprintf(fd_out, "[");  // Inicia a impressão da lista de resultados
  for (size_t i = 0; i < num_pairs; i++) {
    // Lê o valor associado à chave
    char* result = read_pair(kvs_table, keys[i]);
    // Imprime o par chave-valor ou "KVSERROR" se a chave não for encontrada
    dprintf(fd_out, "(%s,%s)", keys[i], result ? result : "KVSERROR");
    free(result);  // Libera a memória alocada para o valor
  }
  dprintf(fd_out, "]\n");

  return 0;
}

/// Deleta pares chave-valor do KVS.
/// Se a chave não for encontrada, imprime "KVSMISSING".
/// @param fd_out O descritor do file para onde a saída será escrita.
/// @param num_pairs O número de chaves a serem deletadas.
/// @param keys O array de chaves.
/// @return 0 se a operação de deleção foi bem-sucedida, 1 caso contrário.
int kvs_delete(int fd_out, size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    // Se o KVS não foi inicializado, retorna erro
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int aux = 0;
  order_keys_values(keys,NULL,num_pairs);
  for (size_t i = 0; i < num_pairs; i++) {
    // Tenta deletar cada chave da tabela
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        dprintf(fd_out, "[");
        aux = 1;
      }
      // Se a chave não for encontrada, imprime "(chave, KVSMISSING)"
      dprintf(fd_out, "(%s,KVSMISSING)", keys[i]);
    }
  }

  if (aux) {
    dprintf(fd_out, "]\n");
  }
  return 0;
}

/// Exibe todo o conteúdo do KVS.
/// Imprime todos os pares chave-valor armazenados no KVS.
/// @param fd_out O descritor de file para onde a saída será escrita.
void kvs_show(int fd_out) {
  for (int i = 0; i < TABLE_SIZE; i++) {
    pthread_rwlock_wrlock(&kvs_table->mutex_index[i]);
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      // Imprime cada par chave-valor
      pthread_rwlock_rdlock(keyNode->mutex_par_hashTable);  //da lock do tipo read a este par da hash table pois nao queremos
                                                            //que o seu valor seja alterado enquanto lemos, mas pode ser lido
                                                            //por outras threads
      if(keyNode->value!=NULL){
        dprintf(fd_out, "(%s, %s)\n", keyNode->key, keyNode->value);
      }
      pthread_rwlock_unlock(keyNode->mutex_par_hashTable);  //damos unlock
      keyNode = keyNode->next;  // Move para o próximo nó
    }
    pthread_rwlock_unlock(&kvs_table->mutex_index[i]);
  }
  //pthread_rwlock_unlock(kvs_table->mutex_hashTable); 
}

/// Faz um backup do KVS para o ficheiro
void kvs_backup(int fd_out) {
  kvs_show(fd_out);
}

/// Aguarda o tempo especificado em milissegundos.
/// @param delay_ms O tempo de atraso em milissegundos.
void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);  // Converte o tempo em milissegundos para timespec
  nanosleep(&delay, NULL);  // Faz a espera pela quantidade de tempo especificada

}

/// Limpa a tabela de hash do KVS, liberando a memória e recriando a tabela vazia.
/// Essa função é chamada para reinicializar o KVS entre os files de entrada.
/// Isso permite limpar todos os pares chave-valor antes de processar um novo file.
void kvs_clear() {
  if (kvs_table != NULL) {
    free_table(kvs_table);  // Libera a memória associada à tabela existente
    kvs_table = create_hash_table();  // Recria a tabela de hash vazia
  }

}