/*

FILE: operations.c

Descrição:
Este file contém as implementações das funções relacionadas à manipulação do KVS (Key-Value Store).
O file fornece as funções essenciais para inicializar, terminar, escrever, ler, deletar e exibir dados no KVS.
As funções implementadas aqui interagem com a tabela de hash onde os pares chave-valor são armazenados e recuperados.

Funções:
- 'kvs_init': Inicializa o KVS criando uma tabela de hash para armazenar os pares chave-valor.
- 'kvs_terminate': Finaliza o KVS, liberando a memória utilizada pela tabela de hash.
- 'kvs_write': Escreve ou atualiza pares chave-valor no KVS.
- 'kvs_read': Lê valores associados às chaves fornecidas e imprime os resultados.
- 'kvs_delete': Deleta pares chave-valor do KVS.
- 'kvs_show': Exibe todos os pares chave-valor armazenados no KVS.
- 'kvs_backup': Função placeholder para o comando de backup (não implementado).
- 'kvs_wait': Realiza uma espera por um tempo específico, em milissegundos.
- 'kvs_clear': Limpa a tabela de hash do KVS e recria uma nova tabela vazia.

Funcionalidade:
O objetivo principal deste file é fornecer a funcionalidade central do KVS, permitindo que o sistema armazene e
recupere pares chave-valor, além de manipular o armazenamento (leitura, escrita, deleção e exibição).

O código também lida com a inicialização e finalização adequada da memória, garantindo que a tabela de hash seja criada,
limpa e liberada corretamente.

Autores:
- Davi Rocha
- Duarte Cruz

Data de Finalização:
- 08/12/2024 - Parte 1

*/



#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

#include "kvs.h"
#include "constants.h"

pthread_mutex_t kvs_mutex = PTHREAD_MUTEX_INITIALIZER;

// A tabela de hash onde as chaves e valores são armazenados
static struct HashTable* kvs_table = NULL;

/// Converte o tempo de delay em milissegundos para um formato de timespec.
/// @param delay_ms O tempo de atraso em milissegundos.
/// @return Uma estrutura timespec representando o atraso.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

/// Inicializa o estado do KVS (Key-Value Store).
/// A função cria uma nova tabela de hash para armazenar os pares chave-valor.
/// @return 0 se o KVS foi inicializado com sucesso, 1 caso contrário (se já foi inicializado).
int kvs_init() {
  // Bloquear o mutex para garantir que apenas uma thread acesse o KVS por vez
  pthread_mutex_lock(&kvs_mutex);
  if (kvs_table != NULL) {
    // Se o KVS já foi inicializado, retorna um erro
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();  // Cria a tabela de hash
  // Desbloquear o mutex após a operação
  pthread_mutex_unlock(&kvs_mutex);
  return kvs_table == NULL;  // Retorna 1 se a criação da tabela falhou
}

/// Finaliza o estado do KVS.
/// Libera os recursos alocados para a tabela de hash.
/// @return 0 se o KVS foi finalizado com sucesso, 1 se o KVS não foi inicializado.
int kvs_terminate() {
  // Bloquear o mutex para garantir que apenas uma thread acesse o KVS por vez
  pthread_mutex_lock(&kvs_mutex);
  if (kvs_table == NULL) {
    // Se o KVS não foi inicializado, retorna erro
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);  // Libera a tabela de hash
  // Desbloquear o mutex após a operação
  pthread_mutex_unlock(&kvs_mutex);
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
  // Bloquear o mutex para garantir que apenas uma thread acesse o KVS por vez
  pthread_mutex_lock(&kvs_mutex);
  if (kvs_table == NULL) {
    // Se o KVS não foi inicializado, retorna erro
    dprintf(fd_out, "KVS state must be initialized\n");
    pthread_mutex_unlock(&kvs_mutex);
    return 1;
  }
  for (size_t i = 0; i < num_pairs; i++) {
    // Tenta escrever cada par chave-valor na tabela
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      dprintf(fd_out, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
      pthread_mutex_unlock(&kvs_mutex);
      return 1;
    }
  }
  // Desbloquear o mutex após a operação
  pthread_mutex_unlock(&kvs_mutex);
  return 0;
}

///ordena uma lista de letras por ordem alfabetica. util para o Read
/// @param list lista de caracteres
/// @param size tamanho da lista
/// @return vazio
void order_list(char list[][MAX_STRING_SIZE], size_t size){
  for (size_t i = 0; i < size - 1; i++) {
    for (size_t j = 0; j < size - i - 1; j++) {
      if (strcmp(list[j], list[j + 1]) > 0) {
        // Troca as strings
        char temp[MAX_STRING_SIZE];
        strcpy(temp, list[j]);
        strcpy(list[j], list[j + 1]);
        strcpy(list[j + 1], temp);
      }
    }
  }
}


/// Lê os valores associados a um conjunto de chaves no KVS.
/// @param fd_out O descritor do file para onde a saída será escrita.
/// @param num_pairs O número de chaves a serem lidas.
/// @param keys O array de chaves.
/// @return 0 se as chaves foram lidas com sucesso, 1 caso contrário.
int kvs_read(int fd_out, size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
  // Bloquear o mutex para garantir que apenas uma thread acesse o KVS por vez
  pthread_mutex_lock(&kvs_mutex);
  if (kvs_table == NULL) {
    // Se o KVS não foi inicializado, retorna erro
    return 1;
  }

  order_list(keys,num_pairs);
  dprintf(fd_out, "[");  // Inicia a impressão da lista de resultados
  for (size_t i = 0; i < num_pairs; i++) {
    // Lê o valor associado à chave
    char* result = read_pair(kvs_table, keys[i]);
    // Imprime o par chave-valor ou "KVSERROR" se a chave não for encontrada
    dprintf(fd_out, "(%s,%s)", keys[i], result ? result : "KVSERROR");
    free(result);  // Libera a memória alocada para o valor
  }
  dprintf(fd_out, "]\n");
  // Desbloquear o mutex após a operação
  pthread_mutex_unlock(&kvs_mutex);

  return 0;
}

/// Deleta pares chave-valor do KVS.
/// Se a chave não for encontrada, imprime "KVSMISSING".
/// @param fd_out O descritor do file para onde a saída será escrita.
/// @param num_pairs O número de chaves a serem deletadas.
/// @param keys O array de chaves.
/// @return 0 se a operação de deleção foi bem-sucedida, 1 caso contrário.
int kvs_delete(int fd_out, size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
  // Bloquear o mutex para garantir que apenas uma thread acesse o KVS por vez
  pthread_mutex_lock(&kvs_mutex);
  if (kvs_table == NULL) {
    // Se o KVS não foi inicializado, retorna erro
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  int aux = 0;
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
  // Desbloquear o mutex após a operação
  pthread_mutex_unlock(&kvs_mutex);
  return 0;
}

/// Exibe todo o conteúdo do KVS.
/// Imprime todos os pares chave-valor armazenados no KVS.
/// @param fd_out O descritor de file para onde a saída será escrita.
void kvs_show(int fd_out) {
  // Bloquear o mutex para garantir que apenas uma thread acesse o KVS por vez
  pthread_mutex_lock(&kvs_mutex);
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      // Imprime cada par chave-valor
      dprintf(fd_out, "(%s, %s)\n", keyNode->key, keyNode->value);
      keyNode = keyNode->next;  // Move para o próximo nó
    }
  }
  // Desbloquear o mutex após a operação
  pthread_mutex_unlock(&kvs_mutex);

}

/// Faz um backup do KVS para o ficheiro
void kvs_backup(int fd_out) {
  kvs_show(fd_out);
}

/// Aguarda o tempo especificado em milissegundos.
/// @param delay_ms O tempo de atraso em milissegundos.
void kvs_wait(unsigned int delay_ms) {
  // Bloquear o mutex para garantir que apenas uma thread acesse o KVS por vez
  pthread_mutex_lock(&kvs_mutex);
  struct timespec delay = delay_to_timespec(delay_ms);  // Converte o tempo em milissegundos para timespec
  nanosleep(&delay, NULL);  // Faz a espera pela quantidade de tempo especificada
  // Desbloquear o mutex após a operação
  pthread_mutex_unlock(&kvs_mutex);

}

/// Limpa a tabela de hash do KVS, liberando a memória e recriando a tabela vazia.
/// Essa função é chamada para reinicializar o KVS entre os files de entrada.
/// Isso permite limpar todos os pares chave-valor antes de processar um novo file.
void kvs_clear() {
  // Bloquear o mutex para garantir que apenas uma thread acesse o KVS por vez
  pthread_mutex_lock(&kvs_mutex);
  if (kvs_table != NULL) {
    free_table(kvs_table);  // Libera a memória associada à tabela existente
    kvs_table = create_hash_table();  // Recria a tabela de hash vazia
  }
  // Desbloquear o mutex após a operação
  pthread_mutex_unlock(&kvs_mutex);

}
