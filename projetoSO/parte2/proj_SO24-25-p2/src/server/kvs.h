#ifndef KVS_H
#define KVS_H

#define TABLE_SIZE 26

#include <pthread.h>
#include <stddef.h>
#include <stdbool.h>

//estrutura para definir uma lista ligada das subscricoes de um cliente
typedef struct Subscriptions{
  struct KeyNode *par; //par associado a subscricao
  struct Subscriptions *next; //proxima subscricao
}Subscriptions;

//estrutura para definir um cliente
typedef struct Cliente {
  int id; 
  char resp_pipe_path[40];
  char notif_pipe_path[40];
  char req_pipe_path[40];
  struct Subscriptions *head_subscricoes; //lista ligada das subscricoes do cliente
  int num_subscricoes; //numero de subscricoes do cliente
  int resp_pipe; //descritor para o response pipe
  int req_pipe; //descritor para o request pipe
  int notif_pipe; //descritor para o notification pipe
  int flag_sigusr1; //flag para saber se houve um sigusr1
  int usado; //flag para saber se uma thread ja o esta a usar
}Cliente;

//estrutura para definir uma lista ligada dos subscritores de um par
typedef struct Subscribers {
  Cliente *subscriber; //cliente associado a subscricao
  struct Subscribers *next; //proxima subscritor
} Subscribers;

//estrutura para definir um par da tabela
typedef struct KeyNode {
  char *key; //chave
  char *value; //valor
  Subscribers *head_subscribers; //lista ligada de clientes subscritos a esta chave
  struct KeyNode *next; //proximo par
} KeyNode;

//estrutura para definir a hashtable
typedef struct HashTable {
  KeyNode *table[TABLE_SIZE];
  pthread_rwlock_t tablelock;
} HashTable;

int hash(const char *key);

/// Creates a new KVS hash table.
/// @return Newly created hash table, NULL on failure
struct HashTable *create_hash_table();

/// @brief notifica todos os subs do par para informar que houve alteracao
/// @param keyNode par em que houve a alteracao
/// @param newValue novo valor do par
/// @return 1 se deu erro, 0 se deu certo
int notificarSubs(KeyNode *keyNode,const char *newValue);

// Writes a key value pair in the hash table.
// @param ht The hash table.
// @param key The key.
// @param value The value.
// @return 0 if successful.
int write_pair(HashTable *ht, const char *key, const char *value);

// Reads the value of a given key.
// @param ht The hash table.
// @param key The key.
// return the value if found, NULL otherwise.
char *read_pair(HashTable *ht, const char *key);

/// @brief apaga a subscricao de um par a todos os clientes que o tenham subscrito
/// @param par par cuja subscricao vai ser apagada de todos os seus subscritores
void deleteSub(KeyNode *par);

/// Deletes a pair from the table.
/// @param ht Hash table to read from.
/// @param key Key of the pair to be deleted.
/// @return 0 if the node was deleted successfully, 1 otherwise.
int delete_pair(HashTable *ht, const char *key);

/// Frees the hashtable.
/// @param ht Hash table to be deleted.
void free_table(HashTable *ht);

/// @brief retorna o keyNode a partir da key
/// @param ht a hashtable
/// @param key a chave do par
/// @return o par correspondente à chave
KeyNode *getKeyNode(HashTable *ht,char *key);

/// @brief verifica se o cliente ja esta subscrito ao par, para nao haver repetidos na tabela
/// @param par par sobre o qual vamos fazer a verificacao
/// @param cliente cliente que vamos verificar se ja esta inscrito
/// @return true se ja estava inscrito, false se nao estava
bool alreadySubbed(KeyNode *par, Cliente *cliente);

/// @brief adiciona subscricao à estrutura cliente
/// @param ht a hashtable
/// @param cliente o cliente ao qual vamos adicionar a subscricao
/// @param key a chave q o cliente subscreveu
/// @return 0 se deu certo, 1 se deu errado
int addSubscription(HashTable *ht,Cliente* cliente, char *key);

/// @brief adiciona subscritor à estrutura keynode
/// @param cliente cliente que é o subscritor
/// @param par keynode que vai ter o novo cliente como subscritor
/// @return 0 se deu certo, 1 se deu errado
int addSubscriberTable(Cliente *cliente, KeyNode *par);

/// @brief remove subscricao da estrutura cliente
/// @param cliente o cliente ao qual vamos remover a subscricao
/// @param key a chave q o cliente deu unsub
/// @return 0 se deu certo, 1 se deu errado
int removeSubscription(Cliente *cliente, char *key);

/// @brief remove cliente dos followers na estrutura da chave 
/// @param par keynode do qual vai ser removido o subscritor
/// @param cliente_desejado cliente que é o subscritor que vai ser removido
/// @return 0 se deu certo, 1 se deu errado
int removeSubscriberTable(KeyNode *par, Cliente *cliente_desejado);


#endif // KVS_H
