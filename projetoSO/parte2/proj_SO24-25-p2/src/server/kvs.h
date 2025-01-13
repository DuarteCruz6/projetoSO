#ifndef KVS_H
#define KVS_H

#define TABLE_SIZE 26

#include <pthread.h>
#include <stddef.h>
#include <stdbool.h>

typedef struct Subscriptions{
  struct KeyNode *par;
  struct Subscriptions *next;
}Subscriptions;

typedef struct Cliente {
  int id;
  char resp_pipe_path[40];
  char notif_pipe_path[40];
  char req_pipe_path[40];
  struct Subscriptions *head_subscricoes;
  int num_subscricoes;
  int resp_pipe;
  int req_pipe;
  int notif_pipe;
  int flag_sigusr1;
  int usado;
}Cliente;

typedef struct Subscribers {
  Cliente *subscriber;
  struct Subscribers *next; // Apontador para o próximo cliente na lista
} Subscribers;


typedef struct KeyNode {
  char *key;
  char *value;
  Subscribers *head_subscribers; //lista ligada de clientes subscritos a esta chave
  struct KeyNode *next;
} KeyNode;

typedef struct HashTable {
  KeyNode *table[TABLE_SIZE];
  pthread_rwlock_t tablelock;
} HashTable;

/// Creates a new KVS hash table.
/// @return Newly created hash table, NULL on failure
struct HashTable *create_hash_table();

int hash(const char *key);

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

/// Deletes a pair from the table.
/// @param ht Hash table to read from.
/// @param key Key of the pair to be deleted.
/// @return 0 if the node was deleted successfully, 1 otherwise.
int delete_pair(HashTable *ht, const char *key);

/// Frees the hashtable.
/// @param ht Hash table to be deleted.
void free_table(HashTable *ht);

KeyNode *getKeyNode(HashTable *ht,char *key);

bool alreadySubbed(KeyNode *par, Cliente *cliente);

void getAllSubsKey(KeyNode *par);

int addSubscription(HashTable *ht,Cliente* cliente, char *key);

int addSubscriberTable(Cliente *cliente, KeyNode *par);

int removeSubscription(Cliente *cliente, char *key);

int removeSubscriberTable(KeyNode *par, Cliente *cliente_desejado);


#endif // KVS_H
