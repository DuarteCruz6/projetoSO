#include "kvs.h"
#include "string.h"

#include <stdlib.h>
#include <pthread.h>
#include <ctype.h>

// Hash function based on key initial.
// @param key Lowercase alphabetical string.
// @return hash.
// NOTE: This is not an ideal hash function, but is useful for test purposes of the project
int hash(const char *key) {
    int firstLetter = tolower(key[0]);
    if (firstLetter >= 'a' && firstLetter <= 'z') {
        return firstLetter - 'a';
    } else if (firstLetter >= '0' && firstLetter <= '9') {
        return firstLetter - '0';
    }
    return -1; // Invalid index for non-alphabetic or number strings
}


struct HashTable* create_hash_table() {
  HashTable *ht = malloc(sizeof(HashTable));
  if (!ht) return NULL;
  for (int i = 0; i < TABLE_SIZE; i++) {
      ht->table[i] = NULL;
  }
  return ht;
}

int write_pair(HashTable *ht, const char *key, const char *value) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];

    // Search for the key node
    while (keyNode != NULL) {
        pthread_rwlock_rdlock(keyNode->mutex_par_hashTable); //damos lock do tipo read pois nao queremos que nenhuma thread o altere mas pode ler
        if (strcmp(keyNode->key, key) == 0) {
            pthread_rwlock_unlock(keyNode->mutex_par_hashTable); //da unlock
            pthread_rwlock_wrlock(keyNode->mutex_par_hashTable); //da lock do tipo write a este par da hash table, pois vamos alterar o seu valor
            free(keyNode->value);
            keyNode->value = strdup(value);
            pthread_rwlock_unlock(keyNode->mutex_par_hashTable); //da unlock
            return 0;
        }
        pthread_rwlock_unlock(keyNode->mutex_par_hashTable); //da unlock
        keyNode = keyNode->next; // Move to the next node
    }

    // Key not found, create a new key node
    pthread_rwlock_t *mutex_par_hash=malloc(sizeof(pthread_rwlock_t)); //criar um mutex do tipo read and write para este par para podermos dar lock
    pthread_rwlock_init(mutex_par_hash,NULL);   //inicializar o mutex
    keyNode = malloc(sizeof(KeyNode));
    keyNode->key = strdup(key); // Allocate memory for the key
    keyNode->value = strdup(value); // Allocate memory for the value
    keyNode->next = ht->table[index]; // Link to existing nodes
    keyNode->mutex_par_hashTable = mutex_par_hash; //guarda o mutex do par
    ht->table[index] = keyNode; // Place new key node at the start of the list
    return 0;
}

char* read_pair(HashTable *ht, const char *key) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    char* value;

    while (keyNode != NULL) {
        pthread_rwlock_rdlock(keyNode->mutex_par_hashTable); //damos lock do tipo read pois nao queremos que nenhuma thread o altere mas pode ler
        if (strcmp(keyNode->key, key) == 0) {
            value = strdup(keyNode->value);
            pthread_rwlock_unlock(keyNode->mutex_par_hashTable);    //damos unlock
            return value; // Return copy of the value if found
        }
        pthread_rwlock_unlock(keyNode->mutex_par_hashTable); //da unlock
        keyNode = keyNode->next; // Move to the next node
    }
    return NULL; // Key not found
}

int delete_pair(HashTable *ht, const char *key) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    KeyNode *prevNode = NULL;

    // Search for the key node
    while (keyNode != NULL) {
        pthread_rwlock_wrlock(keyNode->mutex_par_hashTable); //damos lock do tipo write pois nao queremos que nenhuma thread o leia/altere
        if (strcmp(keyNode->key, key) == 0) {
            // Key found; delete this node
            if (prevNode == NULL) {
                // Node to delete is the first node in the list
                ht->table[index] = keyNode->next; // Update the table to point to the next node
            } else {    
                // Node to delete is not the first; bypass it
                prevNode->next = keyNode->next; // Link the previous node to the next node
            }
            // Free the memory allocated for the key and value
            free(keyNode->key);
            free(keyNode->value);
            pthread_rwlock_unlock(keyNode->mutex_par_hashTable); //damos unlock
            free(keyNode->mutex_par_hashTable);
            free(keyNode); // Free the key node itself
            return 0; // Exit the function
        }
        prevNode = keyNode; // Move prevNode to current node
        pthread_rwlock_unlock(keyNode->mutex_par_hashTable); //da unlock
        keyNode = keyNode->next; // Move to the next node
    }
    
    return 1;
}


//int get_size(HashTable *ht){
//    return sizeof(ht);
//}

void free_table(HashTable *ht) {
    //dar free a toda a hash table
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = ht->table[i];
        while (keyNode != NULL) {
            KeyNode *temp = keyNode;
            keyNode = keyNode->next;
            free(temp->mutex_par_hashTable);
            free(temp->key);
            free(temp->value);
            free(temp);
        }
    }
    free(ht);
}