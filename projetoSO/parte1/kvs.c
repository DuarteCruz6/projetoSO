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
      pthread_rwlock_init(&ht->mutex_index[i], NULL);
  }
  return ht;
}

int write_pair(HashTable *ht, const char *key, const char *value) {
    int index = hash(key);
    pthread_rwlock_wrlock(&ht->mutex_index[index]);  
    KeyNode *keyNode = ht->table[index];

    // Search for the key node
    while (keyNode != NULL) {
        pthread_rwlock_rdlock(keyNode->mutex_par_hashTable); //damos lock do tipo read pois nao queremos que 
                                                             //nenhuma thread o altere mas pode ler
        if (strcmp(keyNode->key, key) == 0) {
            pthread_rwlock_unlock(keyNode->mutex_par_hashTable); //da unlock
            pthread_rwlock_wrlock(keyNode->mutex_par_hashTable); //da lock do tipo write a este par da hash table, 
                                                                 //pois vamos alterar o seu valor
            if(keyNode->value!=NULL){
                free(keyNode->value);
            }
            keyNode->value = strdup(value);
            pthread_rwlock_unlock(keyNode->mutex_par_hashTable); //da unlock
            pthread_rwlock_unlock(&ht->mutex_index[index]);
            return 0;
        }
        pthread_rwlock_unlock(keyNode->mutex_par_hashTable); //da unlock
        keyNode = keyNode->next; // Move to the next node
    }

    // Key not found, create a new key node
    pthread_rwlock_t *mutex_par_hash=malloc(sizeof(pthread_rwlock_t)); //criar um mutex do tipo read and write 
                                                                       //para este par para podermos dar lock
    pthread_rwlock_init(mutex_par_hash,NULL);   //inicializar o mutex
    keyNode = malloc(sizeof(KeyNode));
    keyNode->mutex_par_hashTable = mutex_par_hash; //guarda o mutex do par
    pthread_rwlock_wrlock(keyNode->mutex_par_hashTable);
    keyNode->key = strdup(key); // Allocate memory for the key
    keyNode->value = strdup(value); // Allocate memory for the value
    keyNode->next = ht->table[index]; // Link to existing nodes
    ht->table[index] = keyNode; // Place new key node at the start of the list
    pthread_rwlock_unlock(keyNode->mutex_par_hashTable);
    pthread_rwlock_unlock(&ht->mutex_index[index]);
    return 0;
}

char* read_pair(HashTable *ht, const char *key) {
    int index = hash(key);
    pthread_rwlock_rdlock(&ht->mutex_index[index]);
    KeyNode *keyNode = ht->table[index];
    char* value;

    while (keyNode != NULL) {
        pthread_rwlock_rdlock(keyNode->mutex_par_hashTable); //damos lock do tipo read pois 
                                                              //nao queremos que nenhuma thread o 
                                                              //altere mas pode ler
        if (strcmp(keyNode->key, key) == 0) {
            if(keyNode->value !=NULL){
                value = strdup(keyNode->value);
                pthread_rwlock_unlock(keyNode->mutex_par_hashTable);    //damos unlock
                pthread_rwlock_unlock(&ht->mutex_index[index]);
                return value; // Return copy of the value if found
            }
            pthread_rwlock_unlock(keyNode->mutex_par_hashTable);    //damos unlock
            pthread_rwlock_unlock(&ht->mutex_index[index]);
            return NULL;
        }
        pthread_rwlock_unlock(keyNode->mutex_par_hashTable); //da unlock
        keyNode = keyNode->next; // Move to the next node
    }
    pthread_rwlock_unlock(&ht->mutex_index[index]);
    return NULL; // Key not found
}

int delete_pair(HashTable *ht, const char *key) {
    int index = hash(key); 
    pthread_rwlock_wrlock(&ht->mutex_index[index]);
    KeyNode *keyNode = ht->table[index];

    // Search for the key node
    while (keyNode != NULL) {
        pthread_rwlock_wrlock(keyNode->mutex_par_hashTable); //damos lock do tipo write pois nao queremos que 
                                                             //nenhuma thread o leia/altere
        if (strcmp(keyNode->key, key) == 0) {

            if(keyNode->value==NULL){
                pthread_rwlock_unlock(keyNode->mutex_par_hashTable); //da unlock
                pthread_rwlock_unlock(&ht->mutex_index[index]);
                return 1;
            }
            // Key found; delete this node
            
            // Free the memory allocated for the key and value
            free(keyNode->value);
            keyNode->value=NULL;
            pthread_rwlock_unlock(keyNode->mutex_par_hashTable); //damos unlock
            pthread_rwlock_unlock(&ht->mutex_index[index]);
            return 0; // Exit the function
        }
        pthread_rwlock_unlock(keyNode->mutex_par_hashTable); //da unlock
        keyNode = keyNode->next; // Move to the next node
    }
    pthread_rwlock_unlock(&ht->mutex_index[index]);
    return 1;
}

void free_table(HashTable *ht) {
    //dar free a toda a hash table
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = ht->table[i];
        while (keyNode != NULL) {
            KeyNode *temp = keyNode;
            keyNode = keyNode->next;
            pthread_rwlock_destroy(temp->mutex_par_hashTable);
            free(temp->mutex_par_hashTable);
            free(temp->key);
            if(temp->value!=NULL){
                free(temp->value);
            }
            free(temp);
        }
    }
    free(ht);
}