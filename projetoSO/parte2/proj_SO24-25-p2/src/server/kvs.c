#include "kvs.h"

#include <ctype.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>

#include "string.h"
#include "src/common/io.h"

// Hash function based on key initial.
// @param key Lowercase alphabetical string.
// @return hash.
// NOTE: This is not an ideal hash function, but is useful for test purposes of
// the project
int hash(const char *key) {
  int firstLetter = tolower(key[0]);
  if (firstLetter >= 'a' && firstLetter <= 'z') {
    return firstLetter - 'a';
  } else if (firstLetter >= '0' && firstLetter <= '9') {
    return firstLetter - '0';
  }
  return -1; // Invalid index for non-alphabetic or number strings
}

struct HashTable *create_hash_table() {
  HashTable *ht = malloc(sizeof(HashTable));
  if (!ht)
    return NULL;
  for (int i = 0; i < TABLE_SIZE; i++) {
    ht->table[i] = NULL;
  }
  pthread_rwlock_init(&ht->tablelock, NULL);
  return ht;
}

int notificarSubs(KeyNode *keyNode,const char *newValue){
  Subscribers *currentSub = keyNode->head_subscribers;
  while (currentSub != NULL) {
    Cliente *cliente = currentSub->subscriber;
    int pipe_notif = open(cliente->notif_pipe_path, O_WRONLY); //abre o pipe das notificacoes para escrita
    if (pipe_notif == -1) {
        return 1;
    }
    char mensagem[256];
    snprintf(mensagem, sizeof(mensagem), "(%s,%s)", keyNode->key, newValue);
    if(write_all(pipe_notif, mensagem, 256)!=1){
      //erro
      return 1;
    }
    currentSub = currentSub->next; //próximo subscritor
  }
  return 0;
}

int write_pair(HashTable *ht, const char *key, const char *value) {
  int index = hash(key);

  // Search for the key node
  KeyNode *keyNode = ht->table[index];
  KeyNode *previousNode;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      // overwrite value
      free(keyNode->value);
      keyNode->value = strdup(value);
      return notificarSubs(keyNode, value);
    }
    previousNode = keyNode;
    keyNode = previousNode->next; // Move to the next node
  }
  // Key not found, create a new key node
  keyNode = malloc(sizeof(KeyNode));
  keyNode->key = strdup(key);       // Allocate memory for the key
  keyNode->value = strdup(value);   // Allocate memory for the value
  keyNode->next = ht->table[index]; // Link to existing nodes
  ht->table[index] = keyNode; // Place new key node at the start of the list
  return 0;
}

char *read_pair(HashTable *ht, const char *key) {
  int index = hash(key);

  KeyNode *keyNode = ht->table[index];
  KeyNode *previousNode;
  char *value;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      value = strdup(keyNode->value);
      return value; // Return the value if found
    }
    previousNode = keyNode;
    keyNode = previousNode->next; // Move to the next node
  }

  return NULL; // Key not found
}

int delete_pair(HashTable *ht, const char *key) {
  int index = hash(key);

  // Search for the key node
  KeyNode *keyNode = ht->table[index];
  KeyNode *prevNode = NULL;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      // Key found; delete this node
      if (prevNode == NULL) {
        // Node to delete is the first node in the list
        ht->table[index] =
            keyNode->next; // Update the table to point to the next node
      } else {
        // Node to delete is not the first; bypass it
        prevNode->next =
            keyNode->next; // Link the previous node to the next node
      }
      notificarSubs(keyNode, "DELETED");

      // Free the memory allocated for the key and value
      free(keyNode->key);
      free(keyNode->value);
      free(keyNode); // Free the key node itself
      return 0;      // Exit the function
    }
    prevNode = keyNode;      // Move prevNode to current node
    keyNode = keyNode->next; // Move to the next node
  }

  return 1;
}

void free_table(HashTable *ht) {
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = ht->table[i];
    while (keyNode != NULL) {
      KeyNode *temp = keyNode;
      keyNode = keyNode->next;
      free(temp->key);
      free(temp->value);
      free(temp->head_subscribers);
      free(temp);
    }
  }
  pthread_rwlock_destroy(&ht->tablelock);
  free(ht);
}

//1 se errado, 0 se certo
int addSubscription(HashTable *ht, Cliente* cliente, char *key){
  int index = hash(key);
  Subscribers *newSub = (Subscribers *)malloc(sizeof(Subscribers));
  if (!newSub) {
    return 1; // Retorno de erro ao alocar memória
  }
  newSub->subscriber = cliente;
  newSub->next = NULL;

  KeyNode *keyNode = ht->table[index];
  KeyNode *previousNode;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      //encontramos a chave correta
    
      newSub->next = keyNode->head_subscribers;
      keyNode->head_subscribers = newSub;
      Subscriptions *newSubscription = (Subscriptions *)malloc(sizeof(Subscriptions));
      newSubscription->next = cliente->head_subscricoes;
      cliente->head_subscricoes = newSubscription;
      return 0;
    }
    previousNode = keyNode;
    keyNode = previousNode->next; // Move to the next node
  }
  free(newSub);
  return 1;  
}

//remove subscricao da estrutura cliente
//0 se certo, 1 se errado
int removeSubscription(Cliente* cliente, char *key){
  Subscriptions *subscricao_atual = cliente->head_subscricoes;
  Subscriptions *subscricao_prev = NULL;

  //percorre a lista das subscricoes até encontrar a que queremos
  while(subscricao_atual!=NULL){
    //verifica se é a que queremos
    KeyNode *par_atual = subscricao_atual->par;
    if(strcmp(par_atual->key,key)==0){
      //encontramos a que queremos
      if (removeSubscriberTable(par_atual, cliente)==0){
        //retira a ligacao
        Subscriptions *subscricao_prox = subscricao_atual->next;

        if(subscricao_prev!=NULL){
          subscricao_prev->next = subscricao_prox;
        }else{
          cliente->head_subscricoes=subscricao_prox;
        }
        free(subscricao_atual);
        return 0;
      }
      return 1;
    }else{
      //ainda nao encontrou
      subscricao_prev=subscricao_atual;
      subscricao_atual=subscricao_atual->next;
    }
    
  }
  return 1;
}

//remove cliente dos followers na estrutura da chave 
//0 se certo, 1 se errado
int removeSubscriberTable(KeyNode *par, Cliente *cliente_desejado){
  Subscribers *subscriber_atual = par->head_subscribers;
  Subscribers *subscriber_prev = NULL;

  //percorre a lista de todos os followers do par
  while(subscriber_atual!=NULL){
    Cliente *cliente_atual = subscriber_atual->subscriber;
    //verifica se é o que queremos
    if(cliente_atual->id == cliente_desejado->id){
      //é o cliente que queremos
      Subscribers *subscriber_prox = subscriber_atual->next;

      if(subscriber_prev!=NULL){
        //nao é o primeiro da lista
        subscriber_prev->next = subscriber_prox;
      }else{
        //é o primeiro da lista
        par->head_subscribers = subscriber_prox;
      }
      free(subscriber_atual);
      return 0;
    }else{
      //ainda nao encontrou
      subscriber_prev = subscriber_atual;
      subscriber_atual = subscriber_atual->next;
    }
  }
  //nao encontrou 
  return 1;
}
