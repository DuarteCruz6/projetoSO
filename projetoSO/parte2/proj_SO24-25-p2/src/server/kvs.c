#include "kvs.h"

#include <ctype.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <stdbool.h>

#include "string.h"
#include "src/common/io.h"
#include "src/common/constants.h"

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

//notifica todos os subs do par para informar que houve alteracao
int notificarSubs(KeyNode *keyNode,const char *newValue){
  Subscribers *currentSub = keyNode->head_subscribers;
  while (currentSub != NULL) {
    Cliente *cliente = currentSub->subscriber;
    char mensagem[83];
    pad_string(&mensagem[0], keyNode->key, 41);
    pad_string(&mensagem[41], newValue, 41);
    mensagem[82] = '\0';

    printf("notificacao: _%s_\n",mensagem);
    if(cliente->notif_pipe==NULL){
      cliente->notif_pipe = open(cliente->notif_pipe_path, O_WRONLY); //abre o pipe das notificacoes para escrita
    }
    if(write_all(cliente->notif_pipe, mensagem, 82)!=1){
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
  keyNode->head_subscribers = NULL;
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
      notificarSubs(keyNode, "DELETED");
      Subscribers *currentSub = keyNode->head_subscribers;
      while (currentSub->subscriber != NULL) {
        Cliente *cliente = currentSub->subscriber;
        removeSubscription(cliente,key);
        printf("xau\n");
        currentSub = currentSub->next; //próximo subscritor
        printf("oi again\n");
      }
      // Key found; delete this node
      printf("1\n");
      if (prevNode == NULL) {
        // Node to delete is the first node in the list
        printf("2\n");
        ht->table[index] =
            keyNode->next; // Update the table to point to the next node
            printf("3\n");
      } else {
        printf("4\n");
        // Node to delete is not the first; bypass it
        prevNode->next =
            keyNode->next; // Link the previous node to the next node
            printf("5\n");
      }

      // Free the memory allocated for the key and value
      printf("6\n");
      free(keyNode->key);
      printf("7\n");
      free(keyNode->value);
      printf("8\n");
      free(keyNode); // Free the key node itself
      printf("9\n");
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

KeyNode *getKeyNode(HashTable *ht,char *key){
  int index = hash(key);

  KeyNode *keyNode = ht->table[index];
  KeyNode *previousNode;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      return keyNode; // Return the keynode if found
    }
    previousNode = keyNode;
    keyNode = previousNode->next; // Move to the next node
  }

  return NULL; // Key not found
}

//verifica se algum cliente ja esta subscrito ao par, para nao haver repetidos
bool alreadySubbed(KeyNode *par, Cliente *cliente){
  Subscribers *subAtual = par->head_subscribers;
  while (subAtual!=NULL){
    Cliente *clienteAtual = subAtual->subscriber;
    if(clienteAtual->id == cliente->id){
      //ja estava inscrito
      return true;
    }
    //nao encontramos o cliente ainda, passa para o proximo sub
    subAtual = subAtual -> next;
  }
  return false;
}

//adiciona subscricao à estrutura cliente
//0 se certo, 1 se errado
int addSubscription(HashTable *ht,Cliente *cliente, char *key){
  if(cliente->num_subscricoes>=MAX_NUMBER_SUB){
    return 1;
  }
  Subscriptions *subsCliente = cliente->head_subscricoes;
  Subscriptions *newSub = malloc(sizeof(Subscriptions));
  printf("key _%s_\n",key);
  KeyNode *par = getKeyNode(ht,key);
  if(newSub!=NULL && par!=NULL){
    printf("new Sub e par !=NULL\n");
    if(addSubscriberTable(cliente, par)==0){
      printf("funcao addSubscriberTable deu certo\n");
      newSub->next = subsCliente; //mete a nova Sub no inicio da lista
      if(newSub->next==NULL){
        printf("é null\n");
      }else{
        printf("nao é null\n");
      }
      newSub->par = par; //guarda o keynode na sub
      cliente->head_subscricoes = newSub; //guarda a novaSub como cabeca da lista
      cliente->num_subscricoes++;
      return 0;
    }
    printf("funcao addSubscriberTable deu errado\n");
    return 1;
  }
  printf("new Sub ou par ==NULL\n");
  return 1;
}

//adiciona subscritor à estrutura keynode
//0 se certo, 1 se errado
int addSubscriberTable(Cliente *cliente, KeyNode *par){
  printf("a adicionar cliente com id %d aos subs do par %s \n",cliente->id, par->key);
  Subscribers *subsPar = par->head_subscribers;
  Subscribers *newSub = malloc(sizeof(Subscribers));
  if(newSub!=NULL){
    if(!alreadySubbed(par, cliente)){
      newSub->subscriber = cliente; //guarda o novo sub
      newSub->next = subsPar; //mete o novo sub no inicio da lista e faz o link
      par->head_subscribers = newSub; //guarda o novo Sub como cabeca da lista
      return 0;
    }
  }
  return 1;
}


//remove subscricao da estrutura cliente
//0 se certo, 1 se errado
int removeSubscription(Cliente *cliente, char *key){
  printf("esta no removeSubscription \n");
  Subscriptions *subscricao_atual = cliente->head_subscricoes;
  Subscriptions *subscricao_prev = NULL;

  //percorre a lista das subscricoes até encontrar a que queremos
  while(subscricao_atual!=NULL){
    printf("esta no while do removeSubscription \n");
    //verifica se é a que queremos
    KeyNode *par_atual = subscricao_atual->par;
    if(strcmp(par_atual->key,key)==0){
      printf("encontramos a chave que queremos \n");
      //encontramos a que queremos
      if (removeSubscriberTable(par_atual, cliente)==0){
        //retira a ligacao
        printf("o removeSubscriberTable deu certo \n");
        Subscriptions *subscricao_prox = subscricao_atual->next;

        if(subscricao_prev!=NULL){
          subscricao_prev->next = subscricao_prox;
        }else{
          cliente->head_subscricoes=subscricao_prox;
        }
        free(subscricao_atual);
        cliente->num_subscricoes--;
        return 0;
      }
      printf("o removeSubscriberTable deu erro \n");
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
  printf("estamos no removeSubscriberTable \n");
  Subscribers *subscriber_atual = par->head_subscribers;
  Subscribers *subscriber_prev = NULL;

  //percorre a lista de todos os followers do par
  while(subscriber_atual!=NULL){
    printf("estamos no while do removeSubscriberTable \n");
    Cliente *cliente_atual = subscriber_atual->subscriber;
    //verifica se é o que queremos
    if(cliente_atual->id == cliente_desejado->id){
      printf("encontramos o cliente que queriamos \n");
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
      printf("ainda nao encontramos o cliente que queriamos \n");
      //ainda nao encontrou
      subscriber_prev = subscriber_atual;
      subscriber_atual = subscriber_atual->next;
    }
  }
  //nao encontrou 
  return 1;
}
