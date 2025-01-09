#include "kvs.h"

#include <ctype.h>
#include <stdlib.h>

#include "string.h"

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

void notificarSubs(KeyNode *keyNode, char *newValue){
  Subscribers *currentSub = keyNode->subscribers;
  while (currentSub != NULL) {
    Cliente *cliente = currentSub->cliente;
    int pipe_notif = open(cliente->notif_pipe_path, O_WRONLY); //abre o pipe das notificacoes para escrita
    if (pipe_notif == -1) {
        perror("Erro ao abrir o pipe");
        return;
    }
    char mensagem[256];
    snprintf(mensagem, sizeof(mensagem), "(%s,%s)", keyNode->key, newValue);
    write(pipe_notif, mensagem, strlen(mensagem));
    currentSub = currentSub->next; //próximo subscritor
  }
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
      notificarSubs(keyNode, value);
      return 0;
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
      free(temp->subscribers);
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
  newSub->cliente = cliente;
  newSub->next = NULL;

  KeyNode *keyNode = ht->table[index];
  KeyNode *previousNode;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      newSub->next = keyNode->subscribers;
      keyNode->subscribers = newSub;
      Subscriptions *newSubscription = (Subscriptions *)malloc(sizeof(Subscriptions));
      newSubscription->next = cliente->subscricoes;
      cliente->subscricoes = newSubscription;
      return 0;
    }
    previousNode = keyNode;
    keyNode = previousNode->next; // Move to the next node
  }
  free(newSub);
  return 1;  
}

//1 se errado, 0 se certo
int removeSubscription(HashTable *ht, Cliente* cliente, char *key){
  int index = hash(key);
  KeyNode *keyNode = ht->table[index];
  KeyNode *previousNode;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      Subscribers *currentSub = keyNode->subscribers;
      Subscribers *previousSub = NULL;
      while (currentSub != NULL) {
        if (strcmp(currentSub->cliente->resp_pipe_path, cliente->resp_pipe_path) == 0 &&
          strcmp(currentSub->cliente->notif_pipe_path, cliente->notif_pipe_path) == 0 &&
          strcmp(currentSub->cliente->req_pipe_path, cliente->req_pipe_path) == 0) { {
            //estamos no cliente certo

            //remover da hashtable
            if (previousSub == NULL) {
              keyNode->subscribers = currentSub->next; //remove caso seja o primeiro elemento da lista
            } else {
              previousSub->next = currentSub->next; //remove para os outros casos
            }
            free(currentSub); //liberta a memoria

            //remover do cliente
            Subscriptions *currentSubscr = cliente->subscricoes;
            Subscriptions *prevSubscr = NULL;
            while (currentSubscr != NULL) {
              if (currentSubscr->key == keyNode) {
                if (prevSubscr == NULL) {
                  cliente->subscricoes = currentSubscr->next; // Remove o primeiro elemento
                } else {
                  prevSubscr->next = currentSubscr->next; // Remove o elemento do meio/final
                }
                free(currentSubscr); // Libera a memória da associação removida
                break;
              }
              prevSubscr = currentSubscr;
              currentSubscr = currentSubscr->next;
            }
            return 0; // Sucesso na remoção
        }
        previousSub = currentSub;
        currentSub = currentSub->next;
      }
    }
    previousNode = keyNode;
    keyNode = keyNode->next;
    }
  }
  return 1;  
}