#ifndef KVS_OPERATIONS_H
#define KVS_OPERATIONS_H

#include <stddef.h>

#include "constants.h"
#include "kvs.h"

/// Initializes the KVS state.
/// @return 0 if the KVS state was initialized successfully, 1 otherwise.
int kvs_init();

/// Destroys the KVS state.
/// @return 0 if the KVS state was terminated successfully, 1 otherwise.
int kvs_terminate();

/// Writes a key value pair to the KVS. If key already exists it is updated.
/// @param num_pairs Number of pairs being written.
/// @param keys Array of keys' strings.
/// @param values Array of values' strings.
/// @return 0 if the pairs were written successfully, 1 otherwise.
int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE],
              char values[][MAX_STRING_SIZE]);

/// Reads values from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @param fd File descriptor to write the (successful) output.
/// @return 0 if the key reading, 1 otherwise.
int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd);

/// Deletes key value pairs from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @return 0 if the pairs were deleted successfully, 1 otherwise.
int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd);

/// Writes the state of the KVS.
/// @param fd File descriptor to write the output.
void kvs_show(int fd);

/// Creates a backup of the KVS state and stores it in the correspondent
/// backup file
/// @return 0 if the backup was successful, 1 otherwise.
int kvs_backup(size_t num_backup, char *job_filename, char *directory);

/// Waits for the last backup to be called.
void kvs_wait_backup();

/// Waits for a given amount of time.
/// @param delay_us Delay in milliseconds.
void kvs_wait(unsigned int delay_ms);

// Setter for max_backups
// @param _max_backups
void set_max_backups(int _max_backups);

// Setter for n_current_backups
// @param _n_current_backups
void set_n_current_backups(int _n_current_backups);

// Getter for n_current_backups
// @return n_current_backups
int get_n_current_backups();

//muda o sinal de seguranca quando ha um sigusr1 
void mudarSinalSeguranca();

//retorna o sinal de seguranca
/// @return 1 se houve, 0 se nao
int getSinalSeguranca();

/// @brief adiciona um subscritor a uma chave
/// @param Cliente subscritor novo da chave
/// @param key chave À qual vai ser adicionado um novo subscritor
/// @return 0 se der certo, 1 se der errado
int addSubscriber(Cliente *Cliente, char *key);

/// @brief retira um subscritor a uma chave
/// @param Cliente subscritor que vai ser removido da chave
/// @param key chave À qual vai ser retirado um subscritor
/// @return 0 se der certo, 1 se der errado
int removeSubscriber(Cliente *Cliente, char *key);

/// @brief disconecta um cliente, apagando todas as suas subscricoes
/// @param cliente cliente que vai ser desconectado
/// @return 0 se der certo, 1 se der errado
int disconnectClient(Cliente *cliente);


#endif // KVS_OPERATIONS_H
