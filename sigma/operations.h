#ifndef KVS_OPERATIONS_H
#define KVS_OPERATIONS_H

#include <stddef.h>

/// Initializes the KVS state.
/// @return 0 if the KVS state was initialized successfully, 1 otherwise.
int kvs_init();

/// Destroys the KVS state.
/// @return 0 if the KVS state was terminated successfully, 1 otherwise.
int kvs_terminate();

/// Writes a key value pair to the KVS. If key already exists it is updated.
/// @param fd_out File descriptor to write the (successful) output.
/// @param num_pairs Number of pairs being written.
/// @param keys Array of keys' strings.
/// @param values Array of values' strings.
/// @return 0 if the pairs were written successfully, 1 otherwise.
int kvs_write(int fd_out, size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]);

/// Reads values from the KVS.
/// @param fd_out File descriptor to write the (successful) output.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @return 0 if the key reading, 1 otherwise.
int kvs_read(int fd_out, size_t num_pairs, char keys[][MAX_STRING_SIZE]);

/// Deletes key value pairs from the KVS.
/// @param fd_out File descriptor to write the (successful) output.
/// @param num_pairs Number of pairs to delete.
/// @param keys Array of keys' strings.
/// @return 0 if the pairs were deleted successfully, 1 otherwise.
int kvs_delete(int fd_out, size_t num_pairs, char keys[][MAX_STRING_SIZE]);

/// Writes the state of the KVS to the file described by fd_out.
/// @param fd_out File descriptor to write the output.
void kvs_show(int fd_out);

/// Creates a backup of the KVS state and stores it in the correspondent backup file.
/// @return 0 if the backup was successful, 1 otherwise.
int kvs_backup();

/// Waits for the last backup to be called.
void kvs_wait_backup();

/// Waits for a given amount of time.
/// @param delay_ms Delay in milliseconds.
void kvs_wait(unsigned int delay_ms);

/// Clears the KVS state (Resets the table).
void kvs_clear();

/// @brief Ordena uma lista por ordem alfabetica
/// @param list 
/// @param size 
void order_list(char list[][MAX_STRING_SIZE], size_t size);

#endif  // KVS_OPERATIONS_H
