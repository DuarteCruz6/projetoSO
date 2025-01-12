#ifndef CLIENT_API_H
#define CLIENT_API_H

#include <stddef.h>

#include "src/common/constants.h"

int getSinalSeguranca();

void mudarSinalSeguranca();

/// Connects to a kvs server.
/// @param req_pipe_path Path to the name pipe to be created for requests.
/// @param resp_pipe_path Path to the name pipe to be created for responses.
/// @param server_pipe_path Path to the name pipe where the server is listening.
/// @return 0 if the connection was established successfully, 1 otherwise.
int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *notif_pipe_path, char const *server_pipe_path);
                
/// Disconnects from an KVS server.
/// @return 0 in case of success, 1 otherwise.
int kvs_disconnect(int req_pipe, int resp_pipe, int notif_pipe);

/// Requests a subscription for a key
/// @param key Key to be subscribed
/// @return 1 if the key was subscribed successfully (key existing), 0
/// otherwise.

int kvs_subscribe(int req_pipe, int resp_pipe, const char *key);

/// Remove a subscription for a key
/// @param key Key to be unsubscribed
/// @return 0 if the key was unsubscribed successfully  (subscription existed
/// and was removed), 1 otherwise.

int kvs_unsubscribe(int req_pipe, int resp_pipe, const char *key);

#endif // CLIENT_API_H
