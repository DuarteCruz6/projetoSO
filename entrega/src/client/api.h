#ifndef CLIENT_API_H
#define CLIENT_API_H

#include <stddef.h>

#include "src/common/constants.h"

//retorna o sinal de seguranca (0->false, 1->true)
/// @return 0 se nao houve nenhum sigsur1, 1 se houve.
int getSinalSeguranca();

//muda o sinal de seguranca quando houve um sigusr1
void mudarSinalSeguranca();

//manda request atraves do pipe request
/// @param message mensagem que é para mandar para o server
/// @param size numero de caracteres da mensagem
/// @return 0 se correu tudo bem, 1 se houve algum erro no processo.
int createMessage(char *message, int size);

//recebe a resposta atraves do pipe response
/// @return 0 se correu tudo bem, 1 se houve algum erro no processo.
int getResponse();

/// Connects to a kvs server.
/// @param req_pipe_path Path to the name pipe to be created for requests.
/// @param resp_pipe_path Path to the name pipe to be created for responses.
/// @param notif_pipe_path Path to the name pipe to be created for notifications.
/// @param server_pipe_path Path to the name pipe where the server is listening.
/// @return 0 if the connection was established successfully, 1 otherwise.
int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *notif_pipe_path, char const *server_pipe_path);
                
/// Disconnects from an KVS server.
/// @return 0 in case of success, 1 otherwise.
int kvs_disconnect();

/// Requests a subscription for a key
/// @param key Key to be subscribed
/// @return 0 if the key was subscribed successfully (key existing), 1
/// otherwise.
int kvs_subscribe(const char *key);

/// Remove a subscription for a key
/// @param key Key to be unsubscribed
/// @return 0 if the key was unsubscribed successfully  (subscription existed
/// and was removed), 1 otherwise.
int kvs_unsubscribe(const char *key);

#endif // CLIENT_API_H
