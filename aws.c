// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static io_context_t ctx;

static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	// Preparing the connection buffer to send the reply header
	char header[BUFSIZ];

	sprintf(header, "HTTP/1.1 200 OK\r\n"
					"Accept-Ranges: bytes\r\n"
					"Content-Length: %ld\r\n"
					"Vary: Accept-Encoding\r\n"
					"Connection: close\r\n"
					"Content-Type: application/octet-stream\r\n"
					"\r\n", conn->file_size);
	conn->send_len = strlen(header);
	memcpy(conn->send_buffer, header, conn->send_len);

	// adding to epoll for out events
	int rc = w_epoll_update_ptr_out(epollfd, conn->sockfd, conn);

	DIE(rc < 0, "w_epoll_add_ptr_out");
}

static void connection_prepare_send_404(struct connection *conn)
{
	// Preparing the connection buffer to send the 404 header
	char header[200] = "HTTP/1.1 404 Not Found\r\n"
						"Content-Length: 13\r\n"
						"Content-Type: text/plain\r\n"
						"\r\n"
						"404 Not Found";
	conn->send_len = strlen(header);
	memcpy(conn->send_buffer, header, conn->send_len);

	// adding to epoll for out events
	int rc = w_epoll_update_ptr_out(epollfd, conn->sockfd, conn);

	DIE(rc < 0, "w_epoll_add_ptr_out");
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	// Getting resource type depending on request path/filename
	if (!conn->request_path) {
		conn->res_type = RESOURCE_TYPE_NONE;
		return RESOURCE_TYPE_NONE;
	}
	if (conn->fd < 0) {
		conn->res_type = RESOURCE_TYPE_NONE;
		return RESOURCE_TYPE_NONE;
	}

	char *static_filename = strstr(conn->request_path, "/static");
	char *dynamic_filename = strstr(conn->request_path, "/dynamic");
	char *request_path = conn->request_path + 1;

	if (static_filename) {
		conn->res_type = RESOURCE_TYPE_STATIC;
		printf("%s\n", request_path);
		memcpy(conn->filename, request_path, strlen(request_path) + 1);

		return RESOURCE_TYPE_STATIC;
	} else if (dynamic_filename) {
		conn->res_type = RESOURCE_TYPE_DYNAMIC;
		printf("%s\n", request_path);
		memcpy(conn->filename, request_path, strlen(request_path) + 1);

		return RESOURCE_TYPE_DYNAMIC;
	}

	conn->res_type = RESOURCE_TYPE_NONE;
	return RESOURCE_TYPE_NONE;
}


struct connection *connection_create(int sockfd)
{
	// Initializing connection structure on given socket
	struct connection *conn = malloc(sizeof(*conn));

	DIE(conn == NULL, "malloc");

	conn->sockfd = sockfd;
	conn->state = STATE_INITIAL;
	conn->res_type = RESOURCE_TYPE_NONE;
	memset(conn->recv_buffer, 0, BUFSIZ);
	conn->recv_len = 0;
	memset(conn->send_buffer, 0, BUFSIZ);

	conn->ctx = ctx;

	conn->send_pos = 0;
	conn->file_pos = 0;

	return conn;
}

void connection_start_async_io(struct connection *conn)
{
	// Starting asynchronous operation (read from file)
	conn->eventfd = eventfd(0, 0);

	conn->piocb[0] = &conn->iocb;

	io_set_eventfd(&conn->iocb, conn->eventfd);

	// prepare reading from the file in order to send it back to the client
	int file_rem = conn->file_size - conn->file_pos;

	conn->send_len = (file_rem > BUFSIZ) ? BUFSIZ : file_rem;
	memset(conn->send_buffer, 0, BUFSIZ);
	io_prep_pread(&conn->iocb, conn->fd, conn->send_buffer, conn->send_len, conn->file_pos);

	// after reading min_val bytes, the position from which continues to read changes
	conn->file_pos += conn->send_len;
	conn->send_pos = 0;

	conn->state = STATE_ASYNC_ONGOING;

	if (io_submit(conn->ctx, 1, conn->piocb) < 0) {
		ERR("io_submit failed\n");
		conn->state = STATE_CONNECTION_CLOSED;
		return;
	}
}

void connection_remove(struct connection *conn)
{
	char addr_buffer[BUFSIZ];
	int rc = get_peer_address(conn->sockfd, addr_buffer, BUFSIZ);

	if (rc < 0) {
		ERR("get_peer_address");
		connection_remove(conn);
		return;
	}

	// Removing connection handler
	rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
	DIE(rc < 0, "w_epoll_remove_ptr");

	close(conn->sockfd);
	conn->state = STATE_CONNECTION_CLOSED;
	free(conn);

	dlog(LOG_INFO, "Connection closed from: %s\n", addr_buffer);
}

void handle_new_connection(void)
{
	// Handling a new connection request on the server socket
	static int sockfd;
	socklen_t addrlen = sizeof(struct sockaddr_in);
	struct sockaddr_in addr;
	struct connection *conn;
	int rc;

	// Accepting new connection
	sockfd = accept(listenfd, (SSA *) &addr, &addrlen);
	DIE(sockfd < 0, "accept");

	dlog(LOG_ERR, "Accepted connection from: %s:%d\n",
		inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

	// Setting socket to be non-blocking
	int status = fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFL, 0) | O_NONBLOCK);

	DIE(status == -1, "fcntl");

	// Instantiating new connection handler
	conn = connection_create(sockfd);

	// Adding socket to epoll
	// only input events when adding a new connection
	rc = w_epoll_add_ptr_in(epollfd, sockfd, conn);
	DIE(rc < 0, "w_epoll_add_in");

	// Initializing HTTP_REQUEST parser
	http_parser_init(&conn->request_parser, HTTP_REQUEST);
	// Parser's data is the connection itself
	conn->request_parser.data = conn;
}

void receive_data(struct connection *conn)
{
	char addr_buffer[BUFSIZ];
	int rc = get_peer_address(conn->sockfd, addr_buffer, BUFSIZ);

	if (rc < 0) {
		ERR("get_peer_address");
		connection_remove(conn);
		return;
	}

	char *buf = conn->recv_buffer + conn->recv_len;
	int bytes_recv = recv(conn->sockfd, buf, BUFSIZ, 0);

	conn->recv_len += bytes_recv;

	dlog(LOG_DEBUG, "Received message from: %s\n", addr_buffer);


	if (strstr(conn->recv_buffer, "\r\n\r\n")) {
		conn->state = STATE_REQUEST_RECEIVED;
		return;
	}

	if (bytes_recv == -1)
		connection_remove(conn);

	if (bytes_recv == 0)
		connection_remove(conn);
}

int connection_open_file(struct connection *conn)
{
	// Opening file and updating connection fields
	conn->fd = open(conn->request_path + 1, O_RDONLY, 0777);
	if (conn->fd < 0) {
		ERR("Failed to open file\n");
		return -1;
	}
	// check the file size
	conn->file_size = lseek(conn->fd, 0, SEEK_END);
	lseek(conn->fd, 0, SEEK_SET);
	return 0;
}

void connection_complete_async_io(struct connection *conn)
{
	conn->state = STATE_SENDING_DATA;
}

void parse_header(struct connection *conn)
{
	http_parser_settings settings_on_path = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = 0
	};

	http_parser_execute(&conn->request_parser, &settings_on_path,
										conn->recv_buffer, strlen(conn->recv_buffer));
}

enum connection_state connection_send_static(struct connection *conn)
{
	char addr_buffer[BUFSIZ];
	int rc = get_peer_address(conn->sockfd, addr_buffer, BUFSIZ);

	if (rc < 0) {
		ERR("get_peer_address");
		connection_remove(conn);
		return conn->state;
	}

	// Sending header first
	if (conn->send_len > 0) {
		if (conn->fd < 0)
			conn->state = STATE_SENDING_404;
		else
			conn->state = STATE_SENDING_HEADER;

		int bytes_sent;
		char *buf = conn->send_buffer + conn->send_pos;

		// Sending what is left of the buffer
		bytes_sent = send(conn->sockfd, buf, conn->send_len, 0);
		conn->send_len -= bytes_sent;
		conn->send_pos += bytes_sent;

		dlog(LOG_DEBUG, "Sending header/error to: %s\n", addr_buffer);

		if (conn->send_len == 0) {
			if (conn->fd < 0) {
				conn->state = STATE_404_SENT;
				dlog(LOG_DEBUG, "Error sent to: %s\n", addr_buffer);
				connection_remove(conn);
			} else {
				conn->state = STATE_HEADER_SENT;
				dlog(LOG_DEBUG, "Header sent to: %s\n", addr_buffer);
			}
			return conn->state;
		}

		if (bytes_sent < 0) {
			close(conn->fd);
			connection_remove(conn);
		}
		if (bytes_sent == 0) {
			close(conn->fd);
			connection_remove(conn);
		}

		return conn->state;
	}

	// Starting sending the actual data
	conn->state = STATE_SENDING_DATA;
	int bytes_sent;

	if (conn->file_size > 0) {
		bytes_sent = sendfile(conn->sockfd, conn->fd, NULL, conn->file_size);
		conn->file_size -= bytes_sent;

		dlog(LOG_DEBUG, "Sending data to: %s\n", addr_buffer);

		if (conn->file_size == 0) {
			conn->state = STATE_DATA_SENT;
			dlog(LOG_INFO, "Data sent to: %s\n", addr_buffer);
			close(conn->fd);
			connection_remove(conn);
			return conn->state;
		}

		if (bytes_sent < 0) {
			close(conn->fd);
			connection_remove(conn);
		}

		if (bytes_sent == 0) {
			close(conn->fd);
			connection_remove(conn);
		}
	}
	return conn->state;
}

int connection_send_data(struct connection *conn)
{
	char addr_buffer[BUFSIZ];
	int rc = get_peer_address(conn->sockfd, addr_buffer, BUFSIZ);

	if (rc < 0) {
		ERR("get_peer_address");
		connection_remove(conn);
		return conn->state;
	}

	if (conn->send_len > 0) {
		conn->state = STATE_SENDING_DATA;

		int bytes_sent;
		char *buf = conn->send_buffer + conn->send_pos;

		// Sending what is left of the buffer
		bytes_sent = send(conn->sockfd, buf, conn->send_len, 0);
		conn->send_len -= bytes_sent;
		conn->send_pos += bytes_sent;

		dlog(LOG_DEBUG, "Sending data to: %s\n", addr_buffer);

		if (conn->send_len == 0) {
			conn->state = STATE_DATA_SENT;

			dlog(LOG_INFO, "Data chunk sent to: %s\n", addr_buffer);

			// If done sending a chunk of data, fetch for more
			if (conn->file_pos != conn->file_size) {
				connection_start_async_io(conn);
			} else {
				close(conn->fd);
				connection_remove(conn);
			}

			return conn->state;
		}

		if (bytes_sent < 0) {
			close(conn->fd);
			connection_remove(conn);
		}
		if (bytes_sent == 0) {
			close(conn->fd);
			connection_remove(conn);
		}
	}
	return conn->state;
}


int connection_send_dynamic(struct connection *conn)
{
	char addr_buffer[BUFSIZ];
	int rc = get_peer_address(conn->sockfd, addr_buffer, BUFSIZ);

	if (rc < 0) {
		ERR("get_peer_address");
		connection_remove(conn);
		return conn->state;
	}

	// Sending header first
	if (conn->state != STATE_SENDING_DATA) {
		if (conn->send_len > 0) {
			if (conn->fd < 0)
				conn->state = STATE_SENDING_404;
			else
				conn->state = STATE_SENDING_HEADER;

			int bytes_sent;
			char *buf = conn->send_buffer + conn->send_pos;

			// Sending what is left of the buffer
			bytes_sent = send(conn->sockfd, buf, conn->send_len, 0);
			conn->send_len -= bytes_sent;
			conn->send_pos += bytes_sent;

			dlog(LOG_DEBUG, "Sending header/error to: %s\n", addr_buffer);

			if (conn->send_len == 0) {
				if (conn->fd < 0) {
					conn->state = STATE_404_SENT;
					dlog(LOG_DEBUG, "Error sent to: %s\n", addr_buffer);
					connection_remove(conn);
				} else {
					conn->state = STATE_HEADER_SENT;
					dlog(LOG_DEBUG, "Header sent to: %s\n", addr_buffer);

					// Submit for reading
					connection_start_async_io(conn);
				}
				return conn->state;
			}

			if (bytes_sent < 0) {
				close(conn->fd);
				connection_remove(conn);
			}
			if (bytes_sent == 0) {
				close(conn->fd);
				connection_remove(conn);
			}
		}
		return conn->state;
	}

	connection_send_data(conn);

	return conn->state;
}


void handle_input(struct connection *conn)
{
	// Handling input information
	if (conn->state == STATE_RECEIVING_DATA) {
		receive_data(conn);
		if (conn->state == STATE_CONNECTION_CLOSED)
			return;

		if (conn->state == STATE_REQUEST_RECEIVED) {
			parse_header(conn);
			if (conn->state == STATE_CONNECTION_CLOSED)
				return;
			int err = connection_open_file(conn);
			// If file wasn't found
			if (err == -1)
				connection_prepare_send_404(conn);
			else
				connection_prepare_send_reply_header(conn);
		}
	}
}

void handle_output(struct connection *conn)
{
	// Handling output information: may be a new valid requests or notification of
	// completion of an asynchronous I/O operation
	if (conn->state != STATE_ASYNC_ONGOING) {
		if (conn->res_type == RESOURCE_TYPE_NONE)
			connection_get_resource_type(conn);

		if (conn->res_type == RESOURCE_TYPE_STATIC || conn->res_type == RESOURCE_TYPE_NONE)
			connection_send_static(conn);
		else
			connection_send_dynamic(conn);
	} else {
		struct io_event events[1];
		int rc = io_getevents(conn->ctx, 1, 1, events, NULL);

		if (rc > 0)
			connection_complete_async_io(conn);
		else
			connection_remove(conn);
	}
}

void handle_client(uint32_t event, struct connection *conn)
{
	if (event == 0) {
		if (conn->state == STATE_INITIAL)
			conn->state = STATE_RECEIVING_DATA;

		handle_input(conn);
	} else {
		handle_output(conn);
	}
}

int main(void)
{
	// Initializing asynchronous operations
	int rc = io_setup(1, &ctx);

	DIE(rc < 0, "io_setup");

	// Initializing multiplexing
	epollfd = w_epoll_create();
	DIE(epollfd < 0, "w_epoll_create");

	// Creating server socket
	listenfd = tcp_create_listener(AWS_LISTEN_PORT,
		DEFAULT_LISTEN_BACKLOG);
	DIE(listenfd < 0, "tcp_create_listener");

	// Adding server socket to epoll object
	rc = w_epoll_add_fd_in(epollfd, listenfd);
	DIE(rc < 0, "w_epoll_add_fd_in");

	dlog(LOG_INFO, "Server waiting for connections on port %d\n", AWS_LISTEN_PORT);

	// server main loop
	while (1) {
		struct epoll_event rev;

		// Waiting for events
		rc = w_epoll_wait_infinite(epollfd, &rev);
		DIE(rc < 0, "w_epoll_wait_infinite");

		// event types
		if (rev.data.fd == listenfd) {
			// new connection requests (on server socket)
			dlog(LOG_DEBUG, "New connection\n");
			if (rev.events & EPOLLIN)
				handle_new_connection();
		} else {
			// socket communication (on connection sockets)
			if (rev.events & EPOLLIN) {
				dlog(LOG_DEBUG, "New message\n");
				handle_client(0, rev.data.ptr);
			}
			if (rev.events & EPOLLOUT) {
				dlog(LOG_DEBUG, "Ready to send message\n");
				handle_client(1, rev.data.ptr);
			}
		}
	}

	return 0;
}
