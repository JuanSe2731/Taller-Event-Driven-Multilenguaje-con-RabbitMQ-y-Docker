#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/framing.h>
#include <rabbitmq-c/tcp_socket.h>

#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>

static std::string envs(const char* k, const char* defv) {
  const char* v = std::getenv(k);
  return v ? v : defv;
}

static void die_rpc(amqp_connection_state_t conn, const char* ctx) {
  amqp_rpc_reply_t r = amqp_get_rpc_reply(conn);
  if (r.reply_type == AMQP_RESPONSE_NORMAL) return;
  std::cerr << "[consumer-delta] " << ctx << " failed (rpc)\n";
  std::exit(1);
}

static void die_neg(int rc, const char* ctx) {
  if (rc >= 0) return;
  std::cerr << "[consumer-delta] " << ctx << " failed (rc=" << rc << ")\n";
  std::exit(1);
}

static amqp_connection_state_t connect_and_login(
  const std::string& host, int port,
  const std::string& user, const std::string& pass
) {
  amqp_connection_state_t conn = amqp_new_connection();
  amqp_socket_t* sock = amqp_tcp_socket_new(conn);
  if (!sock) throw std::runtime_error("tcp_socket_new failed");

  int rc = amqp_socket_open(sock, host.c_str(), port);
  if (rc < 0) throw std::runtime_error("socket open failed (rc=" + std::to_string(rc) + ")");

  amqp_rpc_reply_t login = amqp_login(
    conn, "/", 0, 131072, 0,
    AMQP_SASL_METHOD_PLAIN, user.c_str(), pass.c_str()
  );
  if (login.reply_type != AMQP_RESPONSE_NORMAL) {
    throw std::runtime_error("login failed");
  }

  return conn;
}

int main() {
  std::string host = envs("RABBITMQ_HOST", "rabbitmq");
  int port = std::stoi(envs("RABBITMQ_PORT", "5672"));
  std::string user = envs("RABBITMQ_USER", "guest");
  std::string pass = envs("RABBITMQ_PASS", "guest");

  std::string exchange = envs("EXCHANGE", "events.direct");
  std::string exchange_type = envs("EXCHANGE_TYPE", "direct");
  std::string queue = envs("QUEUE", "q.delta");

  const std::string routing_key = "event.delta";

  while (true) {
    try {
      auto conn = connect_and_login(host, port, user, pass);

      amqp_channel_open(conn, 1);
      die_rpc(conn, "channel open");

      amqp_exchange_declare(
        conn, 1,
        amqp_cstring_bytes(exchange.c_str()),
        amqp_cstring_bytes(exchange_type.c_str()),
        0, 1, 0, 0, amqp_empty_table
      );
      die_rpc(conn, "exchange declare");

      amqp_queue_declare(
        conn, 1,
        amqp_cstring_bytes(queue.c_str()),
        0, 1, 0, 0, amqp_empty_table
      );
      die_rpc(conn, "queue declare");

      amqp_queue_bind(
        conn, 1,
        amqp_cstring_bytes(queue.c_str()),
        amqp_cstring_bytes(exchange.c_str()),
        amqp_cstring_bytes(routing_key.c_str()),
        amqp_empty_table
      );
      die_rpc(conn, "queue bind");

      amqp_basic_qos(conn, 1, 0, 10, 0);
      die_rpc(conn, "basic_qos");

      amqp_basic_consume(
        conn, 1,
        amqp_cstring_bytes(queue.c_str()),
        amqp_empty_bytes,
        0, 0, 0,
        amqp_empty_table
      );
      die_rpc(conn, "basic_consume");

      std::cout << "[consumer-delta] waiting queue=" << queue
                << " exchange=" << exchange
                << " type=" << exchange_type
                << " rk=" << routing_key << "\n";

      while (true) {
        amqp_envelope_t envelope;
        amqp_maybe_release_buffers(conn);

        amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, nullptr, 0);
        if (res.reply_type != AMQP_RESPONSE_NORMAL) {
          throw std::runtime_error("consume_message failed");
        }

        std::string body((char*)envelope.message.body.bytes, envelope.message.body.len);
        std::cout << "[consumer-delta] got: " << body << "\n";

        amqp_basic_ack(conn, 1, envelope.delivery_tag, 0);
        amqp_destroy_envelope(&envelope);
      }

    } catch (const std::exception& e) {
      std::cerr << "[consumer-delta] error: " << e.what() << ". retrying in 3s...\n";
      std::this_thread::sleep_for(std::chrono::seconds(3));
    }
  }
}
