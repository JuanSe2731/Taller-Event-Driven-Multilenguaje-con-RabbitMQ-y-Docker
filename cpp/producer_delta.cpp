#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/framing.h>
#include <rabbitmq-c/tcp_socket.h>

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <chrono>
#include <ctime>

static std::string envs(const char* k, const char* defv) {
  const char* v = std::getenv(k);
  return v ? v : defv;
}

static void die_rpc(amqp_connection_state_t conn, const char* ctx) {
  amqp_rpc_reply_t r = amqp_get_rpc_reply(conn);
  if (r.reply_type == AMQP_RESPONSE_NORMAL) return;
  std::cerr << "[producer-delta] " << ctx << " failed (rpc)\n";
  std::exit(1);
}

static void die_neg(int rc, const char* ctx) {
  if (rc >= 0) return;
  std::cerr << "[producer-delta] " << ctx << " failed (rc=" << rc << ")\n";
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
        0, 1, 0, 0,
        amqp_empty_table
      );
      die_rpc(conn, "exchange declare");

      std::cout << "[producer-delta] connected. exchange=" << exchange
                << " type=" << exchange_type
                << " rk=" << routing_key << "\n";

      while (true) {
        std::time_t t = std::time(nullptr);

        std::stringstream ss;
        ss << "{"
           << "\"producer\":\"producer-delta-cpp\","
           << "\"type\":\"delta\","
           << "\"id\":\"" << (long long)t << "\","
           << "\"ts\":\"" << (long long)t << "\","
           << "\"payload\":{\"msg\":\"hello from producer-delta-cpp\",\"n\":" << (long long)t << "}"
           << "}";

        std::string body = ss.str();
        amqp_bytes_t msg;
        msg.len = body.size();
        msg.bytes = (void*)body.data();

        amqp_basic_properties_t props;
        props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
        props.content_type = amqp_cstring_bytes("application/json");
        props.delivery_mode = 2;

        int rc = amqp_basic_publish(
          conn, 1,
          amqp_cstring_bytes(exchange.c_str()),
          amqp_cstring_bytes(routing_key.c_str()),
          0, 0, &props, msg
        );
        if (rc != 0) throw std::runtime_error("basic_publish failed");

        std::cout << "[producer-delta] sent -> " << routing_key << "\n";
        std::this_thread::sleep_for(std::chrono::seconds(5));
      }

    } catch (const std::exception& e) {
      std::cerr << "[producer-delta] error: " << e.what() << ". retrying in 3s...\n";
      std::this_thread::sleep_for(std::chrono::seconds(3));
    }
  }
}
