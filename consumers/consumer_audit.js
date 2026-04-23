const amqp = require("amqplib");

const RABBITMQ_HOST = process.env.RABBITMQ_HOST || "rabbitmq";
const RABBITMQ_PORT = process.env.RABBITMQ_PORT || "5672";
const RABBITMQ_USER = process.env.RABBITMQ_USER || "guest";
const RABBITMQ_PASS = process.env.RABBITMQ_PASS || "guest";

const EXCHANGE = process.env.EXCHANGE || "events.direct";
const EXCHANGE_TYPE = process.env.EXCHANGE_TYPE || "direct";
const QUEUE = process.env.QUEUE || "q.audit";

async function main() {
  const url = `amqp://${RABBITMQ_USER}:${RABBITMQ_PASS}@${RABBITMQ_HOST}:${RABBITMQ_PORT}`;

  while (true) {
    try {
      const conn = await amqp.connect(url);
      const ch = await conn.createChannel();

      await ch.assertExchange(EXCHANGE, EXCHANGE_TYPE, { durable: true });
      await ch.assertQueue(QUEUE, { durable: true });

      await ch.bindQueue(QUEUE, EXCHANGE, "event.alpha");
      await ch.bindQueue(QUEUE, EXCHANGE, "event.beta");

      ch.prefetch(10);

      console.log(`[consumer-audit] waiting messages in ${QUEUE} (alpha,beta) exchange=${EXCHANGE} type=${EXCHANGE_TYPE}`);

      ch.consume(
        QUEUE,
        (msg) => {
          if (!msg) return;
          console.log(`[consumer-audit] audited: ${msg.content.toString("utf8")}`);
          ch.ack(msg);
        },
        { noAck: false }
      );

      return;
    } catch (err) {
      console.error(`[consumer-audit] error: ${err}. retrying in 3s...`);
      await new Promise((r) => setTimeout(r, 3000));
    }
  }
}

main();
