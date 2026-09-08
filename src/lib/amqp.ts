import {
  connect,
  type ChannelModel,
  type ConfirmChannel,
  type RecoveringChannelModel,
} from "amqplib";
import { JobMessageSchema, type JobMessage } from "../domain/job.js";
import { logger } from "./logger.js";
import { EXCHANGES, assertTopology } from "./topology.js";

export interface AmqpSettings {
  url: string;
  /** Bounds the *first* connect so a misconfigured URL fails loudly. */
  firstConnectTimeoutMs?: number;
}

/**
 * Opens a self-recovering connection and asserts the topology on every successful
 * connect (including reconnects), so a broker that was restarted -- or wiped --
 * comes back fully configured without restarting our processes.
 *
 * Recovery covers the *connection* only. Channels opened from it are bound to
 * the connection that existed when they were created and are dead after a
 * reconnect, which is what `createConfirmChannelProvider` exists to handle.
 */
export async function connectAmqp({
  url,
  firstConnectTimeoutMs = 30_000,
}: AmqpSettings): Promise<RecoveringChannelModel> {
  // The recovery layer retries the first connect forever and its events cannot
  // be observed until `connect` resolves, so without this line a wrong
  // AMQP_URL looks exactly like a slow broker: total silence, no listen, no
  // exit. Log going in, and bound the wait so it eventually fails loudly.
  logger.info({ url }, "Connecting to AMQP");

  const model = await Promise.race([
    connect(url, {
      recovery: {
        async setup(connection: ChannelModel) {
          const channel = await connection.createChannel();
          await assertTopology(channel);
          await channel.close();
        },
      },
    }),
    new Promise<never>((_resolve, reject) =>
      setTimeout(
        () => reject(new Error(`Could not reach the AMQP broker at ${url}`)),
        firstConnectTimeoutMs,
      ).unref(),
    ),
  ]);

  model.on("disconnect", (error) => logger.warn({ err: error }, "AMQP connection lost"));
  model.on("reconnect-scheduled", ({ attempt, delay }) =>
    logger.warn({ attempt, delay }, "AMQP reconnect scheduled"),
  );
  model.on("connect", () => logger.info("AMQP connected"));
  // A connection-level 'error' with no listener would crash the process; the
  // recovery layer is already handling the reconnect.
  model.on("error", (error) => logger.error({ err: error }, "AMQP connection error"));

  return model;
}

interface ConfirmChannelSource {
  createConfirmChannel(): Promise<ConfirmChannel>;
}

/**
 * Hands out the process's confirm channel, transparently re-opening it after a
 * reconnect or a channel-level error.
 *
 * The in-flight promise is cached, not just the channel, so concurrent requests
 * during a reconnect share one re-open instead of racing to create several.
 */
export function createConfirmChannelProvider(source: ConfirmChannelSource) {
  let pending: Promise<ConfirmChannel> | null = null;
  let closed = false;

  return {
    async get() {
      if (closed) throw new Error("Confirm channel provider is closed");

      if (!pending) {
        const attempt: Promise<ConfirmChannel> = (async () => {
          const channel = await source.createConfirmChannel();
          const forget = () => {
            if (pending === attempt) pending = null;
          };
          channel.on("close", forget);
          // Without a listener a channel-level error is an unhandled 'error'
          // event, which would take the process down.
          channel.on("error", (error) => {
            logger.warn({ err: error }, "AMQP confirm channel error");
            forget();
          });
          return channel;
        })();

        // A failed open must not be cached, or every later publish replays it.
        attempt.catch(() => {
          if (pending === attempt) pending = null;
        });
        pending = attempt;
      }

      return pending;
    },

    async close() {
      closed = true;
      const channel = pending;
      pending = null;
      if (!channel) return;
      await channel.then((c) => c.close()).catch(() => undefined);
    },
  };
}

export type ConfirmChannelProvider = ReturnType<typeof createConfirmChannelProvider>;

export interface JobPublisherDeps {
  channelProvider: ConfirmChannelProvider;
  exchange?: string;
}

/**
 * Publishes job messages durably and resolves only once the broker has confirmed
 * the message. The API awaits this before replying 202, so a client is never
 * told "accepted" for work the broker did not take.
 *
 * Uses `publish`'s per-message confirm callback rather than `waitForConfirms()`:
 * `waitForConfirms` resolves for *all* outstanding publishes on the channel, so
 * under concurrent uploads every request would inherit the latency of the
 * slowest one. The callback confirms exactly this message.
 */
export function createJobPublisher({
  channelProvider,
  exchange = EXCHANGES.JOBS,
}: JobPublisherDeps) {
  return {
    async publish(routingKey: string, message: JobMessage): Promise<void> {
      // Parse on the way out: the wire contract is enforced here, once, rather
      // than trusted at three call sites.
      const payload = Buffer.from(JSON.stringify(JobMessageSchema.parse(message)));
      const channel = await channelProvider.get();

      await new Promise<void>((resolve, reject) => {
        const accepted = channel.publish(
          exchange,
          routingKey,
          payload,
          {
            persistent: true,
            contentType: "application/json",
            messageId: message.jobId,
          },
          (error) => (error ? reject(error) : resolve()),
        );

        // `false` means the channel's write buffer is full. The message is still
        // queued in the client, and the confirm callback still fires, so there
        // is nothing to do but let it drain -- worth logging, since sustained
        // backpressure here means the broker cannot keep up with ingest.
        if (!accepted) {
          logger.warn({ routingKey }, "AMQP publish buffer full; awaiting drain");
        }
      });
    },
  };
}

export type JobPublisher = ReturnType<typeof createJobPublisher>;
