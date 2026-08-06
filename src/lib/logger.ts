import { pino } from "pino";
import { config } from "../config/index.js";

/**
 * Process-wide structured logger.
 *
 * Silent under NODE_ENV=test so unit runs stay readable; pretty-printed in
 * development, raw NDJSON in production (and inside worker containers, where
 * `docker compose logs` is the consumer).
 */
export const logger = pino({
  level: config.NODE_ENV === "test" ? "silent" : "info",
  transport:
    config.NODE_ENV === "development"
      ? {
          target: "pino-pretty",
          options: {
            colorize: true,
            ignore: "pid,hostname",
            // dateformat tokens: MM=minutes, ss=seconds, l=milliseconds.
            // Capital SS is the ordinal suffix ("thth"), not seconds.
            translateTime: "SYS:HH:MM:ss.l",
          },
        }
      : undefined,
});
