import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { AmqplibInstrumentation } from "@opentelemetry/instrumentation-amqplib";
import { AwsInstrumentation } from "@opentelemetry/instrumentation-aws-sdk";
import { FastifyInstrumentation } from "@opentelemetry/instrumentation-fastify";
import { HttpInstrumentation } from "@opentelemetry/instrumentation-http";
import { IORedisInstrumentation } from "@opentelemetry/instrumentation-ioredis";
import { NodeSDK } from "@opentelemetry/sdk-node";
import { config } from "../config/index.js";

/**
 * OpenTelemetry bootstrap. Imported *first* in both entrypoints, before any
 * instrumented library, because the instrumentations patch modules at require
 * time -- a client built earlier is invisible to them.
 *
 * The amqplib instrumentation is the one that matters most here: it injects
 * trace context into message headers, which is what makes a single Jaeger trace
 * span the API publish and the worker's transcode.
 */
let sdk: NodeSDK | undefined;

export function startTracing(): NodeSDK | undefined {
  // Unit tests have no collector and must not open exporter sockets that keep
  // the process alive after the suite finishes.
  if (config.NODE_ENV === "test" || sdk) return sdk;

  sdk = new NodeSDK({
    serviceName: config.OTEL_SERVICE_NAME,
    traceExporter: new OTLPTraceExporter({
      url: `${config.OTEL_EXPORTER_OTLP_ENDPOINT}/v1/traces`,
    }),
    instrumentations: [
      new HttpInstrumentation(),
      new FastifyInstrumentation(),
      new AmqplibInstrumentation(),
      new IORedisInstrumentation(),
      new AwsInstrumentation(),
    ],
  });

  sdk.start();
  return sdk;
}

/** Flushes pending spans. Entrypoints call this last on the shutdown path. */
export async function stopTracing(): Promise<void> {
  if (!sdk) return;
  await sdk.shutdown();
  sdk = undefined;
}

// Deliberate import-time side effect: entrypoints are the only importers, and
// the whole point is to run before anything else they pull in.
startTracing();
