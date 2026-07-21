import {
  ROOT_CONTEXT,
  SpanKind,
  SpanStatusCode,
  context,
  propagation,
  trace,
  type Attributes,
  type Span,
  type TextMapGetter,
} from '@opentelemetry/api'
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http'
import { resourceFromAttributes } from '@opentelemetry/resources'
import { BatchSpanProcessor } from '@opentelemetry/sdk-trace-base'
import { WebTracerProvider } from '@opentelemetry/sdk-trace-web'
import {
  OTEL_ENABLED,
  OTEL_DEPLOYMENT_ENVIRONMENT,
  OTEL_EXPORTER_OTLP_TRACES_URL,
  OTEL_SERVICE_NAME,
  OTEL_SERVICE_VERSION,
} from './runtimeConfig'

let initialized = false
let tracer = trace.getTracer(OTEL_SERVICE_NAME)

type HeaderCarrier = {
  get: (key: string) => string | string[] | undefined | null
  keys?: () => string[]
}

const headerGetter: TextMapGetter<HeaderCarrier> = {
  get(carrier, key) {
    const value = carrier.get(key)
    return value === null || value === '' ? undefined : value
  },
  keys(carrier) {
    return carrier.keys?.() ?? []
  },
}

function isPromiseLike(value: unknown): value is Promise<unknown> {
  return Boolean(value && typeof (value as Promise<unknown>).then === 'function')
}

export function initTelemetry(): void {
  if (initialized || !OTEL_ENABLED || typeof window === 'undefined') return

  const exporter = new OTLPTraceExporter({
    url: OTEL_EXPORTER_OTLP_TRACES_URL,
  })

  const provider = new WebTracerProvider({
    resource: resourceFromAttributes({
      'service.name': OTEL_SERVICE_NAME,
      'service.namespace': 'chat',
      'service.version': OTEL_SERVICE_VERSION,
      'deployment.environment.name': OTEL_DEPLOYMENT_ENVIRONMENT,
    }),
    spanProcessors: [
      new BatchSpanProcessor(exporter, {
        scheduledDelayMillis: 1000,
        exportTimeoutMillis: 5000,
      }),
    ],
  })

  provider.register()

  tracer = trace.getTracer(OTEL_SERVICE_NAME)
  initialized = true
}

export function injectTraceHeaders(headers: { set: (key: string, value: string) => void }) {
  propagation.inject(context.active(), headers, {
    set(carrier, key, value) {
      carrier.set(key, value)
    },
  })
  return headers
}

export function natsSpanName(operation: string, subject: string): string {
  return `nats ${operation} ${subject}`
}

function runInSpan<T>(span: Span, fn: (span: Span) => T): T {
  try {
    const result = fn(span)
    if (isPromiseLike(result)) {
      return result
        .catch((err) => {
          span.recordException(err)
          span.setStatus({
            code: SpanStatusCode.ERROR,
            message: err instanceof Error ? err.message : String(err),
          })
          throw err
        })
        .finally(() => span.end()) as T
    }
    span.end()
    return result
  } catch (err) {
    span.recordException(err as Error)
    span.setStatus({
      code: SpanStatusCode.ERROR,
      message: err instanceof Error ? err.message : String(err),
    })
    span.end()
    throw err
  }
}

export function withSpan<T>(
  name: string,
  attributes: Attributes,
  fn: (span: Span) => T,
  kind: SpanKind = SpanKind.CLIENT,
): T {
  return tracer.startActiveSpan(name, { kind, attributes }, (span) => {
    return runInSpan(span, fn)
  })
}

export function withLinkedSpan<T>(
  name: string,
  attributes: Attributes,
  headers: HeaderCarrier | undefined,
  fn: (span: Span) => T,
  kind: SpanKind = SpanKind.CONSUMER,
): T {
  const extracted = headers ? propagation.extract(ROOT_CONTEXT, headers, headerGetter) : ROOT_CONTEXT
  const linkedContext = trace.getSpanContext(extracted)
  const links = linkedContext && trace.isSpanContextValid(linkedContext)
    ? [{ context: linkedContext }]
    : []
  return tracer.startActiveSpan(name, { kind, attributes, links }, ROOT_CONTEXT, (span) => {
    return runInSpan(span, fn)
  })
}
