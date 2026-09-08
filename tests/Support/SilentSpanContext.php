<?php

declare(strict_types=1);

namespace PHPdot\RabbitMQ\Tests\Support;

use PHPdot\Contracts\Logs\SpanContextInterface;

/**
 * The context half of the silent tracer fixture: fixed W3C-shaped ids, so code
 * that stamps a traceparent on outbound messages has something valid to emit.
 */
final class SilentSpanContext implements SpanContextInterface
{
    private const string TRACE_ID = '4bf92f3577b34da6a3ce929d0e0e4736';
    private const string SPAN_ID = '00f067aa0ba902b7';

    public function traceId(): string
    {
        return self::TRACE_ID;
    }

    public function spanId(): string
    {
        return self::SPAN_ID;
    }

    public function parentSpanId(): null|string
    {
        return null;
    }

    public function sampled(): bool
    {
        return true;
    }

    public function toTraceparent(): string
    {
        return '00-' . self::TRACE_ID . '-' . self::SPAN_ID . '-01';
    }

    public static function make(): self
    {
        return new self();
    }
}
