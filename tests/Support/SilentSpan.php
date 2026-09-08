<?php

declare(strict_types=1);

namespace PHPdot\RabbitMQ\Tests\Support;

use PHPdot\Contracts\Logs\PendingLogInterface;
use PHPdot\Contracts\Logs\SpanContextInterface;
use PHPdot\Contracts\Logs\SpanInterface;

/**
 * The span half of the silent tracer fixture: attributes, events, and ends
 * are recorded nowhere; log methods return no-op pending logs.
 */
final class SilentSpan implements SpanInterface
{
    public function setAttribute(string $key, string|int|float|bool $value): static
    {
        return $this;
    }

    public function addEvent(string $name, array $attributes = []): static
    {
        return $this;
    }

    public function setStatus(string $status, string $description = ''): static
    {
        return $this;
    }

    public function status(): string
    {
        return 'unset';
    }

    public function context(): SpanContextInterface
    {
        return SilentSpanContext::make();
    }

    public function end(): void {}

    public function debug(string $message, array $context = []): PendingLogInterface
    {
        return SilentPendingLog::make();
    }

    public function info(string $message, array $context = []): PendingLogInterface
    {
        return SilentPendingLog::make();
    }

    public function notice(string $message, array $context = []): PendingLogInterface
    {
        return SilentPendingLog::make();
    }

    public function warning(string $message, array $context = []): PendingLogInterface
    {
        return SilentPendingLog::make();
    }

    public function error(string $message, array $context = []): PendingLogInterface
    {
        return SilentPendingLog::make();
    }

    public function critical(string $message, array $context = []): PendingLogInterface
    {
        return SilentPendingLog::make();
    }

    public function alert(string $message, array $context = []): PendingLogInterface
    {
        return SilentPendingLog::make();
    }

    public function emergency(string $message, array $context = []): PendingLogInterface
    {
        return SilentPendingLog::make();
    }

    public static function make(): self
    {
        return new self();
    }
}
