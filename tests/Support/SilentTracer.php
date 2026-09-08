<?php

declare(strict_types=1);

namespace PHPdot\RabbitMQ\Tests\Support;

use PHPdot\Contracts\Logs\PendingLogInterface;
use PHPdot\Contracts\Logs\SpanContextInterface;
use PHPdot\Contracts\Logs\SpanInterface;
use PHPdot\Contracts\Logs\TracerInterface;
use RuntimeException;

/**
 * A no-output tracer for tests: spans open and close (counted), logs and
 * pending-logs are no-ops — the double the PSR-3 NullLogger used to be, on
 * the tracer surface the package now speaks.
 */
final class SilentTracer implements TracerInterface
{
    public int $spans = 0;

    public function channel(string $name): self
    {
        return $this;
    }

    public function span(string $name, string $kind = 'internal'): SpanInterface
    {
        $this->spans++;

        return SilentSpan::make();
    }

    public function current(): SpanInterface
    {
        return SilentSpan::make();
    }

    public function context(): SpanContextInterface
    {
        throw new RuntimeException('not needed in the silent fixture');
    }

    public function trace(string $name, string $kind, callable $callback): mixed
    {
        $this->spans++;

        return $callback(SilentSpan::make());
    }

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
}
