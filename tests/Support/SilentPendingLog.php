<?php

declare(strict_types=1);

namespace PHPdot\RabbitMQ\Tests\Support;

use PHPdot\Contracts\Logs\PendingLogInterface;

/**
 * The no-op pending log: secure() accepts and forgets — nothing is ever
 * written from a test.
 */
final class SilentPendingLog implements PendingLogInterface
{
    public function secure(): static
    {
        return $this;
    }

    public static function make(): self
    {
        return new self();
    }
}
