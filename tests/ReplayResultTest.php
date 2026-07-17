<?php

declare(strict_types=1);

namespace PHPdot\RabbitMQ\Tests;

use PHPdot\RabbitMQ\Result\ReplayResult;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

final class ReplayResultTest extends TestCase
{
    #[Test]
    public function totalEqualsSumOfReplayedRemovedAndSkipped(): void
    {
        $result = new ReplayResult(5, 3, 2);

        self::assertSame(10, $result->total);
    }

    #[Test]
    public function zeroCountsResultInZeroTotal(): void
    {
        $result = new ReplayResult(0, 0, 0);

        self::assertSame(0, $result->total);
        self::assertSame(0, $result->replayed);
        self::assertSame(0, $result->removed);
        self::assertSame(0, $result->skipped);
    }

    #[Test]
    public function mixedCountsAreAccessible(): void
    {
        $result = new ReplayResult(10, 5, 3);

        self::assertSame(10, $result->replayed);
        self::assertSame(5, $result->removed);
        self::assertSame(3, $result->skipped);
        self::assertSame(18, $result->total);
    }
}
