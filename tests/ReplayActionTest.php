<?php

declare(strict_types=1);

namespace PHPdot\RabbitMQ\Tests;

use PHPdot\RabbitMQ\Enum\ReplayAction;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

final class ReplayActionTest extends TestCase
{
    #[Test]
    public function exactlyThreeCasesExist(): void
    {
        self::assertCount(3, ReplayAction::cases());
    }

    #[Test]
    public function replayCaseExists(): void
    {
        self::assertSame('replay', ReplayAction::REPLAY->value);
    }

    #[Test]
    public function removeCaseExists(): void
    {
        self::assertSame('remove', ReplayAction::REMOVE->value);
    }

    #[Test]
    public function skipCaseExists(): void
    {
        self::assertSame('skip', ReplayAction::SKIP->value);
    }

    #[Test]
    public function allCasesHaveUniqueValues(): void
    {
        $values = array_map(
            static fn(ReplayAction $case): string => $case->value,
            ReplayAction::cases(),
        );

        self::assertSame($values, array_unique($values));
    }

    #[Test]
    public function valuesAreLowercaseStrings(): void
    {
        foreach (ReplayAction::cases() as $case) {
            self::assertSame(strtolower($case->value), $case->value);
        }
    }
}
