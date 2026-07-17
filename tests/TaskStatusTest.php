<?php

declare(strict_types=1);

namespace PHPdot\RabbitMQ\Tests;

use PHPdot\RabbitMQ\Enum\TaskStatus;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

final class TaskStatusTest extends TestCase
{
    #[Test]
    public function allCasesHaveUniqueValues(): void
    {
        $values = array_map(
            static fn(TaskStatus $case): string => $case->value,
            TaskStatus::cases(),
        );

        self::assertSame($values, array_unique($values));
    }

    #[Test]
    public function exactlyThreeCasesExist(): void
    {
        self::assertCount(3, TaskStatus::cases());
    }

    #[Test]
    public function successCaseExists(): void
    {
        self::assertSame('success', TaskStatus::SUCCESS->value);
    }

    #[Test]
    public function retryCaseExists(): void
    {
        self::assertSame('retry', TaskStatus::RETRY->value);
    }

    #[Test]
    public function deadCaseExists(): void
    {
        self::assertSame('dead', TaskStatus::DEAD->value);
    }

    #[Test]
    public function valuesAreLowercaseStrings(): void
    {
        foreach (TaskStatus::cases() as $case) {
            self::assertSame(strtolower($case->value), $case->value);
        }
    }
}
