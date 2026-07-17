<?php

declare(strict_types=1);

namespace PHPdot\RabbitMQ\Tests;

use PHPdot\RabbitMQ\Exception\ConnectionException;
use PHPdot\RabbitMQ\Exception\ConsumeException;
use PHPdot\RabbitMQ\Exception\PublishException;
use PHPdot\RabbitMQ\Exception\RabbitMQException;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class ExceptionTest extends TestCase
{
    #[Test]
    public function queueExceptionExtendsRuntimeException(): void
    {
        $exception = new RabbitMQException('test');

        self::assertInstanceOf(RuntimeException::class, $exception);
    }

    #[Test]
    public function connectionExceptionIsInstanceOfRabbitMQException(): void
    {
        $exception = ConnectionException::connectionFailed('rabbitmq.local', 3, 'RabbitMQConnection refused');

        self::assertInstanceOf(RabbitMQException::class, $exception);
    }

    #[Test]
    public function connectionExceptionFactoryMessageContainsHost(): void
    {
        $exception = ConnectionException::connectionFailed('rabbitmq.local', 3, 'RabbitMQConnection refused');

        self::assertStringContainsString('rabbitmq.local', $exception->getMessage());
    }

    #[Test]
    public function connectionExceptionFactoryMessageContainsError(): void
    {
        $exception = ConnectionException::connectionFailed('rabbitmq.local', 3, 'RabbitMQConnection refused');

        self::assertStringContainsString('RabbitMQConnection refused', $exception->getMessage());
    }

    #[Test]
    public function connectionExceptionFactoryMessageContainsAttempts(): void
    {
        $exception = ConnectionException::connectionFailed('rabbitmq.local', 5, 'Timeout');

        self::assertStringContainsString('5', $exception->getMessage());
    }

    #[Test]
    public function channelNotInitializedReturnsConnectionException(): void
    {
        $exception = ConnectionException::channelNotInitialized();

        self::assertInstanceOf(ConnectionException::class, $exception);
        self::assertInstanceOf(RabbitMQException::class, $exception);
    }

    #[Test]
    public function reconnectFailedContainsError(): void
    {
        $exception = ConnectionException::reconnectFailed('Socket closed');

        self::assertStringContainsString('Socket closed', $exception->getMessage());
    }

    #[Test]
    public function publishExceptionIsInstanceOfRabbitMQException(): void
    {
        $exception = PublishException::publishFailed('events', 'user.created', 'Channel closed');

        self::assertInstanceOf(RabbitMQException::class, $exception);
    }

    #[Test]
    public function publishExceptionFactoryMessageContainsExchange(): void
    {
        $exception = PublishException::publishFailed('events', 'user.created', 'Channel closed');

        self::assertStringContainsString('events', $exception->getMessage());
    }

    #[Test]
    public function publishExceptionFactoryMessageContainsRoutingKey(): void
    {
        $exception = PublishException::publishFailed('events', 'user.created', 'Channel closed');

        self::assertStringContainsString('user.created', $exception->getMessage());
    }

    #[Test]
    public function publishExceptionExchangeNotFoundContainsExchangeName(): void
    {
        $exception = PublishException::exchangeNotFound('missing-exchange');

        self::assertInstanceOf(PublishException::class, $exception);
        self::assertStringContainsString('missing-exchange', $exception->getMessage());
    }

    #[Test]
    public function publishExceptionCompressionFailedReturnsException(): void
    {
        $exception = PublishException::compressionFailed();

        self::assertInstanceOf(PublishException::class, $exception);
        self::assertInstanceOf(RabbitMQException::class, $exception);
    }

    #[Test]
    public function consumeExceptionQueueNotFoundContainsQueueName(): void
    {
        $exception = ConsumeException::queueNotFound('notifications');

        self::assertInstanceOf(ConsumeException::class, $exception);
        self::assertInstanceOf(RabbitMQException::class, $exception);
        self::assertStringContainsString('notifications', $exception->getMessage());
    }

    #[Test]
    public function consumeExceptionInvalidCallbackReturnTypeReturnsException(): void
    {
        $exception = ConsumeException::invalidCallbackReturnType();

        self::assertInstanceOf(ConsumeException::class, $exception);
        self::assertStringContainsString('TaskStatus', $exception->getMessage());
    }

    #[Test]
    public function consumeExceptionInvalidPrefetchCountContainsCount(): void
    {
        $exception = ConsumeException::invalidPrefetchCount(0);

        self::assertInstanceOf(ConsumeException::class, $exception);
        self::assertStringContainsString('0', $exception->getMessage());
    }

    #[Test]
    public function consumeExceptionDecompressFailedReturnsException(): void
    {
        $exception = ConsumeException::decompressFailed();

        self::assertInstanceOf(ConsumeException::class, $exception);
        self::assertInstanceOf(RabbitMQException::class, $exception);
    }
}
