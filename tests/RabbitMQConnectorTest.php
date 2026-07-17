<?php

declare(strict_types=1);

namespace PHPdot\RabbitMQ\Tests;

use PHPdot\Contracts\Pool\ConnectorInterface;
use PHPdot\RabbitMQ\Config\RabbitMQConfig;
use PHPdot\RabbitMQ\RabbitMQConnection;
use PHPdot\RabbitMQ\RabbitMQConnector;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use stdClass;

/**
 * Unit-level coverage for RabbitMQConnector. The `connect()` path requires a
 * live broker and is exercised by the integration suite; here we cover the
 * contract guarantees: interface implementation, type-guard rejection,
 * silent failure on close.
 */
final class RabbitMQConnectorTest extends TestCase
{
    private RabbitMQConnector $connector;

    protected function setUp(): void
    {
        $this->connector = new RabbitMQConnector(new RabbitMQConfig());
    }

    #[Test]
    public function it_implements_the_pool_connector_contract(): void
    {
        self::assertInstanceOf(ConnectorInterface::class, $this->connector);
    }

    #[Test]
    public function is_alive_rejects_foreign_objects(): void
    {
        self::assertFalse($this->connector->isAlive(new stdClass()));
    }

    #[Test]
    public function is_alive_returns_false_for_a_disconnected_connection(): void
    {
        $connection = new RabbitMQConnection(new RabbitMQConfig());

        self::assertFalse($this->connector->isAlive($connection));
    }

    #[Test]
    public function close_is_a_no_op_for_foreign_objects(): void
    {
        // Must not throw.
        $this->connector->close(new stdClass());
        $this->expectNotToPerformAssertions();
    }

    #[Test]
    public function close_swallows_errors_from_the_connection(): void
    {
        $connection = new RabbitMQConnection(new RabbitMQConfig());

        // Closing an already-disconnected connection must not throw.
        $this->connector->close($connection);
        $this->expectNotToPerformAssertions();
    }
}
