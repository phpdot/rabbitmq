<?php

declare(strict_types=1);

/**
 * Adapts `phpdot/rabbitmq`'s `RabbitMQConnection` to `phpdot/pool`'s
 * connector contract.
 *
 * Lets any pool (e.g., `phpdot/pool`) hold and manage `RabbitMQConnection`
 * instances: `connect()` builds a fresh connection and ensures it's open;
 * `isAlive()` reports the underlying AMQP socket state; `close()` shuts
 * the connection down.
 *
 * The connector itself depends only on `PHPdot\Contracts\Pool\ConnectorInterface`
 * — it does not require `phpdot/pool` at runtime, so `phpdot/rabbitmq` stays
 * usable in non-pooled contexts (FPM, CLI, scripts) without pulling pooling
 * code along.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\RabbitMQ;

use PHPdot\Contracts\Pool\ConnectorInterface;
use PHPdot\RabbitMQ\Config\RabbitMQConfig;
use Throwable;

final class RabbitMQConnector implements ConnectorInterface
{
    /**
     * Wire the connector to the config every new connection is built from.
     *
     * @param RabbitMQConfig $config Broker connection settings for each RabbitMQConnection this builds
     */
    public function __construct(
        private readonly RabbitMQConfig $config,
    ) {}

    /**
     * Build a fresh `RabbitMQConnection`, opening the AMQP socket before
     * handing it back.
     */
    public function connect(): object
    {
        $connection = new RabbitMQConnection($this->config);
        $connection->connect();

        return $connection;
    }

    /**
     * Report whether the AMQP socket is still alive. Never throws.
     */
    public function isAlive(object $connection): bool
    {
        if (!$connection instanceof RabbitMQConnection) {
            return false;
        }

        try {
            return $connection->isConnected();
        } catch (Throwable) {
            return false;
        }
    }

    /**
     * Close the connection. Never throws.
     */
    public function close(object $connection): void
    {
        if (!$connection instanceof RabbitMQConnection) {
            return;
        }

        try {
            $connection->close();
        } catch (Throwable) {
        }
    }
}
