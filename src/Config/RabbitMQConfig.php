<?php

declare(strict_types=1);

/**
 * RabbitMQConfig
 *
 * Immutable value object holding all RabbitMQ connection settings,
 * exchange definitions, and queue definitions.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\RabbitMQ\Config;

use PHPdot\Container\Attribute\Config;

#[Config('rabbitmq')]
final readonly class RabbitMQConfig
{
    /**
     * Creates a new connection configuration.
     *
     * @param string $host The RabbitMQ server hostname
     * @param int $port The RabbitMQ server port
     * @param string $username The authentication username
     * @param string $password The authentication password
     * @param string $vhost The virtual host
     * @param int $timeoutMs The connection timeout in milliseconds
     * @param int $maxRetries The maximum number of reconnection attempts
     * @param int $retryDelayMs The base delay between retries in milliseconds
     * @param array<string, array<string, mixed>> $exchanges Exchange definitions keyed by name
     * @param array<string, array<string, mixed>> $queues Queue definitions keyed by name
     */
    public function __construct(
        public string $host = 'localhost',
        public int $port = 5672,
        public string $username = 'guest',
        public string $password = 'guest',
        public string $vhost = '/',
        public int $timeoutMs = 3000,
        public int $maxRetries = 3,
        public int $retryDelayMs = 1000,
        public array $exchanges = [],
        public array $queues = [],
    ) {}
}
