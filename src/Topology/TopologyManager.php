<?php

declare(strict_types=1);

/**
 * TopologyManager
 *
 * Declares exchanges, queues, bindings, and retry infrastructure on AMQP channels.
 * Caches declared state to avoid redundant declarations.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\RabbitMQ\Topology;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Wire\AMQPTable;
use PHPdot\RabbitMQ\Config\RabbitMQConfig;
use PHPdot\RabbitMQ\Exception\ConsumeException;
use PHPdot\RabbitMQ\Exception\PublishException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class TopologyManager
{
    /**
     * @var array<string, bool>
     */
    private array $declaredExchanges = [];

    /**
     * @var array<string, bool>
     */
    private array $declaredQueues = [];

    /**
     * Creates a new topology manager.
     *
     * @param RabbitMQConfig $config The connection configuration containing exchange and queue definitions
     * @param LoggerInterface $logger The logger instance
     */
    public function __construct(
        private readonly RabbitMQConfig $config,
        private readonly LoggerInterface $logger = new NullLogger(),
    ) {}

    /**
     * Prepares topology for publishing to an exchange.
     *
     * Declares the exchange and all queues bound to it. Skips if already declared.
     *
     * @param string $exchange The exchange name to prepare
     * @param AMQPChannel $channel The AMQP channel to declare on
     *
     * @throws PublishException If the exchange is not defined in configuration
     *
     * @return void
     */
    public function prepareForPublish(string $exchange, AMQPChannel $channel): void
    {
        if (isset($this->declaredExchanges[$exchange])) {
            return;
        }

        if (!isset($this->config->exchanges[$exchange])) {
            throw PublishException::exchangeNotFound($exchange);
        }

        $exchangeConfig = $this->config->exchanges[$exchange];

        $channel->exchange_declare(
            $exchange,
            $this->extractString($exchangeConfig['type'] ?? 'direct'),
            false,
            $this->extractBool($exchangeConfig['durable'] ?? true),
            $this->extractBool($exchangeConfig['auto_delete'] ?? false),
        );

        $this->declaredExchanges[$exchange] = true;

        $this->logger->debug('Exchange declared', ['exchange' => $exchange]);

        foreach ($this->config->queues as $queueName => $queueConfig) {
            $bindings = $queueConfig['bindings'] ?? [];

            if (!is_array($bindings)) {
                continue;
            }

            foreach ($bindings as $binding) {
                if (!is_array($binding)) {
                    continue;
                }

                if (($binding['exchange'] ?? '') === $exchange) {
                    if (!isset($this->declaredQueues[$queueName])) {
                        $this->declareQueueWithRetry($queueName, $queueConfig, $channel);
                    }

                    $channel->queue_bind(
                        $queueName,
                        $exchange,
                        $this->extractString($binding['routing_key'] ?? ''),
                    );
                }
            }
        }
    }

    /**
     * Prepares topology for consuming from a queue.
     *
     * Declares the queue, its bindings, and retry infrastructure if enabled.
     *
     * @param string $queue The queue name to prepare
     * @param AMQPChannel $channel The AMQP channel to declare on
     *
     * @throws ConsumeException If the queue is not defined in configuration
     *
     * @return void
     */
    public function prepareForConsume(string $queue, AMQPChannel $channel): void
    {
        if (isset($this->declaredQueues[$queue])) {
            return;
        }

        if (!isset($this->config->queues[$queue])) {
            throw ConsumeException::queueNotFound($queue);
        }

        $queueConfig = $this->config->queues[$queue];

        $this->declareQueueWithRetry($queue, $queueConfig, $channel);

        $bindings = $queueConfig['bindings'] ?? [];

        if (!is_array($bindings)) {
            $bindings = [];
        }

        foreach ($bindings as $binding) {
            if (!is_array($binding)) {
                continue;
            }

            $exchangeName = $this->extractString($binding['exchange'] ?? '');

            if (!isset($this->declaredExchanges[$exchangeName]) && isset($this->config->exchanges[$exchangeName])) {
                $exchangeConfig = $this->config->exchanges[$exchangeName];

                $channel->exchange_declare(
                    $exchangeName,
                    $this->extractString($exchangeConfig['type'] ?? 'direct'),
                    false,
                    $this->extractBool($exchangeConfig['durable'] ?? true),
                    $this->extractBool($exchangeConfig['auto_delete'] ?? false),
                );

                $this->declaredExchanges[$exchangeName] = true;
            }

            $channel->queue_bind(
                $queue,
                $exchangeName,
                $this->extractString($binding['routing_key'] ?? ''),
            );
        }

        $this->declaredQueues[$queue] = true;
    }

    /**
     * Resets all cached declaration state.
     *
     * @return void
     */
    public function reset(): void
    {
        $this->declaredExchanges = [];
        $this->declaredQueues = [];
    }

    /**
     * Get the dead letter exchange for a queue from config.
     *
     * @param string $queue The queue name
     *
     * @return string|null The DLX exchange name, or null if not configured
     */
    public function getDeadLetterExchange(string $queue): ?string
    {
        $queueConfig = $this->config->queues[$queue] ?? [];
        $dead = $queueConfig['dead'] ?? null;

        if ($dead === null) {
            return null;
        }

        if (is_string($dead)) {
            return $dead;
        }

        if (is_array($dead) && isset($dead['exchange'])) {
            $exchange = $dead['exchange'];

            return is_scalar($exchange) ? (string) $exchange : null;
        }

        return null;
    }

    /**
     * Get the dead letter routing key for a queue from config.
     *
     * @param string $queue The queue name
     *
     * @return string The DLX routing key, or empty string if not configured
     */
    public function getDeadLetterRoutingKey(string $queue): string
    {
        $queueConfig = $this->config->queues[$queue] ?? [];
        $dead = $queueConfig['dead'] ?? null;

        if (is_array($dead) && isset($dead['routing_key'])) {
            $routingKey = $dead['routing_key'];

            return is_scalar($routingKey) ? (string) $routingKey : '';
        }

        return '';
    }

    /**
     * Extracts a string value from a mixed config value.
     *
     * @param mixed $value The config value
     *
     * @return string The string value, or empty string if not scalar
     */
    private function extractString(mixed $value): string
    {
        if (is_string($value)) {
            return $value;
        }

        return is_scalar($value) ? (string) $value : '';
    }

    /**
     * Extracts a bool value from a mixed config value.
     *
     * @param mixed $value The config value
     *
     * @return bool The bool value
     */
    private function extractBool(mixed $value): bool
    {
        if (is_bool($value)) {
            return $value;
        }

        return (bool) $value;
    }

    /**
     * Extracts an int value from a mixed config value.
     *
     * @param mixed $value The config value
     *
     * @return int The int value
     */
    private function extractInt(mixed $value): int
    {
        if (is_int($value)) {
            return $value;
        }

        return is_scalar($value) ? (int) $value : 0;
    }

    /**
     * Normalizes a raw queue config array into the expected shape for declareQueue.
     *
     * @param array<string, mixed> $raw The raw queue configuration
     *
     * @return array{durable?: bool, exclusive?: bool, auto_delete?: bool, arguments?: array<string, mixed>}
     */
    private function normalizeQueueConfig(array $raw): array
    {
        $normalized = [];

        if (isset($raw['durable'])) {
            $normalized['durable'] = $this->extractBool($raw['durable']);
        }

        if (isset($raw['exclusive'])) {
            $normalized['exclusive'] = $this->extractBool($raw['exclusive']);
        }

        if (isset($raw['auto_delete'])) {
            $normalized['auto_delete'] = $this->extractBool($raw['auto_delete']);
        }

        $arguments = $raw['arguments'] ?? null;

        if (is_array($arguments)) {
            /**
             * @var array<string, mixed> $arguments
             */
            $normalized['arguments'] = $arguments;
        }

        return $normalized;
    }

    /**
     * Normalizes a raw retry config array into the expected shape for declareRetryInfrastructure.
     *
     * @param array<string, mixed> $raw The raw retry configuration
     *
     * @return array{delay_ms?: int}
     */
    private function normalizeRetryConfig(array $raw): array
    {
        $normalized = [];

        if (isset($raw['delay_ms'])) {
            $normalized['delay_ms'] = $this->extractInt($raw['delay_ms']);
        }

        return $normalized;
    }

    /**
     * Declares a queue with retry infrastructure if enabled.
     *
     * @param string $queue The queue name
     * @param array<string, mixed> $queueConfig Raw queue configuration
     * @param AMQPChannel $channel The AMQP channel
     *
     * @return void
     */
    private function declareQueueWithRetry(string $queue, array $queueConfig, AMQPChannel $channel): void
    {
        $retryRaw = $queueConfig['retry'] ?? [];
        /**
         * @var array<string, mixed> $retryConfig
         */
        $retryConfig = is_array($retryRaw) ? $retryRaw : [];
        $retryEnabled = $this->extractBool($retryConfig['enable'] ?? false);

        $normalizedConfig = $this->normalizeQueueConfig($queueConfig);

        if ($retryEnabled) {
            $this->declareRetryInfrastructure($queue, $this->normalizeRetryConfig($retryConfig), $channel);

            $existingArguments = $normalizedConfig['arguments'] ?? [];
            $normalizedConfig['arguments'] = array_merge($existingArguments, [
                'x-dead-letter-exchange' => $queue . '.retry.exchange',
                'x-dead-letter-routing-key' => $queue,
            ]);
        }

        $this->declareQueue($queue, $normalizedConfig, $channel);
    }

    /**
     * Declares a queue on the given channel.
     *
     * @param string $name The queue name
     * @param array{
     *     durable?: bool,
     *     exclusive?: bool,
     *     auto_delete?: bool,
     *     arguments?: array<string, mixed>,
     * } $config The queue configuration
     * @param AMQPChannel $channel The AMQP channel
     *
     * @return void
     */
    private function declareQueue(string $name, array $config, AMQPChannel $channel): void
    {
        $this->declaredQueues[$name] = true;

        $arguments = new AMQPTable($config['arguments'] ?? []);

        $channel->queue_declare(
            $name,
            false,
            $config['durable'] ?? true,
            $config['exclusive'] ?? false,
            $config['auto_delete'] ?? false,
            false,
            $arguments,
        );
    }

    /**
     * Declares retry exchange and retry queue with TTL and dead-letter routing.
     *
     * @param string $queue The original queue name
     * @param array{delay_ms?: int} $retryConfig The retry configuration
     * @param AMQPChannel $channel The AMQP channel
     *
     * @return void
     */
    private function declareRetryInfrastructure(string $queue, array $retryConfig, AMQPChannel $channel): void
    {
        $retryExchange = $queue . '.retry.exchange';
        $retryQueue = $queue . '.retry';
        $delayMs = $retryConfig['delay_ms'] ?? 5000;

        $channel->exchange_declare(
            $retryExchange,
            'direct',
            false,
            true,
            false,
        );

        $this->declaredExchanges[$retryExchange] = true;

        $retryArguments = new AMQPTable([
            'x-message-ttl' => $delayMs,
            'x-dead-letter-exchange' => '',
            'x-dead-letter-routing-key' => $queue,
        ]);

        $channel->queue_declare(
            $retryQueue,
            false,
            true,
            false,
            false,
            false,
            $retryArguments,
        );

        $channel->queue_bind($retryQueue, $retryExchange, $queue);

        $this->declaredQueues[$retryQueue] = true;
    }
}
