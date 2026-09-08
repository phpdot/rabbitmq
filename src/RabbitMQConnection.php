<?php

declare(strict_types=1);

/**
 * RabbitMQConnection
 *
 * Main entry point for RabbitMQ messaging. Manages the AMQP connection,
 * channel lifecycle, and provides factory methods for publishers and consumers.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\RabbitMQ;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PHPdot\Contracts\Logs\TracerInterface;
use PHPdot\RabbitMQ\Config\RabbitMQConfig;
use PHPdot\RabbitMQ\Exception\ConnectionException;
use PHPdot\RabbitMQ\Topology\TopologyManager;
use Throwable;

final class RabbitMQConnection
{
    private null|AMQPStreamConnection $connection = null;

    private null|AMQPChannel $channel = null;

    private bool $connected = false;

    private readonly TopologyManager $topology;

    /**
     * Creates a new RabbitMQConnection instance.
     *
     * @param RabbitMQConfig $config The connection configuration
     * @param TracerInterface $tracer The logger instance
     */
    public function __construct(
        private readonly RabbitMQConfig $config,
        private readonly TracerInterface $tracer,
    ) {
        $this->topology = new TopologyManager($this->config, $this->tracer);
    }

    /**
     * Establishes the AMQP connection and opens a channel.
     *
     *
     * @throws ConnectionException If the connection cannot be established
     *
     * @return self
     */
    public function connect(): self
    {
        try {
            $this->connection = new AMQPStreamConnection(
                $this->config->host,
                $this->config->port,
                $this->config->username,
                $this->config->password,
                $this->config->vhost,
                false,
                'AMQPLAIN',
                null,
                'en_US',
                $this->config->timeoutMs / 1000,
            );

            $this->channel = $this->connection->channel();
            $this->connected = true;

            $this->tracer->channel('queue')->info('Connected to RabbitMQ', [
                'host' => $this->config->host,
                'port' => $this->config->port,
                'vhost' => $this->config->vhost,
            ]);
        } catch (Throwable $e) {
            $this->connected = false;

            throw ConnectionException::connectionFailed(
                $this->config->host,
                1,
                $e->getMessage(),
            );
        }

        return $this;
    }

    /**
     * Closes the AMQP channel and connection.
     *
     * @return void
     */
    public function close(): void
    {
        try {
            if ($this->channel !== null) {
                $this->channel->close();
            }
        } catch (Throwable) {
        }

        try {
            if ($this->connection !== null) {
                $this->connection->close();
            }
        } catch (Throwable) {
        }

        $this->channel = null;
        $this->connection = null;
        $this->connected = false;
    }

    /**
     * Closes and re-establishes a connection that was previously alive, with
     * exponential backoff. A connection that never existed is a FIRST
     * connect — one dial, one timeout, no retry budget burned, no
     * "Reconnected" line for a connection that was never open (ensureConnected
     * routes first use to connect()). With maxRetries zero a lost connection
     * is refused immediately: the critical line names the exhaustion, the
     * throw names the loss — a supervisor reading the exit owns the restart.
     *
     * @throws ConnectionException If all reconnection attempts fail
     *
     * @return void
     */
    public function reconnect(): void
    {
        $this->close();
        $this->topology->reset();

        $lastError = 'Unknown error';

        for ($attempt = 1; $attempt <= $this->config->maxRetries; $attempt++) {
            try {
                $this->connect();

                $this->tracer->channel('queue')->info('Reconnected to RabbitMQ', [
                    'attempt' => $attempt,
                ]);

                return;
            } catch (Throwable $e) {
                $lastError = $e->getMessage();
                $delay = $this->config->retryDelayMs * (2 ** ($attempt - 1));

                $this->tracer->channel('queue')->warning('Reconnection attempt failed', [
                    'attempt' => $attempt,
                    'max_retries' => $this->config->maxRetries,
                    'delay_ms' => $delay,
                    'error' => $lastError,
                ]);

                usleep($delay * 1000);
            }
        }

        $this->tracer->channel('queue')->critical('RabbitMQ connection permanently lost after retries', [
            'max_retries' => $this->config->maxRetries,
            'error' => $lastError,
        ]);

        throw ConnectionException::reconnectFailed($lastError);
    }

    /**
     * Checks whether the connection is alive.
     *
     * @return bool
     */
    public function isConnected(): bool
    {
        if (!$this->connected || $this->connection === null) {
            return false;
        }

        return $this->connection->isConnected();
    }

    /**
     * Returns the AMQP channel, connecting first if necessary.
     *
     *
     * @throws ConnectionException If the channel is not available
     *
     * @return AMQPChannel
     */
    public function getChannel(): AMQPChannel
    {
        $this->ensureConnected();

        if ($this->channel === null) {
            throw ConnectionException::channelNotInitialized();
        }

        return $this->channel;
    }

    /**
     * Ensures the connection is alive, reconnecting if necessary.
     *
     *
     * @throws ConnectionException If the connection cannot be restored
     *
     * @return void
     */
    public function ensureConnected(): void
    {
        if ($this->isConnected()) {
            return;
        }

        if ($this->connection === null) {
            $this->connect();

            return;
        }

        $this->reconnect();
    }

    /**
     * Creates a new Publisher for the given message content.
     *
     * @param string $content The message body
     *
     * @return Publisher
     */
    public function message(string $content): Publisher
    {
        return new Publisher($content, $this, $this->topology, $this->tracer);
    }

    /**
     * Creates a new Consumer for the given queue.
     *
     * @param string $queue The queue name to consume from
     *
     * @return Consumer
     */
    public function consume(string $queue): Consumer
    {
        return new Consumer($queue, $this, $this->topology, $this->tracer);
    }

    /**
     * Creates a new Replayer for the given dead letter queue.
     *
     * @param string $queue The dead letter queue name to replay from
     *
     * @return Replayer
     */
    public function replay(string $queue): Replayer
    {
        return new Replayer($queue, $this, $this->topology, $this->tracer);
    }

    /**
     * Closes the connection on destruction.
     */
    public function __destruct()
    {
        $this->close();
    }
}
