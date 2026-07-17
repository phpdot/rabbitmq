<?php

declare(strict_types=1);

/**
 * Consumer
 *
 * Consumes messages from a RabbitMQ queue with retry and dead-letter support.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\RabbitMQ;

use Closure;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use PHPdot\RabbitMQ\Enum\TaskStatus;
use PHPdot\RabbitMQ\Exception\ConsumeException;
use PHPdot\RabbitMQ\Topology\TopologyManager;
use Psr\Log\LoggerInterface;
use Throwable;

final class Consumer
{
    private int $prefetchCount = 10;

    /**
     * @var \Closure(Message, int): void|null
     */
    private ?Closure $onRetryCallback = null;

    /**
     * @var \Closure(Message, string): void|null
     */
    private ?Closure $onDeadCallback = null;

    /**
     * Creates a new consumer for the given queue.
     *
     * @param string $queue The queue name to consume from
     * @param RabbitMQConnection $connection The AMQP connection
     * @param TopologyManager $topology The topology manager for queue declarations
     * @param LoggerInterface $logger The logger instance
     */
    public function __construct(
        private readonly string $queue,
        private readonly RabbitMQConnection $connection,
        private readonly TopologyManager $topology,
        private readonly LoggerInterface $logger,
    ) {}

    /**
     * Sets the prefetch count for the consumer.
     *
     * @param int $count The number of messages to prefetch (1-65535)
     *
     * @throws ConsumeException If the count is out of range
     *
     * @return self
     */
    public function prefetch(int $count): self
    {
        if ($count < 1 || $count > 65535) {
            throw ConsumeException::invalidPrefetchCount($count);
        }

        $this->prefetchCount = $count;

        return $this;
    }

    /**
     * Registers a callback to invoke when a message is retried.
     *
     * @param \Closure(Message, int): void $callback The retry callback
     *
     * @return Consumer
     */
    public function onRetry(Closure $callback): self
    {
        $this->onRetryCallback = $callback;

        return $this;
    }

    /**
     * Registers a callback to invoke when a message is sent to the dead letter queue.
     *
     * @param \Closure(Message, string): void $callback The dead letter callback
     *
     * @return Consumer
     */
    public function onDead(Closure $callback): self
    {
        $this->onDeadCallback = $callback;

        return $this;
    }

    /**
     * Starts consuming messages from the queue.
     *
     * @param \Closure(Message): TaskStatus $callback The message handler that returns a TaskStatus
     *
     * @throws ConsumeException If consumption fails
     *
     * @return void
     */
    public function execute(Closure $callback): void
    {
        $this->connection->ensureConnected();
        $channel = $this->connection->getChannel();
        $this->topology->prepareForConsume($this->queue, $channel);
        $channel->basic_qos(0, $this->prefetchCount, false);

        $handler = function (AMQPMessage $amqpMsg) use ($callback): void {
            $this->handleMessage($amqpMsg, $callback);
        };

        $channel->basic_consume(
            $this->queue,
            '',
            false,
            false,
            false,
            false,
            $handler,
        );

        while ($channel->is_consuming()) {
            $channel->wait();
        }
    }

    /**
     * Handles an individual AMQP message.
     *
     * @param AMQPMessage $amqpMsg The raw AMQP message
     * @param \Closure(Message): TaskStatus $callback The message handler
     *
     * @return void
     */
    private function handleMessage(AMQPMessage $amqpMsg, Closure $callback): void
    {
        try {
            $body = $amqpMsg->getBody();

            if ($amqpMsg->has('content_encoding')) {
                /**
                 * @var string $encoding
                 */
                $encoding = $amqpMsg->get('content_encoding');

                if ($encoding === 'gzip') {
                    $decoded = base64_decode($body, true);

                    if ($decoded === false) {
                        $message = Message::fromAMQP($amqpMsg, $this->queue);
                        $this->handleDead($amqpMsg, $message, 'Failed to decode compressed message body');

                        return;
                    }

                    $decompressed = gzuncompress($decoded);

                    if ($decompressed === false) {
                        $message = Message::fromAMQP($amqpMsg, $this->queue);
                        $this->handleDead($amqpMsg, $message, 'Failed to decompress message body');

                        return;
                    }

                    $amqpMsg->setBody($decompressed);
                }
            }

            $message = Message::fromAMQP($amqpMsg, $this->queue);
            $status = $callback($message);

            match ($status) {
                TaskStatus::SUCCESS => $this->handleSuccess($amqpMsg, $message),
                TaskStatus::RETRY => $this->handleRetry($amqpMsg, $message),
                TaskStatus::DEAD => $this->handleDead($amqpMsg, $message, 'Marked as dead by handler'),
            };
        } catch (Throwable $e) {
            $this->logger->error('Consumer error', [
                'queue' => $this->queue,
                'error' => $e->getMessage(),
            ]);

            $message = Message::fromAMQP($amqpMsg, $this->queue);
            $this->handleDead($amqpMsg, $message, $e->getMessage());
        }
    }

    /**
     * Acknowledges a successfully processed message.
     *
     * @param AMQPMessage $amqpMsg The raw AMQP message to acknowledge
     * @param Message $message The parsed message
     *
     * @return void
     */
    private function handleSuccess(AMQPMessage $amqpMsg, Message $message): void
    {
        $amqpMsg->ack();

        $this->logger->debug('Message processed successfully', [
            'queue' => $this->queue,
            'message_id' => $message->messageId(),
        ]);
    }

    /**
     * Retries a message or sends it to the dead letter queue if max retries exceeded.
     *
     * @param AMQPMessage $amqpMsg The raw AMQP message
     * @param Message $message The parsed message
     *
     * @return void
     */
    private function handleRetry(AMQPMessage $amqpMsg, Message $message): void
    {
        $retryCount = $this->extractRetryCount($amqpMsg);
        $maxRetries = $message->maxRetries();

        if ($maxRetries > 0 && $retryCount >= $maxRetries) {
            $this->logger->warning('Max retries exceeded, sending to dead letter', [
                'queue' => $this->queue,
                'message_id' => $message->messageId(),
                'retry_count' => $retryCount,
                'max_retries' => $maxRetries,
            ]);

            $this->handleDead($amqpMsg, $message, 'Max retries exceeded');

            return;
        }

        $amqpMsg->nack(false);

        if ($this->onRetryCallback !== null) {
            ($this->onRetryCallback)($message, $retryCount + 1);
        }

        $this->logger->debug('Message nacked for retry', [
            'queue' => $this->queue,
            'message_id' => $message->messageId(),
            'retry_count' => $retryCount + 1,
        ]);
    }

    /**
     * Sends a message to the dead letter exchange.
     *
     * @param AMQPMessage $amqpMsg The raw AMQP message
     * @param Message $message The parsed message
     * @param string $reason The reason for dead-lettering
     *
     * @return void
     */
    private function handleDead(AMQPMessage $amqpMsg, Message $message, string $reason): void
    {
        $amqpMsg->ack();

        $dlxExchange = $this->topology->getDeadLetterExchange($this->queue);

        if ($dlxExchange === null) {
            $this->logger->warning('No dead letter exchange configured, message discarded', [
                'queue' => $this->queue,
                'message_id' => $message->messageId(),
                'reason' => $reason,
            ]);

            if ($this->onDeadCallback !== null) {
                ($this->onDeadCallback)($message, $reason);
            }

            return;
        }

        $dlxRoutingKey = $this->topology->getDeadLetterRoutingKey($this->queue);
        $channel = $this->connection->getChannel();

        $headers = $message->headers();
        $headers['x-failed-queue'] = $this->queue;
        $headers['x-failed-reason'] = $reason;
        $headers['x-failed-timestamp'] = time();

        $applicationHeaders = new AMQPTable($headers);

        /**
         * @var array<string, mixed> $properties
         */
        $properties = [
            'delivery_mode' => 2,
            'application_headers' => $applicationHeaders,
        ];

        $contentType = $amqpMsg->has('content_type') ? $amqpMsg->get('content_type') : null;
        if (is_string($contentType)) {
            $properties['content_type'] = $contentType;
        }

        $contentEncoding = $amqpMsg->has('content_encoding') ? $amqpMsg->get('content_encoding') : null;
        if (is_string($contentEncoding)) {
            $properties['content_encoding'] = $contentEncoding;
        }

        if ($message->messageId() !== '') {
            $properties['message_id'] = $message->messageId();
        }

        $deadMessage = new AMQPMessage($amqpMsg->getBody(), $properties);
        $channel->basic_publish($deadMessage, $dlxExchange, $dlxRoutingKey !== '' ? $dlxRoutingKey : $this->queue);

        if ($this->onDeadCallback !== null) {
            ($this->onDeadCallback)($message, $reason);
        }

        $this->logger->warning('Message sent to dead letter', [
            'queue' => $this->queue,
            'message_id' => $message->messageId(),
            'reason' => $reason,
        ]);
    }

    /**
     * Extracts the retry count from the x-death header of an AMQP message.
     *
     * @param AMQPMessage $amqpMsg The raw AMQP message
     *
     * @return int The number of times this message has been retried
     */
    private function extractRetryCount(AMQPMessage $amqpMsg): int
    {
        if (!$amqpMsg->has('application_headers')) {
            return 0;
        }

        /**
         * @var AMQPTable $applicationHeaders
         */
        $applicationHeaders = $amqpMsg->get('application_headers');

        /**
         * @var array<string, mixed> $headers
         */
        $headers = $applicationHeaders->getNativeData();

        if (!isset($headers['x-death']) || !is_array($headers['x-death'])) {
            return 0;
        }

        $retryQueue = $this->queue . '.retry';
        $count = 0;

        foreach ($headers['x-death'] as $death) {
            if (!is_array($death)) {
                continue;
            }

            $deathQueue = $death['queue'] ?? '';

            if ($deathQueue === $this->queue || $deathQueue === $retryQueue) {
                $deathCount = $death['count'] ?? 1;
                $count += is_int($deathCount) ? $deathCount : (is_numeric($deathCount) ? intval($deathCount) : 1);
            }
        }

        return $count;
    }
}
