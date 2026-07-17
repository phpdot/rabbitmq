<?php

declare(strict_types=1);

/**
 * Replayer
 *
 * Replays dead-lettered messages from a DLQ back to their original exchange.
 * Uses basic_get (pull mode) for non-blocking, bounded iteration.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\RabbitMQ;

use Closure;
use PhpAmqpLib\Message\AMQPMessage;
use PHPdot\RabbitMQ\Enum\ReplayAction;
use PHPdot\RabbitMQ\Result\ReplayResult;
use PHPdot\RabbitMQ\Topology\TopologyManager;
use Psr\Log\LoggerInterface;
use Throwable;

final class Replayer
{
    private ?int $limit = null;

    /**
     * Creates a new replayer for the given dead letter queue.
     *
     * @param string $queue The dead letter queue name to replay from
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
     * Sets the maximum number of messages to process.
     *
     * @param int $limit The maximum number of messages to process
     *
     * @return self
     */
    public function limit(int $limit): self
    {
        $this->limit = $limit;

        return $this;
    }

    /**
     * Executes the replay, calling the callback for each message to determine the action.
     *
     * @param \Closure(Message): ReplayAction $callback The callback that decides the action for each message
     *
     * @throws \Throwable If the callback throws an exception (message is nacked and exception is rethrown)
     *
     * @return ReplayResult
     */
    public function execute(Closure $callback): ReplayResult
    {
        $this->connection->ensureConnected();
        $channel = $this->connection->getChannel();
        $this->topology->prepareForConsume($this->queue, $channel);

        $replayed = 0;
        $removed = 0;
        $skipped = 0;
        $processed = 0;

        while (true) {
            if ($this->limit !== null && $processed >= $this->limit) {
                break;
            }

            $amqpMsg = $channel->basic_get($this->queue, false);

            if (!$amqpMsg instanceof AMQPMessage) {
                break;
            }

            $message = Message::fromAMQP($amqpMsg, $this->queue);

            try {
                $action = $callback($message);
            } catch (Throwable $e) {
                $amqpMsg->nack(true);

                throw $e;
            }

            match ($action) {
                ReplayAction::REPLAY => $this->handleReplay($amqpMsg, $message, $replayed),
                ReplayAction::REMOVE => $this->handleRemove($amqpMsg, $message, $removed),
                ReplayAction::SKIP => $this->handleSkip($amqpMsg, $message, $skipped),
            };

            $processed++;
        }

        return new ReplayResult($replayed, $removed, $skipped);
    }

    /**
     * Replays a message back to its original exchange.
     *
     * @param AMQPMessage $amqpMsg The raw AMQP message
     * @param Message $message The parsed message
     * @param int $replayed The replay counter (passed by reference)
     *
     * @return void
     */
    private function handleReplay(AMQPMessage $amqpMsg, Message $message, int &$replayed): void
    {
        $originalExchange = $message->originalExchange();
        $originalRoutingKey = $message->originalRoutingKey();

        if ($originalExchange === '') {
            $this->logger->warning('Cannot replay message without original exchange, acknowledging', [
                'queue' => $this->queue,
                'message_id' => $message->messageId(),
            ]);

            $amqpMsg->ack();

            return;
        }

        $headers = $message->headers();

        /**
         * @var array<string, mixed> $cleanHeaders
         */
        $cleanHeaders = array_filter(
            $headers,
            static fn(string $key): bool => !str_starts_with($key, 'x-failed-') && $key !== 'x-death',
            ARRAY_FILTER_USE_KEY,
        );

        $cleanHeaders['x-replayed-at'] = time();
        $cleanHeaders['x-replayed-from'] = $this->queue;

        $publisher = $this->connection->message($amqpMsg->getBody())
            ->header($cleanHeaders);

        if ($message->messageId() !== '') {
            $publisher->messageId($message->messageId());
        }

        $properties = $message->properties();

        if (isset($properties['content_type']) && is_string($properties['content_type'])) {
            $publisher->property('content_type', $properties['content_type']);
        }

        if (isset($properties['content_encoding']) && is_string($properties['content_encoding'])) {
            $publisher->property('content_encoding', $properties['content_encoding']);
        }

        $publisher->publish($originalExchange, $originalRoutingKey);

        $amqpMsg->ack();
        $replayed++;

        $this->logger->info('Message replayed', [
            'queue' => $this->queue,
            'message_id' => $message->messageId(),
            'exchange' => $originalExchange,
            'routing_key' => $originalRoutingKey,
        ]);
    }

    /**
     * Removes a message from the dead letter queue by acknowledging it.
     *
     * @param AMQPMessage $amqpMsg The raw AMQP message
     * @param Message $message The parsed message
     * @param int $removed The remove counter (passed by reference)
     *
     * @return void
     */
    private function handleRemove(AMQPMessage $amqpMsg, Message $message, int &$removed): void
    {
        $amqpMsg->ack();
        $removed++;

        $this->logger->info('Message removed from dead letter queue', [
            'queue' => $this->queue,
            'message_id' => $message->messageId(),
        ]);
    }

    /**
     * Skips a message by nacking it back to the queue.
     *
     * @param AMQPMessage $amqpMsg The raw AMQP message
     * @param Message $message The parsed message
     * @param int $skipped The skip counter (passed by reference)
     *
     * @return void
     */
    private function handleSkip(AMQPMessage $amqpMsg, Message $message, int &$skipped): void
    {
        $amqpMsg->nack(true);
        $skipped++;

        $this->logger->debug('Message skipped during replay', [
            'queue' => $this->queue,
            'message_id' => $message->messageId(),
        ]);
    }
}
