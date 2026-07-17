<?php

declare(strict_types=1);

namespace PHPdot\RabbitMQ\Tests;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use PHPdot\RabbitMQ\Config\RabbitMQConfig;
use PHPdot\RabbitMQ\RabbitMQConnection;
use PHPdot\RabbitMQ\Consumer;
use PHPdot\RabbitMQ\Enum\TaskStatus;
use PHPdot\RabbitMQ\Exception\ConsumeException;
use PHPdot\RabbitMQ\Message;
use PHPdot\RabbitMQ\Topology\TopologyManager;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use ReflectionClass;

final class ConsumerTest extends TestCase
{
    /**
     * Creates a Consumer with a real RabbitMQConnection whose internals are
     * wired to a mock channel via reflection (RabbitMQConnection is final).
     *
     * @return array{0: Consumer, 1: AMQPChannel&\PHPUnit\Framework\MockObject\MockObject}
     */
    private function createConsumer(string $queue = 'test.queue', ?RabbitMQConfig $config = null): array
    {
        $channel = $this->createMock(AMQPChannel::class);

        $channel->method('exchange_declare')->willReturn(null);
        $channel->method('queue_declare')->willReturn(null);
        $channel->method('queue_bind')->willReturn(null);
        $channel->method('basic_qos')->willReturn(null);

        $config ??= new RabbitMQConfig(
            exchanges: ['test' => ['type' => 'direct']],
            queues: [$queue => ['bindings' => [['exchange' => 'test', 'routing_key' => 'key']]]],
        );

        $connection = $this->buildFakeConnection($config, $channel);

        $topology = new TopologyManager($config);
        $consumer = new Consumer($queue, $connection, $topology, new NullLogger());

        return [$consumer, $channel];
    }

    /**
     * Builds a real RabbitMQConnection instance with mock internals via reflection.
     */
    private function buildFakeConnection(
        RabbitMQConfig $config,
        AMQPChannel&\PHPUnit\Framework\MockObject\MockObject $channel,
    ): RabbitMQConnection {
        $connection = new RabbitMQConnection($config, new NullLogger());
        $ref = new ReflectionClass($connection);

        $refConnected = $ref->getProperty('connected');
        $refConnected->setValue($connection, true);

        $refChannel = $ref->getProperty('channel');
        $refChannel->setValue($connection, $channel);

        $streamConn = $this->createMock(AMQPStreamConnection::class);
        $streamConn->method('isConnected')->willReturn(true);

        $refConn = $ref->getProperty('connection');
        $refConn->setValue($connection, $streamConn);

        return $connection;
    }

    /**
     * Creates an AMQPMessage with delivery_info pointing at the mock channel.
     *
     * @param array<string, mixed> $properties
     */
    private function createAmqpMessage(
        AMQPChannel&\PHPUnit\Framework\MockObject\MockObject $channel,
        string $body = 'test body',
        array $properties = [],
        string $deliveryTag = 'tag-1',
    ): AMQPMessage {
        if (!isset($properties['message_id'])) {
            $properties['message_id'] = 'test-msg-id';
        }

        $msg = new AMQPMessage($body, $properties);
        $msg->setChannel($channel);
        $msg->setDeliveryInfo($deliveryTag, false, 'test', 'key');
        $msg->delivery_info['consumer_tag'] = 'consumer-1';

        return $msg;
    }

    /**
     * Sets up basic_consume to capture the handler, is_consuming to loop once,
     * and wait to invoke the handler with the test message.
     */
    private function setupConsumeLoop(AMQPChannel&\PHPUnit\Framework\MockObject\MockObject $channel, AMQPMessage $testMsg): void
    {
        /** @var callable|null $handler */
        $handler = null;

        $channel->method('basic_consume')
            ->willReturnCallback(static function (
                string $queue,
                string $tag,
                bool $noLocal,
                bool $noAck,
                bool $exclusive,
                bool $nowait,
                callable $callback,
            ) use (&$handler): string {
                $handler = $callback;

                return 'consumer-tag';
            });

        $channel->method('is_consuming')
            ->willReturnOnConsecutiveCalls(true, false);

        $channel->method('wait')
            ->willReturnCallback(static function () use (&$handler, $testMsg): void {
                if ($handler !== null) {
                    $handler($testMsg);
                    $handler = null;
                }
            });
    }

    #[Test]
    public function executeWithSuccessCallbackAcksMessage(): void
    {
        [$consumer, $channel] = $this->createConsumer();
        $testMsg = $this->createAmqpMessage($channel);

        $this->setupConsumeLoop($channel, $testMsg);

        $channel->expects(self::once())
            ->method('basic_ack')
            ->with('tag-1');

        $consumer->execute(static fn(Message $msg): TaskStatus => TaskStatus::SUCCESS);
    }

    #[Test]
    public function executeWithRetryCallbackNacksMessage(): void
    {
        [$consumer, $channel] = $this->createConsumer();
        $testMsg = $this->createAmqpMessage($channel);

        $this->setupConsumeLoop($channel, $testMsg);

        $channel->expects(self::once())
            ->method('basic_nack')
            ->with('tag-1', false, false);

        $consumer->execute(static fn(Message $msg): TaskStatus => TaskStatus::RETRY);
    }

    #[Test]
    public function executeWithDeadCallbackAcksAndPublishesToDeadLetter(): void
    {
        $config = new RabbitMQConfig(
            exchanges: ['test' => ['type' => 'direct'], 'dlx' => ['type' => 'direct']],
            queues: [
                'test.queue' => [
                    'bindings' => [['exchange' => 'test', 'routing_key' => 'key']],
                    'dead' => ['exchange' => 'dlx', 'routing_key' => 'dead.key'],
                ],
            ],
        );

        [$consumer, $channel] = $this->createConsumer('test.queue', $config);
        $testMsg = $this->createAmqpMessage($channel);

        $this->setupConsumeLoop($channel, $testMsg);

        $channel->expects(self::once())
            ->method('basic_ack')
            ->with('tag-1');

        $channel->expects(self::once())
            ->method('basic_publish');

        $consumer->execute(static fn(Message $msg): TaskStatus => TaskStatus::DEAD);
    }

    #[Test]
    public function executeWithExceptionSendsToDeadLetter(): void
    {
        $config = new RabbitMQConfig(
            exchanges: ['test' => ['type' => 'direct'], 'dlx' => ['type' => 'direct']],
            queues: [
                'test.queue' => [
                    'bindings' => [['exchange' => 'test', 'routing_key' => 'key']],
                    'dead' => ['exchange' => 'dlx', 'routing_key' => 'dead.key'],
                ],
            ],
        );

        [$consumer, $channel] = $this->createConsumer('test.queue', $config);
        $testMsg = $this->createAmqpMessage($channel);

        $this->setupConsumeLoop($channel, $testMsg);

        $channel->expects(self::once())
            ->method('basic_ack')
            ->with('tag-1');

        /** @var AMQPMessage|null $deadMsg */
        $deadMsg = null;

        $channel->expects(self::once())
            ->method('basic_publish')
            ->willReturnCallback(static function (AMQPMessage $msg) use (&$deadMsg): void {
                $deadMsg = $msg;
            });

        $consumer->execute(static function (Message $msg): TaskStatus {
            throw new \RuntimeException('Something broke');
        });

        self::assertNotNull($deadMsg);

        /** @var AMQPTable $headers */
        $headers = $deadMsg->get('application_headers');
        /** @var array<string, mixed> $data */
        $data = $headers->getNativeData();

        self::assertSame('Something broke', $data['x-failed-reason']);
    }

    #[Test]
    public function executeDecompressesGzipEncodedMessage(): void
    {
        [$consumer, $channel] = $this->createConsumer();

        $original = 'original decompressed body';
        /** @var string $rawCompressed */
        $rawCompressed = gzcompress($original, 9);
        $compressed = base64_encode($rawCompressed);

        $testMsg = $this->createAmqpMessage($channel, $compressed, [
            'message_id' => 'compressed-test',
            'content_encoding' => 'gzip',
        ]);

        $this->setupConsumeLoop($channel, $testMsg);

        /** @var string|null $receivedBody */
        $receivedBody = null;

        $consumer->execute(static function (Message $msg) use (&$receivedBody): TaskStatus {
            $receivedBody = $msg->body();

            return TaskStatus::SUCCESS;
        });

        self::assertSame($original, $receivedBody);
    }

    #[Test]
    public function prefetchThrowsForZero(): void
    {
        [$consumer] = $this->createConsumer();

        $this->expectException(ConsumeException::class);
        $consumer->prefetch(0);
    }

    #[Test]
    public function prefetchThrowsForValueAboveMax(): void
    {
        [$consumer] = $this->createConsumer();

        $this->expectException(ConsumeException::class);
        $consumer->prefetch(65536);
    }

    #[Test]
    public function prefetchAcceptsValidRange(): void
    {
        [$consumer] = $this->createConsumer();

        $result = $consumer->prefetch(100);

        self::assertSame($consumer, $result);
    }

    #[Test]
    public function onRetryCallbackReceivesMessageAndRetryCount(): void
    {
        [$consumer, $channel] = $this->createConsumer();
        $testMsg = $this->createAmqpMessage($channel);

        $this->setupConsumeLoop($channel, $testMsg);

        /** @var int|null $receivedRetryCount */
        $receivedRetryCount = null;
        /** @var Message|null $receivedMessage */
        $receivedMessage = null;

        $consumer->onRetry(static function (Message $msg, int $count) use (&$receivedRetryCount, &$receivedMessage): void {
            $receivedMessage = $msg;
            $receivedRetryCount = $count;
        });

        $consumer->execute(static fn(Message $msg): TaskStatus => TaskStatus::RETRY);

        self::assertNotNull($receivedMessage);
        self::assertSame(1, $receivedRetryCount);
    }

    #[Test]
    public function onDeadCallbackReceivesMessageAndReason(): void
    {
        $config = new RabbitMQConfig(
            exchanges: ['test' => ['type' => 'direct'], 'dlx' => ['type' => 'direct']],
            queues: [
                'test.queue' => [
                    'bindings' => [['exchange' => 'test', 'routing_key' => 'key']],
                    'dead' => ['exchange' => 'dlx'],
                ],
            ],
        );

        [$consumer, $channel] = $this->createConsumer('test.queue', $config);
        $testMsg = $this->createAmqpMessage($channel);

        $this->setupConsumeLoop($channel, $testMsg);

        /** @var string|null $receivedReason */
        $receivedReason = null;
        /** @var Message|null $receivedMessage */
        $receivedMessage = null;

        $consumer->onDead(static function (Message $msg, string $reason) use (&$receivedReason, &$receivedMessage): void {
            $receivedMessage = $msg;
            $receivedReason = $reason;
        });

        $consumer->execute(static fn(Message $msg): TaskStatus => TaskStatus::DEAD);

        self::assertNotNull($receivedMessage);
        self::assertSame('Marked as dead by handler', $receivedReason);
    }

    #[Test]
    public function maxRetriesExceededSendsToDeadLetterInsteadOfRetry(): void
    {
        $config = new RabbitMQConfig(
            exchanges: ['test' => ['type' => 'direct'], 'dlx' => ['type' => 'direct']],
            queues: [
                'test.queue' => [
                    'bindings' => [['exchange' => 'test', 'routing_key' => 'key']],
                    'dead' => ['exchange' => 'dlx', 'routing_key' => 'dead.key'],
                ],
            ],
        );

        [$consumer, $channel] = $this->createConsumer('test.queue', $config);

        $testMsg = $this->createAmqpMessage($channel, 'body', [
            'message_id' => 'retry-exceeded',
            'application_headers' => new AMQPTable([
                'x-retries-max' => 2,
                'x-death' => [
                    ['queue' => 'test.queue', 'count' => 2],
                ],
            ]),
        ]);

        $this->setupConsumeLoop($channel, $testMsg);

        // Should ack (dead letter path), not nack (retry path)
        $channel->expects(self::once())
            ->method('basic_ack')
            ->with('tag-1');

        $channel->expects(self::never())
            ->method('basic_nack');

        $channel->expects(self::once())
            ->method('basic_publish');

        $consumer->execute(static fn(Message $msg): TaskStatus => TaskStatus::RETRY);
    }

    #[Test]
    public function deadLetterMessageContainsFailedQueueHeader(): void
    {
        $config = new RabbitMQConfig(
            exchanges: ['test' => ['type' => 'direct'], 'dlx' => ['type' => 'direct']],
            queues: [
                'test.queue' => [
                    'bindings' => [['exchange' => 'test', 'routing_key' => 'key']],
                    'dead' => ['exchange' => 'dlx', 'routing_key' => 'dead.key'],
                ],
            ],
        );

        [$consumer, $channel] = $this->createConsumer('test.queue', $config);
        $testMsg = $this->createAmqpMessage($channel);

        $this->setupConsumeLoop($channel, $testMsg);

        /** @var AMQPMessage|null $deadMsg */
        $deadMsg = null;

        $channel->expects(self::once())
            ->method('basic_publish')
            ->willReturnCallback(static function (AMQPMessage $msg) use (&$deadMsg): void {
                $deadMsg = $msg;
            });

        $consumer->execute(static fn(Message $msg): TaskStatus => TaskStatus::DEAD);

        self::assertNotNull($deadMsg);

        /** @var AMQPTable $headers */
        $headers = $deadMsg->get('application_headers');
        /** @var array<string, mixed> $data */
        $data = $headers->getNativeData();

        self::assertSame('test.queue', $data['x-failed-queue']);
        self::assertArrayHasKey('x-failed-timestamp', $data);
    }

    #[Test]
    public function deadLetterPreservesMessageIdAndContentType(): void
    {
        $config = new RabbitMQConfig(
            exchanges: ['test' => ['type' => 'direct'], 'dlx' => ['type' => 'direct']],
            queues: [
                'test.queue' => [
                    'bindings' => [['exchange' => 'test', 'routing_key' => 'key']],
                    'dead' => ['exchange' => 'dlx'],
                ],
            ],
        );

        [$consumer, $channel] = $this->createConsumer('test.queue', $config);
        $testMsg = $this->createAmqpMessage($channel, '{"data":1}', [
            'message_id' => 'preserve-me',
            'content_type' => 'application/json',
        ]);

        $this->setupConsumeLoop($channel, $testMsg);

        /** @var AMQPMessage|null $deadMsg */
        $deadMsg = null;

        $channel->expects(self::once())
            ->method('basic_publish')
            ->willReturnCallback(static function (AMQPMessage $msg) use (&$deadMsg): void {
                $deadMsg = $msg;
            });

        $consumer->execute(static fn(Message $msg): TaskStatus => TaskStatus::DEAD);

        self::assertNotNull($deadMsg);
        self::assertSame('preserve-me', $deadMsg->get('message_id'));
        self::assertSame('application/json', $deadMsg->get('content_type'));
    }

    #[Test]
    public function deadWithNoDeadLetterExchangeDoesNotPublish(): void
    {
        [$consumer, $channel] = $this->createConsumer();
        $testMsg = $this->createAmqpMessage($channel);

        $this->setupConsumeLoop($channel, $testMsg);

        $channel->expects(self::once())
            ->method('basic_ack')
            ->with('tag-1');

        $channel->expects(self::never())
            ->method('basic_publish');

        $consumer->execute(static fn(Message $msg): TaskStatus => TaskStatus::DEAD);
    }

    #[Test]
    public function onDeadCallbackIsCalledEvenWithoutDeadLetterExchange(): void
    {
        [$consumer, $channel] = $this->createConsumer();
        $testMsg = $this->createAmqpMessage($channel);

        $this->setupConsumeLoop($channel, $testMsg);

        /** @var string|null $receivedReason */
        $receivedReason = null;

        $consumer->onDead(static function (Message $msg, string $reason) use (&$receivedReason): void {
            $receivedReason = $reason;
        });

        $consumer->execute(static fn(Message $msg): TaskStatus => TaskStatus::DEAD);

        self::assertSame('Marked as dead by handler', $receivedReason);
    }

    #[Test]
    public function retryCountIsExtractedFromXDeathHeader(): void
    {
        [$consumer, $channel] = $this->createConsumer();

        $testMsg = $this->createAmqpMessage($channel, 'body', [
            'message_id' => 'retry-test',
            'application_headers' => new AMQPTable([
                'x-death' => [
                    ['queue' => 'test.queue', 'count' => 3],
                ],
            ]),
        ]);

        $this->setupConsumeLoop($channel, $testMsg);

        /** @var int|null $receivedRetryCount */
        $receivedRetryCount = null;

        $consumer->onRetry(static function (Message $msg, int $count) use (&$receivedRetryCount): void {
            $receivedRetryCount = $count;
        });

        $consumer->execute(static fn(Message $msg): TaskStatus => TaskStatus::RETRY);

        // extractRetryCount returns 3, then onRetry is called with retryCount + 1 = 4
        self::assertSame(4, $receivedRetryCount);
    }

    #[Test]
    public function successCallbackReceivesCorrectMessageBody(): void
    {
        [$consumer, $channel] = $this->createConsumer();
        $testMsg = $this->createAmqpMessage($channel, 'specific body content');

        $this->setupConsumeLoop($channel, $testMsg);

        /** @var string|null $receivedBody */
        $receivedBody = null;

        $consumer->execute(static function (Message $msg) use (&$receivedBody): TaskStatus {
            $receivedBody = $msg->body();

            return TaskStatus::SUCCESS;
        });

        self::assertSame('specific body content', $receivedBody);
    }

    #[Test]
    public function corruptCompressedMessageIsDeadLetteredNotCrashed(): void
    {
        [$consumer, $channel, $connection] = $this->createConsumerWithDeadLetter('test.queue');
        $handler = null;

        $channel->method('basic_consume')
            ->willReturnCallback(function ($queue, $tag, $noLocal, $noAck, $exclusive, $nowait, $callback) use (&$handler): string {
                $handler = $callback;
                return 'tag';
            });

        $callCount = 0;
        $channel->method('is_consuming')
            ->willReturnCallback(function () use (&$callCount): bool {
                $callCount++;
                return $callCount <= 1;
            });

        $channel->method('wait')
            ->willReturnCallback(function () use (&$handler): void {
                if ($handler !== null) {
                    $corruptMsg = new AMQPMessage('not-valid-base64-gzip-data', [
                        'content_encoding' => 'gzip',
                        'message_id' => 'corrupt-msg',
                    ]);
                    $corruptMsg->setChannel($this->createMockChannel());
                    $corruptMsg->setDeliveryInfo('tag-corrupt', false, 'test-exchange', 'test.key');
                    $handler($corruptMsg);
                    $handler = null;
                }
            });

        $published = false;
        $channel->method('basic_publish')
            ->willReturnCallback(function () use (&$published): void {
                $published = true;
            });

        $consumer->execute(function (Message $msg): TaskStatus {
            return TaskStatus::SUCCESS;
        });

        self::assertTrue($published, 'Corrupt compressed message should be dead-lettered, not crash the consumer');
    }

    /**
     * @return array{Consumer, AMQPChannel&\PHPUnit\Framework\MockObject\MockObject, RabbitMQConnection}
     */
    private function createConsumerWithDeadLetter(string $queue): array
    {
        $channel = $this->createMock(AMQPChannel::class);

        $connection = new RabbitMQConnection(new RabbitMQConfig(
            exchanges: [
                'test' => ['type' => 'direct'],
                'dead' => ['type' => 'direct'],
            ],
            queues: [
                $queue => [
                    'bindings' => [['exchange' => 'test', 'routing_key' => 'key']],
                    'dead' => 'dead',
                ],
            ],
        ));

        $reflection = new ReflectionClass($connection);

        $connProp = $reflection->getProperty('connection');
        $mockConn = $this->createMock(AMQPStreamConnection::class);
        $mockConn->method('isConnected')->willReturn(true);
        $connProp->setValue($connection, $mockConn);

        $chanProp = $reflection->getProperty('channel');
        $chanProp->setValue($connection, $channel);

        $connectedProp = $reflection->getProperty('connected');
        $connectedProp->setValue($connection, true);

        $topology = new TopologyManager(new RabbitMQConfig(
            exchanges: [
                'test' => ['type' => 'direct'],
                'dead' => ['type' => 'direct'],
            ],
            queues: [
                $queue => [
                    'bindings' => [['exchange' => 'test', 'routing_key' => 'key']],
                    'dead' => 'dead',
                ],
            ],
        ));

        $consumer = new Consumer($queue, $connection, $topology, new NullLogger());

        return [$consumer, $channel, $connection];
    }

    private function createMockChannel(): AMQPChannel
    {
        $mock = $this->createMock(AMQPChannel::class);
        $mock->method('basic_ack')->willReturn(null);

        return $mock;
    }
}
