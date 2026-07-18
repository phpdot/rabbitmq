<?php

declare(strict_types=1);

namespace PHPdot\RabbitMQ\Tests;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use PHPdot\RabbitMQ\Config\RabbitMQConfig;
use PHPdot\RabbitMQ\RabbitMQConnection;
use PHPdot\RabbitMQ\Enum\ReplayAction;
use PHPdot\RabbitMQ\Message;
use PHPdot\RabbitMQ\Replayer;
use PHPdot\RabbitMQ\Topology\TopologyManager;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use ReflectionClass;
use RuntimeException;

final class ReplayerTest extends TestCase
{
    private const string DLQ = 'test.queue.dead';

    /**
     * Creates a Replayer with a real RabbitMQConnection whose internals are
     * wired to a mock channel via reflection (RabbitMQConnection is final).
     *
     * @return array{0: Replayer, 1: AMQPChannel&\PHPUnit\Framework\MockObject\MockObject, 2: RabbitMQConnection}
     */
    private function createReplayer(?RabbitMQConfig $config = null): array
    {
        $channel = $this->createMock(AMQPChannel::class);

        $channel->method('exchange_declare')->willReturn(null);
        $channel->method('queue_declare')->willReturn(null);
        $channel->method('queue_bind')->willReturn(null);

        $config ??= new RabbitMQConfig(
            exchanges: ['test' => ['type' => 'direct'], 'dlx' => ['type' => 'direct']],
            queues: [
                self::DLQ => ['bindings' => [['exchange' => 'dlx', 'routing_key' => 'dead.key']]],
                'test.queue' => ['bindings' => [['exchange' => 'test', 'routing_key' => 'key']]],
            ],
        );

        $connection = $this->buildFakeConnection($config, $channel);
        $topology = new TopologyManager($config);
        $replayer = new Replayer(self::DLQ, $connection, $topology, new NullLogger());

        return [$replayer, $channel, $connection];
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

        $streamConn = $this->createStub(AMQPStreamConnection::class);
        $streamConn->method('isConnected')->willReturn(true);

        $refConn = $ref->getProperty('connection');
        $refConn->setValue($connection, $streamConn);

        return $connection;
    }

    /**
     * Creates an AMQPMessage with delivery_info pointing at the mock channel.
     *
     * @param array<string, mixed> $headers
     * @param array<string, mixed> $properties
     */
    private function createDlqMessage(
        AMQPChannel&\PHPUnit\Framework\MockObject\MockObject $channel,
        string $body = '{"data":"test"}',
        array $headers = [],
        array $properties = [],
        string $deliveryTag = 'tag-1',
        string $messageId = 'msg-001',
    ): AMQPMessage {
        $defaultHeaders = [
            'x-original-exchange' => 'test',
            'x-original-routing-key' => 'key',
            'x-failed-queue' => 'test.queue',
            'x-failed-reason' => 'Test failure',
            'x-failed-timestamp' => 1700000000,
        ];

        $mergedHeaders = array_merge($defaultHeaders, $headers);

        $msgProperties = array_merge([
            'message_id' => $messageId,
            'content_type' => 'application/json',
            'application_headers' => new AMQPTable($mergedHeaders),
        ], $properties);

        $msg = new AMQPMessage($body, $msgProperties);
        $msg->setChannel($channel);
        $msg->setDeliveryInfo($deliveryTag, false, 'dlx', 'dead.key');

        return $msg;
    }

    /**
     * Sets up basic_get to return messages from a list, then null.
     *
     * @param list<AMQPMessage> $messages
     */
    private function setupBasicGet(AMQPChannel&\PHPUnit\Framework\MockObject\MockObject $channel, array $messages): void
    {
        $index = 0;

        $channel->method('basic_get')
            ->willReturnCallback(function () use (&$messages, &$index): ?AMQPMessage {
                return $messages[$index++] ?? null;
            });
    }

    #[Test]
    public function replayAcksAndRepublishesToOriginalExchange(): void
    {
        [$replayer, $channel] = $this->createReplayer();
        $msg = $this->createDlqMessage($channel);
        $this->setupBasicGet($channel, [$msg]);

        $channel->expects(self::once())
            ->method('basic_ack')
            ->with('tag-1');

        $channel->expects(self::once())
            ->method('basic_publish');

        $result = $replayer->execute(static fn(Message $m): ReplayAction => ReplayAction::REPLAY);

        self::assertSame(1, $result->replayed);
        self::assertSame(0, $result->removed);
        self::assertSame(0, $result->skipped);
    }

    #[Test]
    public function replayPreservesOriginalMessageId(): void
    {
        [$replayer, $channel] = $this->createReplayer();
        $msg = $this->createDlqMessage($channel, messageId: 'preserve-this-id');
        $this->setupBasicGet($channel, [$msg]);

        /** @var AMQPMessage|null $captured */
        $captured = null;

        $channel->expects(self::once())
            ->method('basic_publish')
            ->willReturnCallback(static function (AMQPMessage $msg) use (&$captured): void {
                $captured = $msg;
            });

        $replayer->execute(static fn(Message $m): ReplayAction => ReplayAction::REPLAY);

        self::assertNotNull($captured);
        self::assertSame('preserve-this-id', $captured->get('message_id'));
    }

    #[Test]
    public function replayPreservesContentEncoding(): void
    {
        [$replayer, $channel] = $this->createReplayer();
        $msg = $this->createDlqMessage($channel, properties: [
            'content_encoding' => 'gzip',
        ]);
        $this->setupBasicGet($channel, [$msg]);

        /** @var AMQPMessage|null $captured */
        $captured = null;

        $channel->expects(self::once())
            ->method('basic_publish')
            ->willReturnCallback(static function (AMQPMessage $msg) use (&$captured): void {
                $captured = $msg;
            });

        $replayer->execute(static fn(Message $m): ReplayAction => ReplayAction::REPLAY);

        self::assertNotNull($captured);
        self::assertSame('gzip', $captured->get('content_encoding'));
    }

    #[Test]
    public function replayStripsFailedHeaders(): void
    {
        [$replayer, $channel] = $this->createReplayer();
        $msg = $this->createDlqMessage($channel, headers: [
            'x-death' => [['queue' => 'test.queue', 'count' => 1]],
        ]);
        $this->setupBasicGet($channel, [$msg]);

        /** @var AMQPMessage|null $captured */
        $captured = null;

        $channel->expects(self::once())
            ->method('basic_publish')
            ->willReturnCallback(static function (AMQPMessage $msg) use (&$captured): void {
                $captured = $msg;
            });

        $replayer->execute(static fn(Message $m): ReplayAction => ReplayAction::REPLAY);

        self::assertNotNull($captured);

        /** @var AMQPTable $headers */
        $headers = $captured->get('application_headers');
        /** @var array<string, mixed> $data */
        $data = $headers->getNativeData();

        self::assertArrayNotHasKey('x-failed-queue', $data);
        self::assertArrayNotHasKey('x-failed-reason', $data);
        self::assertArrayNotHasKey('x-failed-timestamp', $data);
        self::assertArrayNotHasKey('x-death', $data);
    }

    #[Test]
    public function replayAddsReplayedHeaders(): void
    {
        [$replayer, $channel] = $this->createReplayer();
        $msg = $this->createDlqMessage($channel);
        $this->setupBasicGet($channel, [$msg]);

        /** @var AMQPMessage|null $captured */
        $captured = null;

        $channel->expects(self::once())
            ->method('basic_publish')
            ->willReturnCallback(static function (AMQPMessage $msg) use (&$captured): void {
                $captured = $msg;
            });

        $before = time();
        $replayer->execute(static fn(Message $m): ReplayAction => ReplayAction::REPLAY);
        $after = time();

        self::assertNotNull($captured);

        /** @var AMQPTable $headers */
        $headers = $captured->get('application_headers');
        /** @var array<string, mixed> $data */
        $data = $headers->getNativeData();

        self::assertArrayHasKey('x-replayed-at', $data);
        self::assertArrayHasKey('x-replayed-from', $data);
        self::assertSame(self::DLQ, $data['x-replayed-from']);
        self::assertGreaterThanOrEqual($before, $data['x-replayed-at']);
        self::assertLessThanOrEqual($after, $data['x-replayed-at']);
    }

    #[Test]
    public function replayWithNoOriginalExchangeAcksWithoutRepublishing(): void
    {
        [$replayer, $channel] = $this->createReplayer();
        $msg = $this->createDlqMessage($channel, headers: [
            'x-original-exchange' => '',
            'x-original-routing-key' => '',
        ]);
        $this->setupBasicGet($channel, [$msg]);

        $channel->expects(self::once())
            ->method('basic_ack')
            ->with('tag-1');

        $channel->expects(self::never())
            ->method('basic_publish');

        $result = $replayer->execute(static fn(Message $m): ReplayAction => ReplayAction::REPLAY);

        self::assertSame(0, $result->replayed);
    }

    #[Test]
    public function removeAcksWithoutRepublishing(): void
    {
        [$replayer, $channel] = $this->createReplayer();
        $msg = $this->createDlqMessage($channel);
        $this->setupBasicGet($channel, [$msg]);

        $channel->expects(self::once())
            ->method('basic_ack')
            ->with('tag-1');

        $channel->expects(self::never())
            ->method('basic_publish');

        $result = $replayer->execute(static fn(Message $m): ReplayAction => ReplayAction::REMOVE);

        self::assertSame(1, $result->removed);
        self::assertSame(0, $result->replayed);
    }

    #[Test]
    public function skipNacksWithRequeueTrue(): void
    {
        [$replayer, $channel] = $this->createReplayer();
        $msg = $this->createDlqMessage($channel);
        $this->setupBasicGet($channel, [$msg]);

        $channel->expects(self::once())
            ->method('basic_nack')
            ->with('tag-1', false, true);

        $result = $replayer->execute(static fn(Message $m): ReplayAction => ReplayAction::SKIP);

        self::assertSame(1, $result->skipped);
        self::assertSame(0, $result->replayed);
        self::assertSame(0, $result->removed);
    }

    #[Test]
    public function limitStopsAfterNMessages(): void
    {
        [$replayer, $channel] = $this->createReplayer();

        $msg1 = $this->createDlqMessage($channel, deliveryTag: 'tag-1', messageId: 'msg-1');
        $msg2 = $this->createDlqMessage($channel, deliveryTag: 'tag-2', messageId: 'msg-2');
        $msg3 = $this->createDlqMessage($channel, deliveryTag: 'tag-3', messageId: 'msg-3');

        $this->setupBasicGet($channel, [$msg1, $msg2, $msg3]);

        $channel->expects(self::exactly(2))
            ->method('basic_ack');

        $channel->expects(self::never())
            ->method('basic_publish');

        $result = $replayer->limit(2)->execute(static fn(Message $m): ReplayAction => ReplayAction::REMOVE);

        self::assertSame(2, $result->removed);
        self::assertSame(2, $result->total);
    }

    #[Test]
    public function emptyQueueReturnsZeroCounts(): void
    {
        [$replayer, $channel] = $this->createReplayer();
        $this->setupBasicGet($channel, []);

        $channel->expects(self::never())
            ->method('basic_ack');

        $channel->expects(self::never())
            ->method('basic_publish');

        $result = $replayer->execute(static fn(Message $m): ReplayAction => ReplayAction::REPLAY);

        self::assertSame(0, $result->replayed);
        self::assertSame(0, $result->removed);
        self::assertSame(0, $result->skipped);
        self::assertSame(0, $result->total);
    }

    #[Test]
    public function callbackExceptionNacksAndRethrows(): void
    {
        [$replayer, $channel] = $this->createReplayer();
        $msg = $this->createDlqMessage($channel);
        $this->setupBasicGet($channel, [$msg]);

        $channel->expects(self::once())
            ->method('basic_nack')
            ->with('tag-1', false, true);

        $channel->expects(self::never())
            ->method('basic_ack');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Callback failed');

        $replayer->execute(static function (Message $m): ReplayAction {
            throw new RuntimeException('Callback failed');
        });
    }

    #[Test]
    public function resultContainsCorrectCountsForMixedActions(): void
    {
        [$replayer, $channel] = $this->createReplayer();

        $msg1 = $this->createDlqMessage($channel, deliveryTag: 'tag-1', messageId: 'msg-1');
        $msg2 = $this->createDlqMessage($channel, deliveryTag: 'tag-2', messageId: 'msg-2');
        $msg3 = $this->createDlqMessage($channel, deliveryTag: 'tag-3', messageId: 'msg-3');

        $this->setupBasicGet($channel, [$msg1, $msg2, $msg3]);

        $callIndex = 0;
        $actions = [ReplayAction::REPLAY, ReplayAction::REMOVE, ReplayAction::SKIP];

        $channel->expects(self::once())
            ->method('basic_publish')
            ->willReturn(null);

        $result = $replayer->execute(static function (Message $m) use (&$callIndex, $actions): ReplayAction {
            return $actions[$callIndex++];
        });

        self::assertSame(1, $result->replayed);
        self::assertSame(1, $result->removed);
        self::assertSame(1, $result->skipped);
        self::assertSame(3, $result->total);
    }

    #[Test]
    public function connectionReplayReturnsReplayerInstance(): void
    {
        $config = new RabbitMQConfig(
            exchanges: ['test' => ['type' => 'direct']],
            queues: [self::DLQ => ['bindings' => [['exchange' => 'test', 'routing_key' => 'key']]]],
        );

        $connection = new RabbitMQConnection($config, new NullLogger());

        $replayer = $connection->replay(self::DLQ);

        self::assertInstanceOf(Replayer::class, $replayer);
    }
}
