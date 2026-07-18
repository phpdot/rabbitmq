<?php

declare(strict_types=1);

namespace PHPdot\RabbitMQ\Tests;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use PHPdot\RabbitMQ\Config\RabbitMQConfig;
use PHPdot\RabbitMQ\RabbitMQConnection;
use PHPdot\RabbitMQ\Exception\PublishException;
use PHPdot\RabbitMQ\Publisher;
use PHPdot\RabbitMQ\Topology\TopologyManager;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use ReflectionClass;

final class PublisherTest extends TestCase
{
    /**
     * Creates a Publisher with a real RabbitMQConnection whose internals are
     * wired to a mock channel via reflection (RabbitMQConnection is final).
     *
     * @return array{0: Publisher, 1: AMQPChannel&\PHPUnit\Framework\MockObject\MockObject}
     */
    private function createPublisher(string $content): array
    {
        $channel = $this->createMock(AMQPChannel::class);

        $channel->method('exchange_declare')->willReturn(null);
        $channel->method('queue_declare')->willReturn(null);
        $channel->method('queue_bind')->willReturn(null);

        $config = new RabbitMQConfig(
            exchanges: ['test' => ['type' => 'direct']],
            queues: ['test.queue' => ['bindings' => [['exchange' => 'test', 'routing_key' => 'key']]]],
        );

        $connection = $this->buildFakeConnection($config, $channel);

        $topology = new TopologyManager($config);
        $publisher = new Publisher($content, $connection, $topology, new NullLogger());

        return [$publisher, $channel];
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
     * Helper: publish and return the captured AMQPMessage.
     *
     * @return array{0: bool, 1: AMQPMessage}
     */
    private function publishAndCapture(string $content, string $exchange = 'test', string $routingKey = 'key'): array
    {
        [$publisher, $channel] = $this->createPublisher($content);

        /** @var AMQPMessage|null $captured */
        $captured = null;

        $channel->expects(self::once())
            ->method('basic_publish')
            ->willReturnCallback(static function (AMQPMessage $msg) use (&$captured): void {
                $captured = $msg;
            });

        $result = $publisher->publish($exchange, $routingKey);

        self::assertNotNull($captured);

        return [$result, $captured];
    }

    #[Test]
    public function publishCallsBasicPublishOnChannel(): void
    {
        [$publisher, $channel] = $this->createPublisher('hello');

        $channel->expects(self::once())
            ->method('basic_publish');

        $publisher->publish('test', 'key');
    }

    #[Test]
    public function publishCapturesMessageWithCorrectBody(): void
    {
        [, $msg] = $this->publishAndCapture('hello world');

        self::assertSame('hello world', $msg->getBody());
    }

    #[Test]
    public function publishSetsMessageIdAsUuidV7Format(): void
    {
        [, $msg] = $this->publishAndCapture('body');

        /** @var string $messageId */
        $messageId = $msg->get('message_id');

        // UUIDv7 format: 8-4-4-4-12 hex chars, version nibble = 7
        self::assertMatchesRegularExpression(
            '/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/',
            $messageId,
        );
    }

    #[Test]
    public function publishSetsTimestamp(): void
    {
        $before = time();
        [, $msg] = $this->publishAndCapture('body');
        $after = time();

        /** @var int $timestamp */
        $timestamp = $msg->get('timestamp');

        self::assertGreaterThanOrEqual($before, $timestamp);
        self::assertLessThanOrEqual($after, $timestamp);
    }

    #[Test]
    public function publishSetsDeliveryModePersistent(): void
    {
        [, $msg] = $this->publishAndCapture('body');

        self::assertSame(2, $msg->get('delivery_mode'));
    }

    #[Test]
    public function publishAutoDetectsJsonContentType(): void
    {
        [, $msg] = $this->publishAndCapture('{"key":"value"}');

        self::assertSame('application/json', $msg->get('content_type'));
    }

    #[Test]
    public function publishAutoDetectsTextPlainContentType(): void
    {
        [, $msg] = $this->publishAndCapture('just plain text');

        self::assertSame('text/plain', $msg->get('content_type'));
    }

    #[Test]
    public function headerAddsHeadersToPublishedMessage(): void
    {
        [$publisher, $channel] = $this->createPublisher('body');

        /** @var AMQPMessage|null $captured */
        $captured = null;

        $channel->expects(self::once())
            ->method('basic_publish')
            ->willReturnCallback(static function (AMQPMessage $msg) use (&$captured): void {
                $captured = $msg;
            });

        $publisher->header(['x-custom' => 'value'])->publish('test', 'key');

        self::assertNotNull($captured);

        /** @var AMQPTable $headers */
        $headers = $captured->get('application_headers');
        /** @var array<string, mixed> $data */
        $data = $headers->getNativeData();

        self::assertSame('value', $data['x-custom']);
    }

    #[Test]
    public function retrySetsMaxRetriesHeader(): void
    {
        [$publisher, $channel] = $this->createPublisher('body');

        /** @var AMQPMessage|null $captured */
        $captured = null;

        $channel->expects(self::once())
            ->method('basic_publish')
            ->willReturnCallback(static function (AMQPMessage $msg) use (&$captured): void {
                $captured = $msg;
            });

        $publisher->retry(5)->publish('test', 'key');

        self::assertNotNull($captured);

        /** @var AMQPTable $headers */
        $headers = $captured->get('application_headers');
        /** @var array<string, mixed> $data */
        $data = $headers->getNativeData();

        self::assertSame(5, $data['x-retries-max']);
    }

    #[Test]
    public function prioritySetsPriorityOnMessage(): void
    {
        [$publisher, $channel] = $this->createPublisher('body');

        /** @var AMQPMessage|null $captured */
        $captured = null;

        $channel->expects(self::once())
            ->method('basic_publish')
            ->willReturnCallback(static function (AMQPMessage $msg) use (&$captured): void {
                $captured = $msg;
            });

        $publisher->priority(7)->publish('test', 'key');

        self::assertNotNull($captured);
        self::assertSame(7, $captured->get('priority'));
    }

    #[Test]
    public function priorityThrowsForNegativeValue(): void
    {
        [$publisher, $channel] = $this->createPublisher('body');

        $channel->expects(self::never())
            ->method('basic_publish');

        $this->expectException(PublishException::class);
        $publisher->priority(-1);
    }

    #[Test]
    public function priorityThrowsForValueAboveTen(): void
    {
        [$publisher, $channel] = $this->createPublisher('body');

        $channel->expects(self::never())
            ->method('basic_publish');

        $this->expectException(PublishException::class);
        $publisher->priority(11);
    }

    #[Test]
    public function compressCompressesBodyWithGzip(): void
    {
        $original = 'this is the original body content';
        [$publisher, $channel] = $this->createPublisher($original);

        /** @var AMQPMessage|null $captured */
        $captured = null;

        $channel->expects(self::once())
            ->method('basic_publish')
            ->willReturnCallback(static function (AMQPMessage $msg) use (&$captured): void {
                $captured = $msg;
            });

        $publisher->compress()->publish('test', 'key');

        self::assertNotNull($captured);

        $decoded = base64_decode($captured->getBody(), true);
        self::assertNotFalse($decoded);

        $decompressed = gzuncompress($decoded);
        self::assertSame($original, $decompressed);
    }

    #[Test]
    public function compressSetsContentEncodingGzip(): void
    {
        [$publisher, $channel] = $this->createPublisher('body');

        /** @var AMQPMessage|null $captured */
        $captured = null;

        $channel->expects(self::once())
            ->method('basic_publish')
            ->willReturnCallback(static function (AMQPMessage $msg) use (&$captured): void {
                $captured = $msg;
            });

        $publisher->compress()->publish('test', 'key');

        self::assertNotNull($captured);
        self::assertSame('gzip', $captured->get('content_encoding'));
    }

    #[Test]
    public function messageIdOverridesAutoGeneratedId(): void
    {
        [$publisher, $channel] = $this->createPublisher('body');

        /** @var AMQPMessage|null $captured */
        $captured = null;

        $channel->expects(self::once())
            ->method('basic_publish')
            ->willReturnCallback(static function (AMQPMessage $msg) use (&$captured): void {
                $captured = $msg;
            });

        $publisher->messageId('custom-id-123')->publish('test', 'key');

        self::assertNotNull($captured);
        self::assertSame('custom-id-123', $captured->get('message_id'));
    }

    #[Test]
    public function appIdSetsAppIdOnMessage(): void
    {
        [$publisher, $channel] = $this->createPublisher('body');

        /** @var AMQPMessage|null $captured */
        $captured = null;

        $channel->expects(self::once())
            ->method('basic_publish')
            ->willReturnCallback(static function (AMQPMessage $msg) use (&$captured): void {
                $captured = $msg;
            });

        $publisher->appId('my-app')->publish('test', 'key');

        self::assertNotNull($captured);
        self::assertSame('my-app', $captured->get('app_id'));
    }

    #[Test]
    public function publishSetsOriginalExchangeHeader(): void
    {
        [, $msg] = $this->publishAndCapture('body', 'test', 'key');

        /** @var AMQPTable $headers */
        $headers = $msg->get('application_headers');
        /** @var array<string, mixed> $data */
        $data = $headers->getNativeData();

        self::assertSame('test', $data['x-original-exchange']);
    }

    #[Test]
    public function publishSetsOriginalRoutingKeyHeader(): void
    {
        [, $msg] = $this->publishAndCapture('body', 'test', 'my.routing.key');

        /** @var AMQPTable $headers */
        $headers = $msg->get('application_headers');
        /** @var array<string, mixed> $data */
        $data = $headers->getNativeData();

        self::assertSame('my.routing.key', $data['x-original-routing-key']);
    }

    #[Test]
    public function publishReturnsTrueOnSuccess(): void
    {
        [$result] = $this->publishAndCapture('body');

        self::assertTrue($result);
    }
}
