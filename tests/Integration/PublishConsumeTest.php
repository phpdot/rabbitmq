<?php

declare(strict_types=1);

namespace PHPdot\RabbitMQ\Tests\Integration;

use PhpAmqpLib\Message\AMQPMessage;
use PHPdot\RabbitMQ\Config\RabbitMQConfig;
use PHPdot\RabbitMQ\RabbitMQConnection;
use PHPdot\RabbitMQ\Message;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

#[Group('integration')]
final class PublishConsumeTest extends TestCase
{
    private RabbitMQConnection $connection;
    private string $exchangeName;
    private string $queueName;

    protected function setUp(): void
    {
        try {
            (new RabbitMQConnection(new RabbitMQConfig(
                host: 'localhost',
                port: 5672,
                username: 'guest',
                password: 'guest',
                timeoutMs: 500,
                maxRetries: 1,
            )))->connect()->close();
        } catch (\Throwable $e) {
            $this->markTestSkipped('RabbitMQ is not available: ' . $e->getMessage());
        }

        $this->exchangeName = 'test.exchange.' . uniqid();
        $this->queueName = 'test.queue.' . uniqid();

        $this->connection = new RabbitMQConnection(new RabbitMQConfig(
            host: 'localhost',
            port: 5672,
            username: 'guest',
            password: 'guest',
            exchanges: [
                $this->exchangeName => ['type' => 'direct', 'durable' => false, 'auto_delete' => true],
            ],
            queues: [
                $this->queueName => [
                    'bindings' => [['exchange' => $this->exchangeName, 'routing_key' => 'test.key']],
                    'durable' => false,
                    'auto_delete' => true,
                ],
            ],
        ));
    }

    protected function tearDown(): void
    {
        if (!isset($this->connection)) {
            return;
        }

        try {
            $channel = $this->connection->getChannel();
            $channel->queue_delete($this->queueName);
            $channel->exchange_delete($this->exchangeName);
        } catch (\Throwable) {
        }

        $this->connection->close();
    }

    private function getOneMessage(): ?Message
    {
        $channel = $this->connection->getChannel();

        usleep(100000);

        /** @var AMQPMessage|null $amqpMsg */
        $amqpMsg = $channel->basic_get($this->queueName, true);

        if ($amqpMsg === null) {
            return null;
        }

        return Message::fromAMQP($amqpMsg, $this->queueName);
    }

    #[Test]
    public function publishAndConsumeSimpleMessage(): void
    {
        $this->connection->message('{"action":"test"}')
            ->publish($this->exchangeName, 'test.key');

        $msg = $this->getOneMessage();

        self::assertNotNull($msg);
        self::assertSame('{"action":"test"}', $msg->body());
        self::assertSame($this->queueName, $msg->queue());
    }

    #[Test]
    public function publishAndConsumeWithHeaders(): void
    {
        $this->connection->message('header-test')
            ->header(['traceparent' => '00-abc-def-01'])
            ->header(['x-custom' => 'value'])
            ->publish($this->exchangeName, 'test.key');

        $msg = $this->getOneMessage();

        self::assertNotNull($msg);
        self::assertSame('00-abc-def-01', $msg->header('traceparent'));
        self::assertSame('value', $msg->header('x-custom'));
    }

    #[Test]
    public function publishAndConsumeCompressed(): void
    {
        $original = str_repeat('hello world ', 1000);

        $this->connection->message($original)
            ->compress()
            ->publish($this->exchangeName, 'test.key');

        $channel = $this->connection->getChannel();
        usleep(100000);

        /** @var AMQPMessage|null $amqpMsg */
        $amqpMsg = $channel->basic_get($this->queueName, true);

        self::assertNotNull($amqpMsg);

        $body = $amqpMsg->getBody();
        $decoded = base64_decode($body, true);
        self::assertIsString($decoded);

        $decompressed = gzuncompress($decoded);
        self::assertIsString($decompressed);
        self::assertSame($original, $decompressed);
    }

    #[Test]
    public function publishWithPriority(): void
    {
        $this->connection->message('priority-test')
            ->priority(5)
            ->publish($this->exchangeName, 'test.key');

        $msg = $this->getOneMessage();

        self::assertNotNull($msg);
        self::assertSame(5, $msg->priority());
    }

    #[Test]
    public function messageIdIsUuidv7(): void
    {
        $this->connection->message('id-test')
            ->publish($this->exchangeName, 'test.key');

        $msg = $this->getOneMessage();

        self::assertNotNull($msg);
        self::assertNotEmpty($msg->messageId());
        self::assertMatchesRegularExpression(
            '/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/',
            $msg->messageId(),
        );
    }

    #[Test]
    public function customMessageId(): void
    {
        $this->connection->message('custom-id')
            ->messageId('my-custom-id-123')
            ->publish($this->exchangeName, 'test.key');

        $msg = $this->getOneMessage();

        self::assertNotNull($msg);
        self::assertSame('my-custom-id-123', $msg->messageId());
    }

    #[Test]
    public function connectionAutoConnects(): void
    {
        self::assertFalse($this->connection->isConnected());

        $this->connection->message('auto-connect')
            ->publish($this->exchangeName, 'test.key');

        self::assertTrue($this->connection->isConnected());
    }

    #[Test]
    public function closeAndReconnect(): void
    {
        $this->connection->message('before-close')
            ->publish($this->exchangeName, 'test.key');

        self::assertTrue($this->connection->isConnected());

        $this->connection->close();
        self::assertFalse($this->connection->isConnected());

        $this->connection->message('after-reconnect')
            ->publish($this->exchangeName, 'test.key');

        self::assertTrue($this->connection->isConnected());

        $msg1 = $this->getOneMessage();
        $msg2 = $this->getOneMessage();

        self::assertNotNull($msg1);
        self::assertNotNull($msg2);
    }

    #[Test]
    public function multipleMessagesInOrder(): void
    {
        for ($i = 1; $i <= 5; $i++) {
            $this->connection->message("message-{$i}")
                ->publish($this->exchangeName, 'test.key');
        }

        usleep(100000);

        $channel = $this->connection->getChannel();
        $bodies = [];

        for ($i = 0; $i < 5; $i++) {
            /** @var AMQPMessage|null $amqpMsg */
            $amqpMsg = $channel->basic_get($this->queueName, true);

            if ($amqpMsg !== null) {
                $bodies[] = $amqpMsg->getBody();
            }
        }

        self::assertSame(['message-1', 'message-2', 'message-3', 'message-4', 'message-5'], $bodies);
    }

    #[Test]
    public function originalExchangeAndRoutingKeyHeaders(): void
    {
        $this->connection->message('origin-test')
            ->publish($this->exchangeName, 'test.key');

        $msg = $this->getOneMessage();

        self::assertNotNull($msg);
        self::assertSame($this->exchangeName, $msg->originalExchange());
        self::assertSame('test.key', $msg->originalRoutingKey());
    }

    #[Test]
    public function retryMaxHeaderSet(): void
    {
        $this->connection->message('retry-test')
            ->retry(5)
            ->publish($this->exchangeName, 'test.key');

        $msg = $this->getOneMessage();

        self::assertNotNull($msg);
        self::assertSame(5, $msg->maxRetries());
    }

    #[Test]
    public function emptyQueueReturnsNull(): void
    {
        $channel = $this->connection->getChannel();

        $this->connection->ensureConnected();

        $channel->exchange_declare($this->exchangeName, 'direct', false, false, true);
        $channel->queue_declare($this->queueName, false, false, false, true);
        $channel->queue_bind($this->queueName, $this->exchangeName, 'test.key');

        /** @var AMQPMessage|null $amqpMsg */
        $amqpMsg = $channel->basic_get($this->queueName, true);

        self::assertNull($amqpMsg);
    }
}
