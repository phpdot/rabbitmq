<?php

declare(strict_types=1);

namespace PHPdot\RabbitMQ\Tests\Integration;

use PhpAmqpLib\Message\AMQPMessage;
use PHPdot\RabbitMQ\Config\RabbitMQConfig;
use PHPdot\RabbitMQ\RabbitMQConnection;
use PHPdot\RabbitMQ\Topology\TopologyManager;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

#[Group('integration')]
final class RetryDeadLetterTest extends TestCase
{
    private RabbitMQConnection $connection;
    private string $exchangeName;
    private string $queueName;
    private string $deadExchange;
    private string $deadQueue;

    protected function setUp(): void
    {
        try {
            (new RabbitMQConnection(new RabbitMQConfig(
                host: getenv('RABBITMQ_HOST') ?: 'localhost',
                port: (int) (getenv('RABBITMQ_PORT') ?: 5672),
                username: getenv('RABBITMQ_USER') ?: 'guest',
                password: getenv('RABBITMQ_PASS') ?: 'guest',
                timeoutMs: 500,
                maxRetries: 1,
            )))->connect()->close();
        } catch (\Throwable $e) {
            $this->markTestSkipped('RabbitMQ is not available: ' . $e->getMessage());
        }

        $suffix = uniqid();
        $this->exchangeName = 'test.retry.exchange.' . $suffix;
        $this->queueName = 'test.retry.queue.' . $suffix;
        $this->deadExchange = 'test.dead.exchange.' . $suffix;
        $this->deadQueue = 'test.dead.queue.' . $suffix;

        $this->connection = new RabbitMQConnection(new RabbitMQConfig(
            host: getenv('RABBITMQ_HOST') ?: 'localhost',
            port: (int) (getenv('RABBITMQ_PORT') ?: 5672),
            username: getenv('RABBITMQ_USER') ?: 'guest',
            password: getenv('RABBITMQ_PASS') ?: 'guest',
            exchanges: [
                $this->exchangeName => ['type' => 'direct', 'durable' => true, 'auto_delete' => true],
                $this->deadExchange => ['type' => 'direct', 'durable' => true, 'auto_delete' => true],
            ],
            queues: [
                $this->queueName => [
                    'bindings' => [['exchange' => $this->exchangeName, 'routing_key' => 'test.key']],
                    'durable' => true,
                    'auto_delete' => true,
                    'retry' => ['enable' => true, 'delay_ms' => 100],
                    'dead' => ['exchange' => $this->deadExchange, 'routing_key' => 'dead.key'],
                ],
                $this->deadQueue => [
                    'bindings' => [['exchange' => $this->deadExchange, 'routing_key' => 'dead.key']],
                    'durable' => true,
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
            $channel->queue_delete($this->queueName . '.retry');
            $channel->queue_delete($this->deadQueue);
            $channel->exchange_delete($this->exchangeName);
            $channel->exchange_delete($this->queueName . '.retry.exchange');
            $channel->exchange_delete($this->deadExchange);
        } catch (\Throwable) {
        }

        $this->connection->close();
    }

    #[Test]
    public function retryInfrastructureCreatedByTopologyManager(): void
    {
        $config = new RabbitMQConfig(
            host: getenv('RABBITMQ_HOST') ?: 'localhost',
            port: (int) (getenv('RABBITMQ_PORT') ?: 5672),
            exchanges: [
                $this->exchangeName => ['type' => 'direct', 'durable' => true, 'auto_delete' => true],
                $this->deadExchange => ['type' => 'direct', 'durable' => true, 'auto_delete' => true],
            ],
            queues: [
                $this->queueName => [
                    'bindings' => [['exchange' => $this->exchangeName, 'routing_key' => 'test.key']],
                    'durable' => true,
                    'auto_delete' => true,
                    'retry' => ['enable' => true, 'delay_ms' => 100],
                    'dead' => ['exchange' => $this->deadExchange, 'routing_key' => 'dead.key'],
                ],
                $this->deadQueue => [
                    'bindings' => [['exchange' => $this->deadExchange, 'routing_key' => 'dead.key']],
                    'durable' => true,
                    'auto_delete' => true,
                ],
            ],
        );

        $topology = new TopologyManager($config, new NullLogger());
        $channel = $this->connection->getChannel();

        $topology->prepareForConsume($this->queueName, $channel);

        $retryQueue = $this->queueName . '.retry';

        /** @var array{string, int, int} $result */
        $result = $channel->queue_declare($retryQueue, true);

        self::assertIsArray($result);
    }

    #[Test]
    public function nackWithDlxRoutesToRetryQueue(): void
    {
        $config = new RabbitMQConfig(
            host: getenv('RABBITMQ_HOST') ?: 'localhost',
            port: (int) (getenv('RABBITMQ_PORT') ?: 5672),
            exchanges: [
                $this->exchangeName => ['type' => 'direct', 'durable' => true, 'auto_delete' => true],
                $this->deadExchange => ['type' => 'direct', 'durable' => true, 'auto_delete' => true],
            ],
            queues: [
                $this->queueName => [
                    'bindings' => [['exchange' => $this->exchangeName, 'routing_key' => 'test.key']],
                    'durable' => true,
                    'auto_delete' => true,
                    'retry' => ['enable' => true, 'delay_ms' => 100],
                    'dead' => ['exchange' => $this->deadExchange, 'routing_key' => 'dead.key'],
                ],
                $this->deadQueue => [
                    'bindings' => [['exchange' => $this->deadExchange, 'routing_key' => 'dead.key']],
                    'durable' => true,
                    'auto_delete' => true,
                ],
            ],
        );

        $topology = new TopologyManager($config, new NullLogger());
        $channel = $this->connection->getChannel();

        $topology->prepareForConsume($this->queueName, $channel);
        $topology->prepareForConsume($this->deadQueue, $channel);
        $topology->prepareForPublish($this->exchangeName, $channel);

        $this->connection->message('retry-me')
            ->retry(3)
            ->publish($this->exchangeName, 'test.key');

        usleep(100000);

        /** @var AMQPMessage|null $amqpMsg */
        $amqpMsg = $channel->basic_get($this->queueName, false);
        self::assertNotNull($amqpMsg);

        $amqpMsg->nack(false);

        usleep(500000);

        /** @var AMQPMessage|null $redelivered */
        $redelivered = $channel->basic_get($this->queueName, true);

        self::assertNotNull($redelivered, 'Message should have been redelivered from retry queue after TTL');
        self::assertSame('retry-me', $redelivered->getBody());
    }

    #[Test]
    public function topologyManagerReportsDeadLetterConfig(): void
    {
        $config = new RabbitMQConfig(
            queues: [
                'myqueue' => [
                    'bindings' => [],
                    'dead' => ['exchange' => 'dead.exchange', 'routing_key' => 'dead.key'],
                ],
            ],
        );

        $topology = new TopologyManager($config, new NullLogger());

        self::assertSame('dead.exchange', $topology->getDeadLetterExchange('myqueue'));
        self::assertSame('dead.key', $topology->getDeadLetterRoutingKey('myqueue'));
    }

    #[Test]
    public function topologyManagerReportsStringDeadConfig(): void
    {
        $config = new RabbitMQConfig(
            queues: [
                'myqueue' => [
                    'bindings' => [],
                    'dead' => 'failed-exchange',
                ],
            ],
        );

        $topology = new TopologyManager($config, new NullLogger());

        self::assertSame('failed-exchange', $topology->getDeadLetterExchange('myqueue'));
        self::assertSame('', $topology->getDeadLetterRoutingKey('myqueue'));
    }

    #[Test]
    public function topologyManagerReportsNullWhenNoDeadConfig(): void
    {
        $config = new RabbitMQConfig(
            queues: [
                'myqueue' => [
                    'bindings' => [],
                ],
            ],
        );

        $topology = new TopologyManager($config, new NullLogger());

        self::assertNull($topology->getDeadLetterExchange('myqueue'));
    }
}
