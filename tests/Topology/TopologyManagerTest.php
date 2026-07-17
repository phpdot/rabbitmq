<?php

declare(strict_types=1);

namespace PHPdot\RabbitMQ\Tests\Topology;

use PHPdot\RabbitMQ\Config\RabbitMQConfig;
use PHPdot\RabbitMQ\Exception\ConsumeException;
use PHPdot\RabbitMQ\Exception\PublishException;
use PHPdot\RabbitMQ\Topology\TopologyManager;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

final class TopologyManagerTest extends TestCase
{
    #[Test]
    public function constructAcceptsConnectionConfig(): void
    {
        $config = new RabbitMQConfig();
        $manager = new TopologyManager($config);

        self::assertInstanceOf(TopologyManager::class, $manager);
    }

    #[Test]
    public function resetClearsDeclaredCache(): void
    {
        $config = new RabbitMQConfig();
        $manager = new TopologyManager($config);

        // reset() should not throw and should be callable multiple times
        $manager->reset();
        $manager->reset();

        self::assertInstanceOf(TopologyManager::class, $manager);
    }

    #[Test]
    public function prepareForPublishThrowsForUndefinedExchange(): void
    {
        $config = new RabbitMQConfig(
            exchanges: [],
        );
        $manager = new TopologyManager($config);

        $this->expectException(PublishException::class);
        $this->expectExceptionMessageMatches('/not-configured/');

        // We need a channel mock, but since the exception is thrown before any channel call,
        // we can use a minimal mock
        $channel = $this->createMock(\PhpAmqpLib\Channel\AMQPChannel::class);
        $manager->prepareForPublish('not-configured', $channel);
    }

    #[Test]
    public function prepareForConsumeThrowsForUndefinedQueue(): void
    {
        $config = new RabbitMQConfig(
            queues: [],
        );
        $manager = new TopologyManager($config);

        $this->expectException(ConsumeException::class);
        $this->expectExceptionMessageMatches('/not-configured/');

        $channel = $this->createMock(\PhpAmqpLib\Channel\AMQPChannel::class);
        $manager->prepareForConsume('not-configured', $channel);
    }

    #[Test]
    public function prepareForPublishDeclaresExchangeOnChannel(): void
    {
        $config = new RabbitMQConfig(
            exchanges: [
                'events' => ['type' => 'topic', 'durable' => true],
            ],
        );
        $manager = new TopologyManager($config);

        $channel = $this->createMock(\PhpAmqpLib\Channel\AMQPChannel::class);
        $channel->expects(self::once())
            ->method('exchange_declare')
            ->with('events', 'topic', false, true, false);

        $manager->prepareForPublish('events', $channel);
    }

    #[Test]
    public function prepareForPublishSkipsWhenAlreadyDeclared(): void
    {
        $config = new RabbitMQConfig(
            exchanges: [
                'events' => ['type' => 'topic', 'durable' => true],
            ],
        );
        $manager = new TopologyManager($config);

        $channel = $this->createMock(\PhpAmqpLib\Channel\AMQPChannel::class);
        $channel->expects(self::once())
            ->method('exchange_declare');

        $manager->prepareForPublish('events', $channel);
        // Second call should skip declaration
        $manager->prepareForPublish('events', $channel);
    }

    #[Test]
    public function resetAllowsRedeclaration(): void
    {
        $config = new RabbitMQConfig(
            exchanges: [
                'events' => ['type' => 'topic', 'durable' => true],
            ],
        );
        $manager = new TopologyManager($config);

        $channel = $this->createMock(\PhpAmqpLib\Channel\AMQPChannel::class);
        $channel->expects(self::exactly(2))
            ->method('exchange_declare');

        $manager->prepareForPublish('events', $channel);
        $manager->reset();
        $manager->prepareForPublish('events', $channel);
    }

    #[Test]
    public function prepareForPublishDeclaresQueuesBoundToExchange(): void
    {
        $config = new RabbitMQConfig(
            exchanges: [
                'events' => ['type' => 'topic'],
            ],
            queues: [
                'notifications' => [
                    'durable' => true,
                    'bindings' => [
                        ['exchange' => 'events', 'routing_key' => 'user.created'],
                    ],
                ],
            ],
        );
        $manager = new TopologyManager($config);

        $channel = $this->createMock(\PhpAmqpLib\Channel\AMQPChannel::class);
        $channel->expects(self::once())
            ->method('exchange_declare');
        $channel->expects(self::once())
            ->method('queue_declare');
        $channel->expects(self::once())
            ->method('queue_bind')
            ->with('notifications', 'events', 'user.created');

        $manager->prepareForPublish('events', $channel);
    }

    #[Test]
    public function prepareForConsumeDeclaresQueueAndBindings(): void
    {
        $config = new RabbitMQConfig(
            exchanges: [
                'events' => ['type' => 'topic'],
            ],
            queues: [
                'notifications' => [
                    'durable' => true,
                    'bindings' => [
                        ['exchange' => 'events', 'routing_key' => 'user.#'],
                    ],
                ],
            ],
        );
        $manager = new TopologyManager($config);

        $channel = $this->createMock(\PhpAmqpLib\Channel\AMQPChannel::class);
        $channel->expects(self::once())
            ->method('queue_declare');
        $channel->expects(self::once())
            ->method('exchange_declare');
        $channel->expects(self::once())
            ->method('queue_bind')
            ->with('notifications', 'events', 'user.#');

        $manager->prepareForConsume('notifications', $channel);
    }

    #[Test]
    public function prepareForConsumeSkipsWhenAlreadyDeclared(): void
    {
        $config = new RabbitMQConfig(
            exchanges: [
                'events' => ['type' => 'topic'],
            ],
            queues: [
                'notifications' => [
                    'durable' => true,
                    'bindings' => [
                        ['exchange' => 'events', 'routing_key' => 'user.#'],
                    ],
                ],
            ],
        );
        $manager = new TopologyManager($config);

        $channel = $this->createMock(\PhpAmqpLib\Channel\AMQPChannel::class);
        $channel->expects(self::once())
            ->method('queue_declare');

        $manager->prepareForConsume('notifications', $channel);
        // Second call should skip
        $manager->prepareForConsume('notifications', $channel);
    }
}
