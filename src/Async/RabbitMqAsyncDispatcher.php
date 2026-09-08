<?php

declare(strict_types=1);

/**
 * The queue-backed AsyncDispatcherInterface: publish means publish. The
 * event serializes into the message body (the consumer rebuilds it), the
 * event and handler classes ride as headers so routing and inspection need
 * no unserialize, and the listener's 0-10 priority maps straight onto the
 * broker's. The message persists (delivery mode 2) and survives the
 * request that published it.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\RabbitMQ\Async;

use PHPdot\Contracts\Event\AsyncDispatcherInterface;
use PHPdot\Contracts\Logs\TracerInterface;
use PHPdot\RabbitMQ\Config\RabbitMQConfig;
use PHPdot\RabbitMQ\Publisher;
use PHPdot\RabbitMQ\RabbitMQConnection;
use PHPdot\RabbitMQ\Topology\TopologyManager;

final class RabbitMqAsyncDispatcher implements AsyncDispatcherInterface
{
    /**
     * @param RabbitMQConnection $connection The live AMQP connection
     * @param string $exchange The exchange async events publish to (fanout: one shared pipe)
     * @param TracerInterface $tracer The observability boundary — spans and the queue channel
     * @param string $routingKey Empty for fanout — a routing dimension with nothing to route
     * @param int $maxRetries The cap the consumer enforces before dead-lettering; 0 leaves the retry cycle uncapped
     */
    public function __construct(
        private readonly RabbitMQConnection $connection,
        private readonly RabbitMQConfig $config,
        private readonly string $exchange,
        private readonly TracerInterface $tracer,
        private readonly string $routingKey = '',
        private readonly int $maxRetries = 3,
    ) {}

    public function publishAsync(object $event, string $handlerClass, int $priority = 0): void
    {
        (new Publisher(serialize($event), $this->connection, new TopologyManager($this->config, $this->tracer), $this->tracer))
            ->header([
                'x-event-class' => $event::class,
                'x-handler-class' => $handlerClass,
            ])
            ->retry($this->maxRetries)
            ->priority($priority)
            ->compress()
            ->publish($this->exchange, $this->routingKey);
    }
}
