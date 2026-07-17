<?php

declare(strict_types=1);

/**
 * Publisher
 *
 * Fluent message builder for publishing messages to RabbitMQ exchanges.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\RabbitMQ;

use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use PHPdot\RabbitMQ\Exception\PublishException;
use PHPdot\RabbitMQ\Topology\TopologyManager;
use Psr\Log\LoggerInterface;
use Throwable;

final class Publisher
{
    /**
     * @var array<string, mixed>
     */
    private array $headers = [];

    /**
     * @var array<string, bool|int|string>
     */
    private array $properties = [];

    private ?int $priority = null;

    private string $messageId = '';

    private string $appId = '';

    private bool $compress = false;

    /**
     * Creates a new publisher for the given content.
     *
     * @param string $content The message body to publish
     * @param RabbitMQConnection $connection The AMQP connection
     * @param TopologyManager $topology The topology manager for exchange/queue declarations
     * @param LoggerInterface $logger The logger instance
     */
    public function __construct(
        private readonly string $content,
        private readonly RabbitMQConnection $connection,
        private readonly TopologyManager $topology,
        private readonly LoggerInterface $logger,
    ) {}

    /**
     * Merges additional headers into the message.
     *
     * @param array<string, mixed> $header The headers to merge
     *
     * @return self
     */
    public function header(array $header): self
    {
        $this->headers = array_merge($this->headers, $header);

        return $this;
    }

    /**
     * Sets the maximum retry count for the message.
     *
     * @param int $maxRetries The maximum number of retries
     *
     * @return Publisher
     */
    public function retry(int $maxRetries): self
    {
        $this->headers['x-retries-max'] = $maxRetries;

        return $this;
    }

    /**
     * Sets the message priority.
     *
     * @param int|null $priority The priority value between 0 and 10, or null to unset
     *
     * @throws PublishException If the priority is out of range
     *
     * @return Publisher
     */
    public function priority(?int $priority): self
    {
        if ($priority !== null && ($priority < 0 || $priority > 10)) {
            throw new PublishException(
                sprintf('Priority must be between 0 and 10, got %d.', $priority),
            );
        }

        $this->priority = $priority;

        return $this;
    }

    /**
     * Sets the message identifier.
     *
     * @param string $messageId The message identifier
     *
     * @return Publisher
     */
    public function messageId(string $messageId): self
    {
        $this->messageId = $messageId;

        return $this;
    }

    /**
     * Sets the application identifier.
     *
     * @param string $appId The application identifier
     *
     * @return Publisher
     */
    public function appId(string $appId): self
    {
        $this->appId = $appId;

        return $this;
    }

    /**
     * Sets an additional AMQP message property.
     *
     * @param string $key The property key
     * @param bool|int|string $value The property value
     *
     * @return Publisher
     */
    public function property(string $key, bool|int|string $value): self
    {
        $this->properties[$key] = $value;

        return $this;
    }

    /**
     * Enables gzip compression for the message body.
     *
     * @return Publisher
     */
    public function compress(): self
    {
        $this->compress = true;

        return $this;
    }

    /**
     * Publishes the message to the specified exchange.
     *
     * @param string $exchange The target exchange name
     * @param string $routingKey The routing key
     *
     * @throws PublishException If the publish operation fails
     *
     * @return bool True on successful publish
     */
    public function publish(string $exchange, string $routingKey = ''): bool
    {
        try {
            $this->connection->ensureConnected();
            $channel = $this->connection->getChannel();
            $this->topology->prepareForPublish($exchange, $channel);

            $body = $this->content;

            $messageId = $this->messageId !== '' ? $this->messageId : self::generateMessageId();

            /**
             * @var array<string, bool|int|string> $msgProperties
             */
            $msgProperties = $this->properties;
            $msgProperties['message_id'] = $messageId;
            $msgProperties['timestamp'] = time();
            $msgProperties['delivery_mode'] = 2;

            $hostname = gethostname();
            $msgProperties['app_id'] = $this->appId !== '' ? $this->appId : ($hostname !== false ? $hostname : 'unknown');

            if (!isset($msgProperties['content_type'])) {
                $msgProperties['content_type'] = json_validate($body) ? 'application/json' : 'text/plain';
            }

            if ($this->priority !== null) {
                $msgProperties['priority'] = $this->priority;
            }

            $this->headers['x-original-exchange'] = $exchange;
            $this->headers['x-original-routing-key'] = $routingKey;

            if ($this->compress) {
                $compressed = gzcompress($body, 9);

                if ($compressed === false) {
                    throw PublishException::compressionFailed();
                }

                $body = base64_encode($compressed);
                $msgProperties['content_encoding'] = 'gzip';
            }

            $applicationHeaders = new AMQPTable($this->headers);
            $msgProperties['application_headers'] = $applicationHeaders;

            $message = new AMQPMessage($body, $msgProperties);
            $channel->basic_publish($message, $exchange, $routingKey);

            $this->logger->debug('Message published', [
                'exchange' => $exchange,
                'routing_key' => $routingKey,
                'message_id' => $messageId,
            ]);

            return true;
        } catch (PublishException $e) {
            throw $e;
        } catch (Throwable $e) {
            throw PublishException::publishFailed($exchange, $routingKey, $e->getMessage());
        }
    }

    /**
     * Generates a UUIDv7 message identifier per RFC 9562.
     *
     * @return string
     */
    private static function generateMessageId(): string
    {
        $timestamp = (int) (microtime(true) * 1000);
        $bytes = random_bytes(16);
        $bytes[0] = chr(($timestamp >> 40) & 0xFF);
        $bytes[1] = chr(($timestamp >> 32) & 0xFF);
        $bytes[2] = chr(($timestamp >> 24) & 0xFF);
        $bytes[3] = chr(($timestamp >> 16) & 0xFF);
        $bytes[4] = chr(($timestamp >> 8) & 0xFF);
        $bytes[5] = chr($timestamp & 0xFF);
        $bytes[6] = chr(0x70 | (ord($bytes[6]) & 0x0F));
        $bytes[8] = chr(0x80 | (ord($bytes[8]) & 0x3F));
        $hex = bin2hex($bytes);

        return sprintf(
            '%s-%s-%s-%s-%s',
            substr($hex, 0, 8),
            substr($hex, 8, 4),
            substr($hex, 12, 4),
            substr($hex, 16, 4),
            substr($hex, 20, 12),
        );
    }
}
