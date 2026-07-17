<?php

declare(strict_types=1);

namespace PHPdot\RabbitMQ\Tests;

use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use PHPdot\RabbitMQ\Message;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

final class MessageTest extends TestCase
{
    #[Test]
    public function fromAmqpExtractsBody(): void
    {
        $msg = new AMQPMessage('hello world');
        $message = Message::fromAMQP($msg, 'test-queue');

        self::assertSame('hello world', $message->body());
    }

    #[Test]
    public function fromAmqpExtractsMessageId(): void
    {
        $msg = new AMQPMessage('body', [
            'message_id' => 'test-id-123',
        ]);
        $message = Message::fromAMQP($msg, 'test-queue');

        self::assertSame('test-id-123', $message->messageId());
    }

    #[Test]
    public function fromAmqpExtractsHeadersFromAmqpTable(): void
    {
        $msg = new AMQPMessage('body', [
            'application_headers' => new AMQPTable([
                'x-retries-max' => 5,
                'traceparent' => '00-abc-def-01',
            ]),
        ]);
        $message = Message::fromAMQP($msg, 'test-queue');

        $headers = $message->headers();

        self::assertSame(5, $headers['x-retries-max']);
        self::assertSame('00-abc-def-01', $headers['traceparent']);
    }

    #[Test]
    public function headerReturnsDefaultForMissingKey(): void
    {
        $msg = new AMQPMessage('body');
        $message = Message::fromAMQP($msg, 'test-queue');

        self::assertSame('', $message->header('nonexistent'));
        self::assertSame('fallback', $message->header('nonexistent', 'fallback'));
    }

    #[Test]
    public function headersReturnsPlainArray(): void
    {
        $msg = new AMQPMessage('body', [
            'application_headers' => new AMQPTable([
                'foo' => 'bar',
            ]),
        ]);
        $message = Message::fromAMQP($msg, 'test-queue');

        $headers = $message->headers();

        self::assertIsArray($headers);
        self::assertSame('bar', $headers['foo']);
    }

    #[Test]
    public function queueReturnsTheQueueName(): void
    {
        $msg = new AMQPMessage('body');
        $message = Message::fromAMQP($msg, 'notifications');

        self::assertSame('notifications', $message->queue());
    }

    #[Test]
    public function maxRetriesReadsHeader(): void
    {
        $msg = new AMQPMessage('body', [
            'application_headers' => new AMQPTable([
                'x-retries-max' => 5,
            ]),
        ]);
        $message = Message::fromAMQP($msg, 'test-queue');

        self::assertSame(5, $message->maxRetries());
    }

    #[Test]
    public function maxRetriesDefaultsToZeroWhenNoHeader(): void
    {
        $msg = new AMQPMessage('body');
        $message = Message::fromAMQP($msg, 'test-queue');

        self::assertSame(0, $message->maxRetries());
    }

    #[Test]
    public function originalExchangeReadsHeader(): void
    {
        $msg = new AMQPMessage('body', [
            'application_headers' => new AMQPTable([
                'x-original-exchange' => 'events',
            ]),
        ]);
        $message = Message::fromAMQP($msg, 'test-queue');

        self::assertSame('events', $message->originalExchange());
    }

    #[Test]
    public function originalExchangeReturnsEmptyStringWhenNotSet(): void
    {
        $msg = new AMQPMessage('body');
        $message = Message::fromAMQP($msg, 'test-queue');

        self::assertSame('', $message->originalExchange());
    }

    #[Test]
    public function originalRoutingKeyReadsHeader(): void
    {
        $msg = new AMQPMessage('body', [
            'application_headers' => new AMQPTable([
                'x-original-routing-key' => 'user.created',
            ]),
        ]);
        $message = Message::fromAMQP($msg, 'test-queue');

        self::assertSame('user.created', $message->originalRoutingKey());
    }

    #[Test]
    public function originalRoutingKeyReturnsEmptyStringWhenNotSet(): void
    {
        $msg = new AMQPMessage('body');
        $message = Message::fromAMQP($msg, 'test-queue');

        self::assertSame('', $message->originalRoutingKey());
    }

    #[Test]
    public function failedReasonReadsHeader(): void
    {
        $msg = new AMQPMessage('body', [
            'application_headers' => new AMQPTable([
                'x-failed-reason' => 'timeout exceeded',
            ]),
        ]);
        $message = Message::fromAMQP($msg, 'test-queue');

        self::assertSame('timeout exceeded', $message->failedReason());
    }

    #[Test]
    public function failedReasonReturnsEmptyStringWhenNotSet(): void
    {
        $msg = new AMQPMessage('body');
        $message = Message::fromAMQP($msg, 'test-queue');

        self::assertSame('', $message->failedReason());
    }

    #[Test]
    public function priorityReadsFromProperties(): void
    {
        $msg = new AMQPMessage('body', [
            'priority' => 5,
        ]);
        $message = Message::fromAMQP($msg, 'test-queue');

        self::assertSame(5, $message->priority());
    }

    #[Test]
    public function priorityDefaultsToZero(): void
    {
        $msg = new AMQPMessage('body');
        $message = Message::fromAMQP($msg, 'test-queue');

        self::assertSame(0, $message->priority());
    }

    #[Test]
    public function bodyReturnsMessageBodyString(): void
    {
        $msg = new AMQPMessage('{"key":"value"}');
        $message = Message::fromAMQP($msg, 'test-queue');

        self::assertSame('{"key":"value"}', $message->body());
    }

    #[Test]
    public function messageIdReturnsEmptyStringWhenNotSet(): void
    {
        $msg = new AMQPMessage('body');
        $message = Message::fromAMQP($msg, 'test-queue');

        self::assertSame('', $message->messageId());
    }
}
