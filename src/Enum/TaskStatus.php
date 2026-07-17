<?php

declare(strict_types=1);

/**
 * TaskStatus
 *
 * Represents the outcome of processing a queued message.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\RabbitMQ\Enum;

enum TaskStatus: string
{
    case SUCCESS = 'success';
    case RETRY = 'retry';
    case DEAD = 'dead';
}
