<?php

declare(strict_types=1);

/**
 * ReplayAction
 *
 * Represents the action to take on a dead-lettered message during replay.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\RabbitMQ\Enum;

enum ReplayAction: string
{
    case REPLAY = 'replay';
    case REMOVE = 'remove';
    case SKIP = 'skip';
}
