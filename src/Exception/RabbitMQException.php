<?php

declare(strict_types=1);

/**
 * RabbitMQException
 *
 * Base exception for all queue-related errors.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\RabbitMQ\Exception;

use RuntimeException;

class RabbitMQException extends RuntimeException {}
