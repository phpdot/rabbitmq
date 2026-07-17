<?php

declare(strict_types=1);

/**
 * ReplayResult
 *
 * Immutable value object representing the outcome of a dead letter queue replay.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\RabbitMQ\Result;

final readonly class ReplayResult
{
    public int $total;

    /**
     * Creates a new replay result.
     *
     * @param int $replayed The number of messages replayed to their original exchange
     * @param int $removed The number of messages removed (acknowledged and discarded)
     * @param int $skipped The number of messages skipped (nacked back to queue)
     */
    public function __construct(
        public int $replayed,
        public int $removed,
        public int $skipped,
    ) {
        $this->total = $this->replayed + $this->removed + $this->skipped;
    }
}
