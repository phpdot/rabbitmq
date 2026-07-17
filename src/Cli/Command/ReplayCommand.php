<?php

declare(strict_types=1);

/**
 * `rabbitmq:replay` Command
 *
 * Requeues messages from a dead-letter queue back to their original exchange
 * by wrapping the existing Replayer class.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\RabbitMQ\Cli\Command;

use PHPdot\RabbitMQ\Config\RabbitMQConfig;
use PHPdot\RabbitMQ\Enum\ReplayAction;
use PHPdot\RabbitMQ\Message;
use PHPdot\RabbitMQ\RabbitMQConnection;
use PHPdot\RabbitMQ\Replayer;
use PHPdot\RabbitMQ\Topology\TopologyManager;
use Psr\Log\NullLogger;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Throwable;

#[AsCommand(name: 'rabbitmq:replay', description: 'Replay messages from a dead-letter queue back to their origin')]
final class ReplayCommand extends Command
{
    /**
     * Bind the command to the broker configuration it connects with.
     *
     * @param RabbitMQConfig $config Broker connection settings
     */
    public function __construct(
        private readonly RabbitMQConfig $config,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('queue', InputArgument::REQUIRED, 'Dead-letter queue name')
            ->addOption('limit', 'l', InputOption::VALUE_REQUIRED, 'Max messages to process', '10')
            ->addOption('dry-run', null, InputOption::VALUE_NONE, 'Skip without acking — preview only');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $queueArg = $input->getArgument('queue');
        $queue = is_string($queueArg) ? $queueArg : '';
        $limitOpt = $input->getOption('limit');
        $limit = max(1, is_numeric($limitOpt) ? (int) $limitOpt : 10);
        $dryRun = (bool) $input->getOption('dry-run');

        $connection = new RabbitMQConnection($this->config);
        try {
            $connection->connect();
        } catch (Throwable $e) {
            $output->writeln(sprintf('<error>Cannot connect: %s</error>', $e->getMessage()));

            return Command::FAILURE;
        }

        $topology = new TopologyManager($this->config, new NullLogger());
        $replayer = (new Replayer($queue, $connection, $topology, new NullLogger()))->limit($limit);

        $output->writeln('');
        $output->writeln(sprintf(
            $dryRun
                ? '<comment>Peeking %s</comment> (dry run, limit %d — no changes)'
                : '<info>Replaying from %s</info> (limit %d)',
            $queue,
            $limit,
        ));
        $output->writeln('');

        $i = 0;
        try {
            $result = $replayer->execute(static function (Message $msg) use ($output, $dryRun, &$i): ReplayAction {
                $i++;
                $hops = self::extractHops($msg);
                $reason = self::extractReason($msg);
                $note = $hops > 0 ? sprintf('(%d hops, last: %s)', $hops, $reason) : '';

                if ($dryRun) {
                    $output->writeln(sprintf('  msg #%-3d → would replay %s', $i, $note));

                    return ReplayAction::SKIP;
                }
                $output->writeln(sprintf('  <info>✓</info> msg #%-3d → replayed %s', $i, $note));

                return ReplayAction::REPLAY;
            });
        } catch (Throwable $e) {
            $output->writeln(sprintf('<error>Replay aborted: %s</error>', $e->getMessage()));
            $connection->close();

            return Command::FAILURE;
        }

        $output->writeln('');
        $output->writeln(sprintf(
            '<info>Done — %d processed: %d replayed, %d removed, %d skipped.</info>',
            $result->total,
            $result->replayed,
            $result->removed,
            $result->skipped,
        ));
        $output->writeln('');

        $connection->close();

        return Command::SUCCESS;
    }

    /**
     * Extract hops.
     *
     * @param Message $msg
     *
     * @return int
     */
    private static function extractHops(Message $msg): int
    {
        $deaths = $msg->headers()['x-death'] ?? [];
        if (!is_array($deaths)) {
            return 0;
        }
        $count = 0;
        foreach ($deaths as $entry) {
            if (is_array($entry) && isset($entry['count']) && is_int($entry['count'])) {
                $count += $entry['count'];
            }
        }

        return $count;
    }

    /**
     * Extract reason.
     *
     * @param Message $msg
     *
     * @return string
     */
    private static function extractReason(Message $msg): string
    {
        $reason = $msg->failedReason();
        if ($reason !== '') {
            return $reason;
        }
        $deaths = $msg->headers()['x-death'] ?? [];
        if (is_array($deaths)) {
            $first = $deaths[0] ?? null;
            if (is_array($first) && isset($first['reason']) && is_string($first['reason'])) {
                return $first['reason'];
            }
        }

        return 'unknown';
    }
}
