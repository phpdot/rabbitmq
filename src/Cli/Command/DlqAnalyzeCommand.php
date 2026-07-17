<?php

declare(strict_types=1);

/**
 * `rabbitmq:dlq:analyze` Command
 *
 * Reads dead-letter queue messages (without consuming them) and groups them
 * by death reason and original routing-key — a one-screen incident triage view.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\RabbitMQ\Cli\Command;

use PhpAmqpLib\Wire\AMQPTable;
use PHPdot\RabbitMQ\Config\RabbitMQConfig;
use PHPdot\RabbitMQ\RabbitMQConnection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Throwable;

#[AsCommand(name: 'rabbitmq:dlq:analyze', description: 'Group dead-letter messages by reason and origin')]
final class DlqAnalyzeCommand extends Command
{
    private const int BAR_WIDTH = 20;

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
            ->addOption('limit', 'l', InputOption::VALUE_REQUIRED, 'Max messages to sample', '500');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $queueArg = $input->getArgument('queue');
        $queue = is_string($queueArg) ? $queueArg : '';
        $limitOpt = $input->getOption('limit');
        $limit = max(1, is_numeric($limitOpt) ? (int) $limitOpt : 500);

        $connection = new RabbitMQConnection($this->config);
        try {
            $connection->connect();
        } catch (Throwable $e) {
            $output->writeln(sprintf('<error>Cannot connect: %s</error>', $e->getMessage()));

            return Command::FAILURE;
        }

        $channel = $connection->getChannel();

        $reasons = [];
        $routingKeys = [];
        $tags = [];
        $sampled = 0;

        for ($i = 0; $i < $limit; $i++) {
            try {
                $msg = $channel->basic_get($queue, false);
            } catch (Throwable $e) {
                $output->writeln(sprintf('<error>basic_get failed: %s</error>', $e->getMessage()));
                break;
            }

            if ($msg === null) {
                break;
            }

            $sampled++;
            $tags[] = $msg->getDeliveryTag();

            $reason = self::extractReason($msg);
            $reasons[$reason] = ($reasons[$reason] ?? 0) + 1;

            $rk = self::extractOriginalRoutingKey($msg);
            $routingKeys[$rk] = ($routingKeys[$rk] ?? 0) + 1;
        }

        foreach ($tags as $tag) {
            try {
                $channel->basic_reject($tag, true);
            } catch (Throwable) {
            }
        }

        $output->writeln('');
        $output->writeln(sprintf('<info>Analysis of %s</info> (%d messages sampled)', $queue, $sampled));
        $output->writeln('');

        if ($sampled === 0) {
            $output->writeln('  <comment>(queue is empty)</comment>');
            $output->writeln('');
            $connection->close();

            return Command::SUCCESS;
        }

        $output->writeln('<info>Reason for dead-lettering:</info>');
        self::renderHistogram($output, $reasons, $sampled);
        $output->writeln('');

        $output->writeln('<info>By original routing-key:</info>');
        self::renderHistogram($output, $routingKeys, $sampled);
        $output->writeln('');

        $connection->close();

        return Command::SUCCESS;
    }

    /**
     * Read the dead-letter reason from the message x-death headers.
     *
     * @param \PhpAmqpLib\Message\AMQPMessage $msg
     *
     * @return string
     */
    private static function extractReason(\PhpAmqpLib\Message\AMQPMessage $msg): string
    {
        if (!$msg->has('application_headers')) {
            return 'unknown';
        }
        $headers = $msg->get('application_headers');
        if (!$headers instanceof AMQPTable) {
            return 'unknown';
        }
        $native = $headers->getNativeData();
        $deaths = $native['x-death'] ?? null;
        if (!is_array($deaths) || $deaths === []) {
            return 'unknown';
        }
        $first = $deaths[0] ?? null;
        if (is_array($first) && isset($first['reason']) && is_string($first['reason'])) {
            return $first['reason'];
        }

        return 'unknown';
    }

    /**
     * Read the original routing key from the message x-death headers.
     *
     * @param \PhpAmqpLib\Message\AMQPMessage $msg
     *
     * @return string
     */
    private static function extractOriginalRoutingKey(\PhpAmqpLib\Message\AMQPMessage $msg): string
    {
        $fallback = $msg->getRoutingKey();
        $fallback = is_string($fallback) ? $fallback : '';

        if (!$msg->has('application_headers')) {
            return $fallback;
        }
        $headers = $msg->get('application_headers');
        if (!$headers instanceof AMQPTable) {
            return $fallback;
        }
        $native = $headers->getNativeData();
        $deaths = $native['x-death'] ?? null;
        if (is_array($deaths) && isset($deaths[0]) && is_array($deaths[0])) {
            $rks = $deaths[0]['routing-keys'] ?? null;
            if (is_array($rks) && isset($rks[0]) && is_string($rks[0])) {
                return $rks[0];
            }
        }

        return $fallback;
    }

    /**
     * Render a text bar chart of a label-to-count map to the console.
     *
     * @param OutputInterface $output Console output to write the chart to
     * @param array<string, int> $counts Count per label, rendered largest first
     * @param int $total Grand total used to compute each bar's percentage
     *
     * @return void
     */
    private static function renderHistogram(OutputInterface $output, array $counts, int $total): void
    {
        arsort($counts);
        $longest = 0;
        foreach (array_keys($counts) as $label) {
            $longest = max($longest, strlen($label));
        }
        foreach ($counts as $label => $count) {
            $pct = $total === 0 ? 0 : (int) round(($count / $total) * 100);
            $bars = (int) round(($count / $total) * self::BAR_WIDTH);
            $bar = str_repeat('█', $bars) . str_repeat('░', self::BAR_WIDTH - $bars);
            $output->writeln(sprintf(
                '  %-' . max(20, $longest + 2) . 's %4d  %s  %3d%%',
                $label,
                $count,
                $bar,
                $pct,
            ));
        }
    }
}
