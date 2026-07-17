<?php

declare(strict_types=1);

/**
 * `rabbitmq:peek` Command
 *
 * Inspects messages in a queue without consuming them. Uses `basic.get` with
 * manual ack and rejects with `requeue=true` so messages return to the queue
 * unchanged.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\RabbitMQ\Cli\Command;

use PHPdot\RabbitMQ\Config\RabbitMQConfig;
use PHPdot\RabbitMQ\RabbitMQConnection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Throwable;

#[AsCommand(name: 'rabbitmq:peek', description: 'Inspect messages in a queue without consuming them')]
final class PeekCommand extends Command
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
            ->addArgument('queue', InputArgument::REQUIRED, 'Queue name to peek')
            ->addOption('limit', 'l', InputOption::VALUE_REQUIRED, 'Max messages to inspect', '5');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $queueArg = $input->getArgument('queue');
        $queue = is_string($queueArg) ? $queueArg : '';
        $limitOpt = $input->getOption('limit');
        $limit = max(1, is_numeric($limitOpt) ? (int) $limitOpt : 5);

        $connection = new RabbitMQConnection($this->config);
        try {
            $connection->connect();
        } catch (Throwable $e) {
            $output->writeln(sprintf('<error>Cannot connect: %s</error>', $e->getMessage()));

            return Command::FAILURE;
        }

        $channel = $connection->getChannel();

        $output->writeln('');
        $output->writeln(sprintf('<info>Peeking %s</info> (up to %d messages — NOT acked, will return to queue)', $queue, $limit));
        $output->writeln('');

        $count = 0;
        $tags = [];

        for ($i = 1; $i <= $limit; $i++) {
            try {
                $msg = $channel->basic_get($queue, false);
            } catch (Throwable $e) {
                $output->writeln(sprintf('<error>basic_get failed: %s</error>', $e->getMessage()));
                break;
            }

            if ($msg === null) {
                break;
            }

            $count++;
            $deliveryTag = $msg->getDeliveryTag();
            $tags[] = $deliveryTag;

            $headers = [];
            if ($msg->has('application_headers')) {
                $h = $msg->get('application_headers');
                if ($h instanceof \PhpAmqpLib\Wire\AMQPTable) {
                    foreach ($h->getNativeData() as $headerKey => $headerVal) {
                        $headers[(string) $headerKey] = $headerVal;
                    }
                }
            }

            $routingKey = $msg->getRoutingKey();
            $body = $msg->getBody();

            $output->writeln(sprintf('  <info>msg #%d</info>', $i));
            $output->writeln(sprintf('    routing-key: %s', $routingKey));
            if ($headers !== []) {
                $compact = [];
                foreach ($headers as $k => $v) {
                    $compact[] = $k . '=' . (is_scalar($v) ? (string) $v : '<' . gettype($v) . '>');
                }
                $output->writeln(sprintf('    headers:     %s', implode(', ', $compact)));
            }
            $preview = strlen($body) > 200 ? substr($body, 0, 200) . '…' : $body;
            $output->writeln(sprintf('    body:        %s', $preview));
            $output->writeln('');
        }

        foreach ($tags as $tag) {
            try {
                $channel->basic_reject($tag, true);
            } catch (Throwable) {
            }
        }

        if ($count === 0) {
            $output->writeln('  <comment>(queue is empty)</comment>');
            $output->writeln('');
        } else {
            $output->writeln(sprintf('<info>Peeked %d message(s) — all returned to queue.</info>', $count));
            $output->writeln('');
        }

        $connection->close();

        return Command::SUCCESS;
    }
}
