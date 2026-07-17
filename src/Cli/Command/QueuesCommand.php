<?php

declare(strict_types=1);

/**
 * `rabbitmq:queues` Command
 *
 * Lists all configured queues with current message count and consumer count
 * via passive `queue.declare` calls. Supports `--filter` to grep names and
 * `--watch` to refresh continuously.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\RabbitMQ\Cli\Command;

use PHPdot\RabbitMQ\Config\RabbitMQConfig;
use PHPdot\RabbitMQ\RabbitMQConnection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Throwable;

#[AsCommand(name: 'rabbitmq:queues', description: 'Show queue depths and consumer counts')]
final class QueuesCommand extends Command
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
            ->addOption('filter', 'f', InputOption::VALUE_REQUIRED, 'Substring filter on queue name')
            ->addOption('watch', 'w', InputOption::VALUE_NONE, 'Refresh continuously every second');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $filter = $input->getOption('filter');
        $watch  = (bool) $input->getOption('watch');

        $filterStr = is_string($filter) && $filter !== '' ? $filter : null;

        $connection = new RabbitMQConnection($this->config);

        try {
            $connection->connect();
        } catch (Throwable $e) {
            $output->writeln(sprintf('<error>Cannot connect: %s</error>', $e->getMessage()));

            return Command::FAILURE;
        }

        do {
            if ($watch) {
                $output->write("\033[2J\033[H");
                $output->writeln(sprintf('<info>RabbitMQ queues</info> (refreshing every 1s — Ctrl-C to stop)  %s', date('H:i:s')));
                $output->writeln('');
            }

            $rows = [];
            $totalMessages = 0;
            $totalConsumers = 0;
            $count = 0;

            foreach (array_keys($this->config->queues) as $name) {
                if ($filterStr !== null && !str_contains($name, $filterStr)) {
                    continue;
                }

                try {
                    $channel = $connection->getChannel();
                    $info = $channel->queue_declare($name, true);
                    if ($info === null) {
                        $rows[] = [$name, '<error>?</error>', '<error>?</error>'];
                        continue;
                    }
                    $messages = isset($info[1]) && is_int($info[1]) ? $info[1] : 0;
                    $consumers = isset($info[2]) && is_int($info[2]) ? $info[2] : 0;
                    $rows[] = [$name, (string) $messages, (string) $consumers];
                    $totalMessages += $messages;
                    $totalConsumers += $consumers;
                    $count++;
                } catch (Throwable) {
                    $rows[] = [$name, '<error>?</error>', '<error>?</error>'];
                    $connection->close();
                    $connection->connect();
                }
            }

            $table = new Table($output);
            $table->setHeaders(['Queue', 'Messages', 'Consumers']);
            $table->setStyle('box-double');
            foreach ($rows as $row) {
                $table->addRow($row);
            }
            $table->render();

            $output->writeln('');
            $output->writeln(sprintf(
                '<info>%d queues, %d messages, %d consumers</info>',
                $count,
                $totalMessages,
                $totalConsumers,
            ));
            $output->writeln('');

            if ($watch) {
                sleep(1);
            }
        } while ($watch);

        $connection->close();

        return Command::SUCCESS;
    }
}
