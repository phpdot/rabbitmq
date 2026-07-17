<?php

declare(strict_types=1);

/**
 * `rabbitmq:topology:declare` Command
 *
 * Applies the exchanges, queues, and bindings defined in `config/rabbitmq.php`
 * to the broker. Idempotent: existing matching resources are skipped. Mismatches
 * cause an abort unless `--force` is passed (which drops and recreates).
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\RabbitMQ\Cli\Command;

use PHPdot\RabbitMQ\Config\RabbitMQConfig;
use PHPdot\RabbitMQ\RabbitMQConnection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Question\ConfirmationQuestion;
use Throwable;

#[AsCommand(name: 'rabbitmq:topology:declare', description: 'Declare exchanges, queues, and bindings from config')]
final class TopologyDeclareCommand extends Command
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
            ->addOption('dry-run', null, InputOption::VALUE_NONE, 'Show planned changes without applying them')
            ->addOption('force', null, InputOption::VALUE_NONE, 'Drop and recreate mismatched queues (DESTRUCTIVE)');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $dryRun = (bool) $input->getOption('dry-run');
        $force  = (bool) $input->getOption('force');

        $connection = new RabbitMQConnection($this->config);
        try {
            $connection->connect();
        } catch (Throwable $e) {
            $output->writeln(sprintf('<error>Cannot connect: %s</error>', $e->getMessage()));

            return Command::FAILURE;
        }

        $output->writeln('');
        $output->writeln($dryRun
            ? '<comment>Dry run — no changes will be made.</comment>'
            : '<info>Declaring topology from config/rabbitmq.php</info>');
        $output->writeln('');

        $created = 0;
        $existing = 0;
        $recreated = 0;
        $messagesLost = 0;
        $blocked = 0;

        $output->writeln('<info>Exchanges:</info>');
        foreach ($this->config->exchanges as $name => $cfg) {
            $type = is_string($cfg['type'] ?? null) ? $cfg['type'] : 'direct';
            $durable = (bool) ($cfg['durable'] ?? true);
            $autoDelete = (bool) ($cfg['auto_delete'] ?? false);

            $exists = false;
            try {
                $connection->getChannel()->exchange_declare($name, $type, true);
                $exists = true;
            } catch (Throwable) {
                $this->reopen($connection);
            }

            if ($exists) {
                $output->writeln(sprintf('  <info>✓</info> %-30s (%s, %s)  [exists]', $name, $type, $durable ? 'durable' : 'transient'));
                $existing++;
                continue;
            }

            $output->writeln(sprintf(
                '  <comment>+</comment> %-30s (%s, %s)  [%s]',
                $name,
                $type,
                $durable ? 'durable' : 'transient',
                $dryRun ? 'would create' : 'created',
            ));
            if (!$dryRun) {
                $connection->getChannel()->exchange_declare($name, $type, false, $durable, $autoDelete);
                $created++;
            }
        }
        $output->writeln('');

        $output->writeln('<info>Queues:</info>');
        foreach ($this->config->queues as $name => $cfg) {
            $durable = (bool) ($cfg['durable'] ?? true);
            $argsRaw = $cfg['arguments'] ?? null;
            /**
             * @var array<string, mixed> $args
             */
            $args = is_array($argsRaw) ? $argsRaw : [];

            $exists = false;
            $messageCount = 0;
            try {
                $info = $connection->getChannel()->queue_declare($name, true);
                if ($info !== null) {
                    $messageCount = isset($info[1]) && is_int($info[1]) ? $info[1] : 0;
                }
                $exists = true;
            } catch (Throwable) {
                $this->reopen($connection);
            }

            if ($exists) {
                $mismatch = false;
                try {
                    $connection->getChannel()->queue_declare($name, false, $durable, false, false, false, $this->amqpArgs($args));
                } catch (Throwable) {
                    $mismatch = true;
                    $this->reopen($connection);
                }

                if (!$mismatch) {
                    $output->writeln(sprintf('  <info>✓</info> %-30s (%s)  [exists]', $name, $durable ? 'durable' : 'transient'));
                    $existing++;
                    continue;
                }

                if (!$force) {
                    $output->writeln(sprintf('  <error>✗</error> %-30s CONFIG DIFFERS FROM BROKER', $name));
                    $blocked++;
                    continue;
                }

                $output->writeln(sprintf(
                    '  <comment>⊖</comment> %-30s (%d messages, will be lost)  [%s]',
                    $name,
                    $messageCount,
                    $dryRun ? 'would drop' : 'dropping',
                ));

                if (!$dryRun) {
                    if (!$this->confirmDestructive($input, $output, $name, $messageCount)) {
                        $output->writeln('  <comment>Skipped.</comment>');
                        $blocked++;
                        continue;
                    }
                    $connection->getChannel()->queue_delete($name);
                    $messagesLost += $messageCount;
                    $connection->getChannel()->queue_declare($name, false, $durable, false, false, false, $this->amqpArgs($args));
                    $recreated++;
                }
                $output->writeln(sprintf(
                    '  <comment>+</comment> %-30s (%s)  [%s]',
                    $name,
                    $durable ? 'durable' : 'transient',
                    $dryRun ? 'would recreate' : 'recreated',
                ));
                continue;
            }

            $output->writeln(sprintf(
                '  <comment>+</comment> %-30s (%s)  [%s]',
                $name,
                $durable ? 'durable' : 'transient',
                $dryRun ? 'would create' : 'created',
            ));
            if (!$dryRun) {
                $connection->getChannel()->queue_declare($name, false, $durable, false, false, false, $this->amqpArgs($args));
                $created++;
            }

            $bindings = is_array($cfg['bindings'] ?? null) ? $cfg['bindings'] : [];
            foreach ($bindings as $binding) {
                if (!is_array($binding)) {
                    continue;
                }
                $exchange = is_string($binding['exchange'] ?? null) ? $binding['exchange'] : '';
                $routing  = is_string($binding['routing_key'] ?? null) ? $binding['routing_key'] : '';
                if ($exchange === '') {
                    continue;
                }

                $output->writeln(sprintf(
                    '  <comment>+ binding</comment> %s → %-20s %s  [%s]',
                    $exchange,
                    $name,
                    $routing,
                    $dryRun ? 'would create' : 'created',
                ));
                if (!$dryRun) {
                    $connection->getChannel()->queue_bind($name, $exchange, $routing);
                }
            }
        }
        $output->writeln('');

        if ($blocked > 0 && !$force) {
            $output->writeln(sprintf('<error>Aborted — %d mismatch(es) detected.</error>', $blocked));
            $output->writeln('');
            $output->writeln('To preview changes:    <comment>--dry-run</comment>');
            $output->writeln('To force recreate:     <comment>--force</comment>  (DESTRUCTIVE — drops mismatched queues, loses messages)');
            $output->writeln('');
            $connection->close();

            return Command::FAILURE;
        }

        $output->writeln(sprintf(
            '<info>Done — %d created, %d already existing, %d recreated.</info>',
            $created,
            $existing,
            $recreated,
        ));
        if ($messagesLost > 0) {
            $output->writeln(sprintf('<comment>%d messages lost.</comment>', $messagesLost));
        }
        $output->writeln('');

        $connection->close();

        return Command::SUCCESS;
    }

    /**
     * Wrap an argument map in an AMQP table for the broker wire protocol.
     *
     * @param array<string, mixed> $args
     *
     * @return \PhpAmqpLib\Wire\AMQPTable
     */
    private function amqpArgs(array $args): \PhpAmqpLib\Wire\AMQPTable
    {
        return new \PhpAmqpLib\Wire\AMQPTable($args);
    }

    /**
     * Recover the channel after a failed declaration by reconnecting.
     *
     * @param RabbitMQConnection $connection The connection to close and reopen
     *
     * @return void
     */
    private function reopen(RabbitMQConnection $connection): void
    {
        try {
            $connection->close();
        } catch (Throwable) {
        }
        $connection->connect();
    }

    /**
     * Confirm destructive.
     *
     * @param InputInterface $input
     * @param OutputInterface $output
     * @param string $queue
     * @param int $messageCount
     *
     * @return bool
     */
    private function confirmDestructive(InputInterface $input, OutputInterface $output, string $queue, int $messageCount): bool
    {
        if (!$input->isInteractive()) {
            return true;
        }
        $helper = $this->getHelper('question');
        \assert($helper instanceof \Symfony\Component\Console\Helper\QuestionHelper);
        $question = new ConfirmationQuestion(
            sprintf('  Confirm DROP of queue <comment>%s</comment> (%d messages will be lost)? [y/N] ', $queue, $messageCount),
            false,
        );

        return (bool) $helper->ask($input, $output, $question);
    }
}
