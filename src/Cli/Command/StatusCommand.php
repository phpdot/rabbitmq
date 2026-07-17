<?php

declare(strict_types=1);

/**
 * `rabbitmq:status` Command
 *
 * Reports broker connectivity and detects topology drift between
 * the configured exchanges/queues and what's actually declared on the broker.
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
use Symfony\Component\Console\Output\OutputInterface;
use Throwable;

#[AsCommand(name: 'rabbitmq:status', description: 'Show broker connectivity and topology drift')]
final class StatusCommand extends Command
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

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $output->writeln('');
        $output->writeln('<info>RabbitMQ Status</info>');
        $output->writeln('');

        $output->writeln(sprintf('  Host           %s:%d', $this->config->host, $this->config->port));
        $output->writeln(sprintf('  Vhost          %s', $this->config->vhost));
        $output->writeln(sprintf('  User           %s', $this->config->username));

        $connection = new RabbitMQConnection($this->config);
        $start = microtime(true);

        try {
            $connection->connect();
            $elapsed = (microtime(true) - $start) * 1000;
            $output->writeln(sprintf('  Connection     <info>✓ ok</info> (%.1f ms)', $elapsed));
        } catch (Throwable $e) {
            $output->writeln(sprintf('  Connection     <error>✗ failed</error>: %s', $e->getMessage()));
            $output->writeln('');

            return Command::FAILURE;
        }

        $output->writeln('');
        $output->writeln('<info>Topology in config/rabbitmq.php:</info>');
        $output->writeln('');

        $missing = 0;
        $channel = $connection->getChannel();

        foreach ($this->config->exchanges as $name => $cfg) {
            $cfgType = $cfg['type'] ?? null;
            $type = is_string($cfgType) ? $cfgType : 'direct';
            $durable = (bool) ($cfg['durable'] ?? true) ? 'durable' : 'transient';
            try {
                $channel->exchange_declare($name, $type, true);
                $output->writeln(sprintf('  <info>✓</info> exchange  %-30s (%s, %s)', $name, $type, $durable));
            } catch (Throwable) {
                $output->writeln(sprintf('  <error>✗</error> exchange  %-30s NOT declared on broker', $name));
                $missing++;
                $connection->close();
                $connection->connect();
                $channel = $connection->getChannel();
            }
        }

        foreach ($this->config->queues as $name => $cfg) {
            $durable = (bool) ($cfg['durable'] ?? true) ? 'durable' : 'transient';
            try {
                $channel->queue_declare($name, true);
                $output->writeln(sprintf('  <info>✓</info> queue     %-30s (%s)', $name, $durable));
            } catch (Throwable) {
                $output->writeln(sprintf('  <error>✗</error> queue     %-30s NOT declared on broker', $name));
                $missing++;
                $connection->close();
                $connection->connect();
                $channel = $connection->getChannel();
            }
        }

        $output->writeln('');
        if ($missing > 0) {
            $output->writeln(sprintf('<comment>%d resource(s) missing — run `rabbitmq:topology:declare` to fix.</comment>', $missing));
        } else {
            $output->writeln('<info>All resources declared.</info>');
        }
        $output->writeln('');

        $connection->close();

        return Command::SUCCESS;
    }
}
