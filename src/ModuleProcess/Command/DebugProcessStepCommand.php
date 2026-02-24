<?php

namespace App\ModuleProcess\Command;

use App\Message\RunProcessStepMessage;
use App\MessageHandler\RunProcessStepHandler;
use App\ModuleProcess\Orchestrator\ProcessOrchestrator;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:process:debug', description: 'Manual debug runner for RunProcessStepHandler')]
final class DebugProcessStepCommand extends Command
{
	public function __construct(private RunProcessStepHandler $handler, private ProcessOrchestrator $orchestrator)
	{
		parent::__construct();
	}
	protected function configure(): void
	{
		$this->addArgument('processId', InputArgument::REQUIRED)->addArgument('stepName', InputArgument::REQUIRED)->addOption('show-payload', null, InputOption::VALUE_NONE, 'Print input/output payload');
	}
	protected function execute(InputInterface $input, OutputInterface $output): int
	{
		$processId = ( int ) $input->getArgument('processId');
		$stepName = ( string ) $input->getArgument('stepName');
		$showPayload = ( bool ) $input->getOption('show-payload');

		$output->writeln('');
		$output->writeln("<info>▶ Running process {$processId} step '{$stepName}'</info>");

		try
		{

			/* ===============================
			 * INPUT PAYLOAD
			 * =============================== */

			if ($showPayload)
			{
				$inputPayload = $this->orchestrator->getInputPayload($processId, $stepName);

				$output->writeln('<comment>Input payload:</comment>');
				$output->writeln(json_encode($inputPayload, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE));
				$output->writeln('');
			}

			$startedAt = microtime(true);

			/* ===============================
			 * EXECUTE HANDLER (sync)
			 * =============================== */

			($this->handler)(new RunProcessStepMessage($processId, $stepName));

			$duration = round(microtime(true) - $startedAt, 3);

			/* ===============================
			 * OUTPUT PAYLOAD
			 * =============================== */

			if ($showPayload)
			{
				$outputPayload = $this->orchestrator->getOutputPayload($processId, $stepName);

				$output->writeln('<comment>Output payload:</comment>');
				$output->writeln(json_encode($outputPayload, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE));
				$output->writeln('');
			}

			$output->writeln("<info>✔ DONE in {$duration}s</info>");

			return Command::SUCCESS;
		}
		catch ( \Throwable $e )
		{

			$output->writeln('');
			$output->writeln('<error>✖ ERROR</error>');
			$output->writeln($e->getMessage());
			$output->writeln('');
			$output->writeln($e->getTraceAsString());

			return Command::FAILURE;
		}
	}
}