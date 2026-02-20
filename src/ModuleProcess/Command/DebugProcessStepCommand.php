<?php

namespace App\ModuleProcess\Command;


use App\Message\RunProcessStepMessage;
use App\MessageHandler\RunProcessStepHandler;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:process:debug', description: 'Manual debug runner for RunProcessStepHandler')]
final class DebugProcessStepCommand extends Command
{
	public function __construct(private RunProcessStepHandler $handler)
	{
		parent::__construct();
	}
	protected function configure(): void
	{
		$this->addArgument('processId', InputArgument::REQUIRED)->addArgument('stepName', InputArgument::REQUIRED);
	}
	protected function execute(InputInterface $input, OutputInterface $output): int
	{
		$processId = ( int ) $input->getArgument('processId');
		$stepName = ( string ) $input->getArgument('stepName');

		$output->writeln("Running process {$processId} step {$stepName}");

		try
		{
			($this->handler)(new RunProcessStepMessage($processId, $stepName));

			$output->writeln('<info>OK</info>');
		}
		catch ( \Throwable $e )
		{
			$output->writeln('<error>ERROR: ' . $e->getMessage() . '</error>');
			return Command::FAILURE;
		}

		return Command::SUCCESS;
	}
}
