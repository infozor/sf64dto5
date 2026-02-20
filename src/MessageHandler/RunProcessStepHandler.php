<?php

namespace App\MessageHandler;

use App\Message\RunProcessStepMessage;
use App\ModuleProcess\Orchestrator\ProcessOrchestrator;
use Doctrine\DBAL\Connection;
use Symfony\Component\Messenger\Attribute\AsMessageHandler;

#[AsMessageHandler]
final class RunProcessStepHandler
{


	/**
	 * Выполнение бизнес-шагов.
	 *
	 * ВАЖНО:
	 * - здесь только реальные executable steps
	 * - orchestration-шаги (fan_out, next) НЕ указываются
	 * - граф процесса определяется processGraph
	 */
	private array $stepExecutors = [

			// root steps
			'prepare' => 'callPrepare',

			// dispatch fan-out
			'call_api_a' => 'callApiA',
			'call_api_b' => 'callApiB',
			'generate_doc' => 'generateDocument',

			// archive fan-out
			'archive_db' => 'archiveDb',
			'archive_files' => 'archiveFiles',

			// final step
			'finalize' => 'callFinalize'
	];
	private array $processGraph = [

			'prepare' => [
					'next' => 'dispatch'
			],

			'dispatch' => [
					'fan_out' => [
							'group' => 'dispatch_group',
							'steps' => [
									'call_api_a',
									'call_api_b',
									'generate_doc'
							],
							'join_to' => 'post_dispatch'
					]
			],

			'post_dispatch' => [
					'next' => 'archive'
			],

			'archive' => [
					'fan_out' => [
							'group' => 'archive_group',
							'steps' => [
									'archive_db',
									'archive_files'
							],
							'join_to' => 'finalize'
					]
			],

			'archive_db' => [],
			'archive_files' => [],

			'call_api_a' => [],
			'call_api_b' => [],
			'generate_doc' => [],

			'finalize' => []
	];
	public function __construct(private Connection $db, private ProcessOrchestrator $orchestrator, private string $projectDir)
	{
	}
	public function __invoke(RunProcessStepMessage $message): void
	{
		$this->db->beginTransaction();

		$step = $this->db->fetchAssociative('SELECT * FROM process_step
             WHERE process_instance_id = ? AND step_name = ?
             FOR UPDATE', [
				$message->processId,
				$message->stepName
		]);

		if (!$step)
		{
			$this->db->rollBack();
			return;
		}

		// DONE / FAILED — идемпотентность
		if (in_array($step['status'], [
				'DONE',
				'FAILED'
		], true))
		{
			$this->db->rollBack();
			return;
		}

		// RUNNING retry защита
		if ($step['status'] === 'RUNNING' && $step['attempt'] > 1)
		{
			$this->db->rollBack();
			return;
		}

		// Атомарный захват
		$affected = $this->db->executeStatement('UPDATE process_step
             SET status = ?, attempt = attempt + 1, locked_at = NOW()
             WHERE id = ? AND status = ?', [
				'RUNNING',
				$step['id'],
				'PENDING'
		]);

		if ($affected === 0 && $step['status'] !== 'RUNNING')
		{
			$this->db->rollBack();
			return;
		}

		$this->db->commit();

		try
		{

			/**
			 * 1️⃣ Выполняем бизнес-шаг
			 */
			$this->executeBusinessStep($message->processId, $message->stepName);

			/**
			 * 2️⃣ Помечаем DONE
			 */
			$this->orchestrator->markStepDone($message->processId, $message->stepName);

			/**
			 * 3️⃣ Обрабатываем переходы (вместо хардкода)
			 */
			$this->handleTransitions($message->processId, $message->stepName);

			/**
			 * 4️⃣ Join (как и раньше)
			 */
			/*
			 if (!empty($step['join_group']))
			 {
			 $this->orchestrator->tryJoin($message->processId, $step['join_group'], 'finalize');
			 }
			 */

			if (!empty($step['join_group']))
			{
				$nextStep = $this->resolveJoinTarget($step['join_group']);

				if ($nextStep !== null)
				{
					$this->orchestrator->tryJoin($message->processId, $step['join_group'], $nextStep);
				}
			}
		}
		catch ( \Throwable $e )
		{

			$this->orchestrator->markStepFailed($message->processId, $message->stepName, $e->getMessage());

			throw $e;
		}
	}
	
	private function executeBusinessStep(int $processId, string $stepName): void
	{
		/**
		 * Если бизнес-исполнителя нет —
		 * это orchestration step (fan-out / next / join node).
		 * Просто ничего не выполняем.
		 */
		if (!isset($this->stepExecutors[$stepName]))
		{
			return;
		}

		$method = $this->stepExecutors[$stepName];

		$this->$method($processId);
	}
	
	private function handleTransitions(int $processId, string $stepName): void
	{
		$node = $this->processGraph[$stepName] ?? null;

		if (!$node)
		{
			return;
		}

		/**
		 * NEXT transition
		 */
		if (isset($node['next']))
		{

			$this->orchestrator->createStep($processId, $node['next']);

			return;
		}

		/**
		 * FAN OUT
		 */
		if (isset($node['fan_out']))
		{

			$fan = $node['fan_out'];

			$this->orchestrator->fanOut($processId, $fan['group'], $fan['steps']);

			return;
		}

		/**
		 * JOIN handling
		 * (если текущий шаг входит в fanout другого узла)
		 */
		foreach ( $this->processGraph as $parentStep => $parentNode )
		{

			if (!isset($parentNode['fan_out']))
			{
				continue;
			}

			$fan = $parentNode['fan_out'];

			if (!in_array($stepName, $fan['steps'], true))
			{
				continue;
			}

			$this->orchestrator->tryJoin($processId, $fan['group'], $fan['join_to']);
		}
	}
	
	private function resolveJoinTarget(string $joinGroup): ?string
	{
		foreach ( $this->processGraph as $step => $config )
		{
			if (!isset($config['fan_out']))
				continue;

			if ($config['fan_out']['group'] === $joinGroup)
				return $config['fan_out']['join_to'] ?? null;
		}

		return null;
	}

	// ===== Бизнес =====
	private function callPrepare(int $processId): void
	{
		sleep(1);
	}
	private function callApiA(int $processId): void
	{
		sleep(1);
	}
	private function callApiB(int $processId): void
	{
		sleep(1);
	}
	private function generateDocument(int $processId): void
	{
		sleep(1);
	}
	private function callFinalize(int $processId): void
	{
		sleep(1);
	}
	private function archiveDb(int $processId): void
	{
		sleep(1);
	}
	private function archiveFiles(int $processId): void
	{
		sleep(1);
	}
}
