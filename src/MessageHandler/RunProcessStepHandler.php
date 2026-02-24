<?php

namespace App\MessageHandler;

use App\Message\RunProcessStepMessage;
use App\ModuleProcess\Orchestrator\ProcessOrchestrator;
use Doctrine\DBAL\Connection;
use Symfony\Component\Messenger\Attribute\AsMessageHandler;

#[AsMessageHandler]
final class RunProcessStepHandler
{
	private array $stepExecutors = [

			'prepare' => 'callPrepare',

			'call_api_a' => 'callApiA',
			'call_api_b' => 'callApiB',
			'generate_doc' => 'generateDocument',

			'archive_db' => 'archiveDb',
			'archive_files' => 'archiveFiles',

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
             WHERE process_instance_id = ?
             AND step_name = ?
             FOR UPDATE', [
				$message->processId,
				$message->stepName
		]);

		if (!$step)
		{
			$this->db->rollBack();
			return;
		}

		if (in_array($step['status'], [
				'DONE',
				'FAILED'
		], true))
		{
			$this->db->rollBack();
			return;
		}

		if ($step['status'] === 'RUNNING' && $step['attempt'] > 1)
		{
			$this->db->rollBack();
			return;
		}

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
			 * ==============================
			 * 1️⃣ Load INPUT payload
			 * ==============================
			 */
			$input = json_decode($step['input_payload'] ?? '{}', true) ?? [];

			/**
			 * ==============================
			 * 2️⃣ Execute business logic
			 * ==============================
			 */
			$output = $this->executeBusinessStep($message->processId, $message->stepName, $input);


			/**
			 * ==============================
			 * 4️⃣ DONE
			 * ==============================
			 */
			$this->orchestrator->markStepDone($message->processId, $message->stepName, $output);

			/**
			 * ==============================
			 * 5️⃣ transitions
			 * ==============================
			 */
			$this->handleTransitions($message->processId, $message->stepName);

			/**
			 * ==============================
			 * 6️⃣ join
			 * ==============================
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

	/**
	 * ==============================
	 * BUSINESS EXECUTION
	 * ==============================
	 */
	private function executeBusinessStep(int $processId, string $stepName, array $input): ?array
	{
		if (!isset($this->stepExecutors[$stepName]))
		{
			//return null; // orchestration node
			return [];
		}

		$method = $this->stepExecutors[$stepName];

		return $this->$method($processId, $input);
	}

	/**
	 * ==============================
	 * TRANSITIONS (без изменений)
	 * ==============================
	 */
	private function handleTransitions(int $processId, string $stepName, array $outputPayload = []): void
	{
		$node = $this->processGraph[$stepName] ?? null;

		if (!$node)
		{
			return;
		}

		if (isset($node['next']))
		{
			$this->orchestrator->createStep($processId, $node['next'], $outputPayload);
			return;
		}

		if (isset($node['fan_out']))
		{
			$fan = $node['fan_out'];
			$this->orchestrator->fanOut($processId, $fan['group'], $fan['steps'],$outputPayload);
			return;
		}

		foreach ( $this->processGraph as $parentNode )
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
		foreach ( $this->processGraph as $config )
		{

			if (!isset($config['fan_out']))
			{
				continue;
			}

			if ($config['fan_out']['group'] === $joinGroup)
			{
				return $config['fan_out']['join_to'] ?? null;
			}
		}

		return null;
	}

	// ================= BUSINESS =================
	private function callPrepare(int $processId, array $input): array
	{
		sleep(1);

		return [
				'preparedAt' => date('c')
		];
	}
	private function callApiA(int $processId, array $input): array
	{
		sleep(1);

		return [
				'apiA' => 'ok'
		];
	}
	private function callApiB(int $processId, array $input): array
	{
		sleep(1);

		return [
				'apiB' => 'ok'
		];
	}
	private function generateDocument(int $processId, array $input): array
	{
		sleep(1);

		return [
				'documentId' => rand(1000, 9999)
		];
	}
	private function archiveDb(int $processId, array $input): array
	{
		sleep(1);

		return [
				'dbArchived' => true
		];
	}
	private function archiveFiles(int $processId, array $input): array
	{
		sleep(1);

		return [
				'filesArchived' => true
		];
	}
	private function callFinalize(int $processId, array $input): array
	{
		sleep(1);

		return [
				'finalized' => true
		];
	}
}
