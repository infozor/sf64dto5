<?php

namespace App\MessageHandler;

use App\Message\RunProcessStepMessage;
use App\ModuleProcess\Orchestrator\ProcessOrchestrator;
use Doctrine\DBAL\Connection;
use Symfony\Component\Messenger\Attribute\AsMessageHandler;
use App\ModuleProcess\Orchestrator\ProcessContextStore;
use App\ModuleProcess\Orchestrator\StepContext;

#[AsMessageHandler]
final class RunProcessStepHandler
{
	/**
	 * ==============================
	 * BUSINESS EXECUTORS
	 * ==============================
	 *
	 * ✅ Только реальные business-шаги.
	 * orchestration nodes (dispatch, archive, post_dispatch)
	 * здесь НЕ должны присутствовать.
	 */
	private array $stepExecutors = [

			'prepare' => 'callPrepare',

			'call_api_a' => 'callApiA',
			'call_api_b' => 'callApiB',
			'generate_doc' => 'generateDocument',

			'archive_db' => 'archiveDb',
			'archive_files' => 'archiveFiles',

			'finalize' => 'callFinalize'
	];

	/**
	 * ==============================
	 * PROCESS GRAPH
	 * ==============================
	 *
	 * orchestration nodes допустимы без executor.
	 */
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
	public function __construct(private Connection $db, private ProcessOrchestrator $orchestrator, private string $projectDir, private ProcessContextStore $contextStore)
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

		/**
		 * ✅ Idempotency guard
		 */
		if (in_array($step['status'], [
				'DONE',
				'FAILED'
		], true))
		{
			$this->db->rollBack();
			return;
		}

		/**
		 * ✅ Messenger retry protection
		 */
		if ($step['status'] === 'RUNNING' && $step['attempt'] > 1)
		{
			$this->db->rollBack();
			return;
		}

		/**
		 * ✅ Atomic worker claim
		 */
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
			 * 1️⃣ LOAD INPUT
			 * ==============================
			 */
			$input = json_decode($step['input_payload'] ?? '{}', true) ?? [];

			/**
			 * ==============================
			 * 2️⃣ EXECUTE
			 * ==============================
			 */
			$output = $this->executeBusinessStep($message->processId, $message->stepName, $input);

			/**
			 * ==============================
			 * 3️⃣ DONE
			 * ==============================
			 */
			$this->orchestrator->markStepDone($message->processId, $message->stepName, $output);

			/**
			 * ==============================
			 * 4️⃣ TRANSITIONS
			 * ==============================
			 */
			$this->handleTransitions($message->processId, $message->stepName, $output);

			/**
			 * ==============================
			 * 5️⃣ JOIN CHECK
			 * ==============================
			 *
			 * ✅ FIX:
			 * join target берётся из graph,
			 * а не хардкодится finalize.
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
	 *
	 * ✅ orchestration nodes возвращают []
	 * (НЕ null — это важно для payload pipeline)
	 */
	private function executeBusinessStep(int $processId, string $stepName, array $input): array
	{
		if (!isset($this->stepExecutors[$stepName]))
		{
			return []; // orchestration node
		}

		$method = $this->stepExecutors[$stepName];

		/**
		 * ✅ FIX:
		 * StepContext теперь единая точка доступа
		 * к shared state процесса.
		 */
		$contextData = $this->contextStore->load($processId);
		$ctx = new StepContext($processId, $contextData, $input);

		$output = $this->$method($ctx);

		/**
		 * ✅ сохраняем обновлённый context
		 */
		//$this->contextStore->save($processId, $ctx->all());
		$this->contextStore->append($processId, $stepName, $output);
		

		return $output ?? [];
	}

	/**
	 * ==============================
	 * TRANSITIONS
	 * ==============================
	 */
	private function handleTransitions(int $processId, string $stepName, array $outputPayload = []): void
	{
		$node = $this->processGraph[$stepName] ?? null;

		if (!$node)
		{
			return;
		}

		/**
		 * linear transition
		 */
		if (isset($node['next']))
		{
			$this->orchestrator->createStep($processId, $node['next'], $outputPayload);
			return;
		}

		/**
		 * fan-out transition
		 */
		if (isset($node['fan_out']))
		{

			$fan = $node['fan_out'];

			$this->orchestrator->fanOut($processId, $fan['group'], $fan['steps'], $outputPayload);

			return;
		}
	}

	/**
	 * ==============================
	 * Resolve join target from graph
	 * ==============================
	 */
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
	private function callPrepare(StepContext $ctx): array
	{
		sleep(1);

		$ctx->set('preparedAt', date('c'));

		return [
				'preparedAt' => $ctx->get('preparedAt')
		];
	}
	private function callApiA(StepContext $ctx): array
	{
		sleep(1);
		return [
				'apiA' => 'ok'
		];
	}
	private function callApiB(StepContext $ctx): array
	{
		sleep(1);
		return [
				'apiB' => 'ok'
		];
	}
	private function generateDocument(StepContext $ctx): array
	{
		sleep(1);

		$docId = rand(1000, 9999);
		$ctx->set('documentId', $docId);

		return [
				'documentId' => $docId
		];
	}
	private function archiveDb(StepContext $ctx): array
	{
		sleep(1);
		return [
				'dbArchived' => true
		];
	}
	private function archiveFiles(StepContext $ctx): array
	{
		sleep(1);
		return [
				'filesArchived' => true
		];
	}
	private function callFinalize(StepContext $ctx): array
	{
		sleep(1);
		return [
				'finalized' => true
		];
	}
}