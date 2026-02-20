<?php

namespace App\ModuleProcess\Orchestrator;

use Doctrine\DBAL\Connection;
use Symfony\Component\Messenger\MessageBusInterface;
use App\Message\RunProcessStepMessage;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;

final class ProcessOrchestrator
{
	public function __construct(private Connection $db, private MessageBusInterface $bus)
	{
	}

	/**
	 * ВАЖНО:
	 * source_job_id хранится в process_instance
	 * и автоматически подтягивается при dispatch step.
	 */
	private function dispatchStep(int $processId, string $stepName): void
	{
		$sourceJobId = $this->db->fetchOne('SELECT source_job_id FROM process_instance WHERE id = ?', [
				$processId
		]);

		$this->bus->dispatch(new RunProcessStepMessage($processId, $stepName, [], $sourceJobId ? ( int ) $sourceJobId : null));
	}
	public function startProcess(string $processType, ?string $businessKey, array $payload, ?int $sourceJobId = null): int
	{
		$this->db->beginTransaction();

		$processId = $this->db->fetchOne('SELECT id FROM process_instance WHERE process_type = ? AND business_key = ? FOR UPDATE', [
				$processType,
				$businessKey
		]);

		if (!$processId)
		{
			try
			{
				$this->db->insert('process_instance', [
						'process_type' => $processType,
						'business_key' => $businessKey,
						'status' => 'RUNNING',
						'payload' => json_encode($payload),
						'source_job_id' => $sourceJobId,
						'started_at' => (new \DateTime())->format('Y-m-d H:i:s')
				]);
				$processId = ( int ) $this->db->lastInsertId();
			}
			catch ( UniqueConstraintViolationException $e )
			{
				$processId = ( int ) $this->db->fetchOne('SELECT id FROM process_instance WHERE process_type = ? AND business_key = ?', [
						$processType,
						$businessKey
				]);
			}
		}

		$this->db->executeStatement('INSERT INTO process_step (process_instance_id, step_name, status)
             VALUES (?, ?, ?)
             ON CONFLICT (process_instance_id, step_name) DO NOTHING', [
				$processId,
				'prepare',
				'PENDING'
		]);

		$this->db->commit();

		//$this->bus->dispatch(new RunProcessStepMessage($processId, 'prepare'));
		$this->dispatchStep($processId, 'prepare');

		return $processId;
	}
	public function markStepDone(int $processId, string $stepName): void
	{
		$this->db->beginTransaction();

		$step = $this->db->fetchAssociative('SELECT * FROM process_step WHERE process_instance_id = ? AND step_name = ? FOR UPDATE', [
				$processId,
				$stepName
		]);

		if (!$step)
		{
			$this->db->rollBack();
			throw new \RuntimeException("process_step not found: {$processId} / {$stepName}");
		}

		if ($step['status'] === 'DONE')
		{
			$this->db->commit();
			return;
		}

		$this->db->executeStatement('UPDATE process_step
             SET status = ?, updated_at = NOW(), finished_at = NOW()
             WHERE id = ? AND status != ?', [
				'DONE',
				$step['id'],
				'DONE'
		]);

		if ($stepName === 'finalize')
		{
			$this->db->executeStatement('UPDATE process_instance SET status = ?, finished_at = NOW() WHERE id = ?', [
					'COMPLETED',
					$processId
			]);
		}

		$this->db->commit();
	}

	/* Патч 1: fanOut — диспатчить только реально созданные шаги
	 * Эффект:
	 * retry dispatch не создаёт дубликатов сообщений
	 * fanOut становится exactly-once по диспатчу
	 */
	public function fanOut(int $processId, string $joinGroup, array $steps): void
	{
		$this->db->beginTransaction();

		$created = [];

		foreach ( $steps as $stepName )
		{
			$affected = $this->db->executeStatement('INSERT INTO process_step (process_instance_id, step_name, status, join_group)
             VALUES (?, ?, ?, ?)
             ON CONFLICT (process_instance_id, step_name) DO NOTHING', [
					$processId,
					$stepName,
					'PENDING',
					$joinGroup
			]);

			if ($affected === 1)
			{
				$created[] = $stepName; // ← только новые шаги диспатчим
			}
		}

		$this->db->commit();

		foreach ( $created as $stepName )
		{
			//$this->bus->dispatch(new RunProcessStepMessage($processId, $stepName));
			$this->dispatchStep($processId, $stepName);
		}
	}
	public function tryJoin(int $processId, string $joinGroup, string $nextStep): void
	{
		$this->db->beginTransaction();

		$rows = $this->db->fetchAllAssociative('SELECT id, status FROM process_step
             WHERE process_instance_id = ? AND join_group = ?
             FOR UPDATE', [
				$processId,
				$joinGroup
		]);

		if (!$rows)
		{
			$this->db->rollBack();
			throw new \RuntimeException("Join group '{$joinGroup}' is empty for process {$processId}");
		}

		foreach ( $rows as $row )
		{
			if ($row['status'] !== 'DONE')
			{
				$this->db->commit();
				return;
			}
		}

		$exists = $this->db->fetchOne('SELECT 1 FROM process_step WHERE process_instance_id = ? AND step_name = ?', [
				$processId,
				$nextStep
		]);

		$shouldDispatch = false;

		if (!$exists)
		{
			$this->db->insert('process_step', [
					'process_instance_id' => $processId,
					'step_name' => $nextStep,
					'status' => 'PENDING'
			]);
			$shouldDispatch = true;
		}

		$this->db->commit();

		if ($shouldDispatch)
		{
			//$this->bus->dispatch(new RunProcessStepMessage($processId, $nextStep));
			$this->dispatchStep($processId, $nextStep);
		}
	}
	public function markStepFailed(int $processId, string $stepName, string $error): void
	{
		$this->db->beginTransaction();

		$step = $this->db->fetchAssociative('SELECT * FROM process_step WHERE process_instance_id = ? AND step_name = ? FOR UPDATE', [
				$processId,
				$stepName
		]);

		if (!$step)
		{
			$this->db->rollBack();
			throw new \RuntimeException("process_step not found: {$processId} / {$stepName}");
		}

		if ($step['status'] === 'DONE')
		{
			$this->db->commit();
			return;
		}

		$this->db->executeStatement('UPDATE process_step
             SET status = ?, last_error = ?, finished_at = NOW()
             WHERE id = ? AND status != ?', [
				'FAILED',
				mb_substr($error, 0, 4000),
				$step['id'],
				'DONE'
		]);

		$this->db->executeStatement('UPDATE process_instance
             SET status = ?
             WHERE id = ? AND status NOT IN (?, ?)', [
				'FAILED',
				$processId,
				'COMPLETED',
				'FAILED'
		]);

		$this->db->commit();
	}
	public function afterPrepare(int $processId): void
	{
		$this->db->beginTransaction();

		$exists = $this->db->fetchOne('SELECT 1 FROM process_step WHERE process_instance_id = ? AND step_name = ? FOR UPDATE', [
				$processId,
				'dispatch'
		]);

		$shouldDispatch = false;

		if (!$exists)
		{
			$this->db->insert('process_step', [
					'process_instance_id' => $processId,
					'step_name' => 'dispatch',
					'status' => 'PENDING'
			]);
			$shouldDispatch = true;
		}

		$this->db->commit();

		if ($shouldDispatch)
		{
			//$this->bus->dispatch(new RunProcessStepMessage($processId, 'dispatch'));
			$this->dispatchStep($processId, 'dispatch');
		}
	}
	public function createStep(int $processId, string $stepName, ?string $joinGroup = null): void
	{

		// Проверяем, существует ли уже такой шаг
		$exists = $this->db->fetchOne('SELECT id FROM process_step
         WHERE process_instance_id = ?
         AND step_name = ?', [
				$processId,
				$stepName
		]);

		if ($exists)
		{
			return; // идемпотентность
		}

		// Создаём шаг
		$this->db->insert('process_step', [
				'process_instance_id' => $processId,
				'step_name' => $stepName,
				'status' => 'PENDING',
				'attempt' => 0,
				'join_group' => $joinGroup,
				'created_at' => (new \DateTime())->format('Y-m-d H:i:s')
		]);

		// Отправляем в очередь
		//$this->bus->dispatch(new \App\Message\RunProcessStepMessage($processId, $stepName));
		$this->dispatchStep($processId, $stepName);
	}
}

