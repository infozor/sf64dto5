<?php

namespace App\ModuleProcess\Orchestrator;

use Doctrine\DBAL\Connection;
use Symfony\Component\Messenger\MessageBusInterface;
use App\Message\RunProcessStepMessage;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;

final class ProcessOrchestrator__
{
	public function __construct(private Connection $db, private MessageBusInterface $bus)
	{
	}
	private function dispatchStep(int $processId, string $stepName): void
	{
		$sourceJobId = $this->db->fetchOne('SELECT source_job_id FROM process_instance WHERE id = ?', [
				$processId
		]);

		$this->bus->dispatch(new RunProcessStepMessage($processId, $stepName, [], $sourceJobId ? ( int ) $sourceJobId : null));
	}

	/* ============================================================
	 START PROCESS
	 ============================================================ */
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

		// prepare step получает payload процесса как input
		$this->db->executeStatement('INSERT INTO process_step
             (process_instance_id, step_name, status, input_payload)
             VALUES (?, ?, ?, ?)
             ON CONFLICT (process_instance_id, step_name) DO NOTHING', [
				$processId,
				'prepare',
				'PENDING',
				json_encode($payload)
		]);

		$this->db->commit();

		$this->dispatchStep($processId, 'prepare');

		return $processId;
	}

	/* ============================================================
	 STEP DONE + OUTPUT
	 ============================================================ */
	public function markStepDone(int $processId, string $stepName, array $outputPayload = []): void
	{
		$this->db->beginTransaction();

		$step = $this->db->fetchAssociative('SELECT * FROM process_step
             WHERE process_instance_id = ? AND step_name = ?
             FOR UPDATE', [
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
             SET status = ?, output_payload = ?, finished_at = NOW()
             WHERE id = ?', [
				'DONE',
				json_encode($outputPayload),
				$step['id']
		]);

		if ($stepName === 'finalize')
		{
			$this->db->executeStatement('UPDATE process_instance
                 SET status = ?, finished_at = NOW()
                 WHERE id = ?', [
					'COMPLETED',
					$processId
			]);
		}

		$this->db->commit();
	}

	/* ============================================================
	 CREATE STEP WITH INPUT
	 ============================================================ */
	public function createStep(int $processId, string $stepName, array $inputPayload = [], ?string $joinGroup = null): void
	{
		$exists = $this->db->fetchOne('SELECT id FROM process_step
             WHERE process_instance_id = ?
             AND step_name = ?', [
				$processId,
				$stepName
		]);

		if ($exists)
		{
			return;
		}

		$this->db->insert('process_step', [
				'process_instance_id' => $processId,
				'step_name' => $stepName,
				'status' => 'PENDING',
				'attempt' => 0,
				'join_group' => $joinGroup,
				'input_payload' => json_encode($inputPayload),
				'created_at' => (new \DateTime())->format('Y-m-d H:i:s')
		]);

		$this->dispatchStep($processId, $stepName);
	}
}
