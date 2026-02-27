<?php

namespace App\ModuleProcess\Orchestrator;

use Doctrine\DBAL\Connection;

/**
 * ProcessContextStore
 *
 * Shared state процесса.
 *
 * Архитектура:
 *  - append-only журнал
 *  - НЕТ update'ов
 *  - НЕТ overwrite
 *
 * => нет гонок между параллельными fan-out шагами
 */
final class ProcessContextStore
{
	public function __construct(private Connection $db)
	{
	}
	
	/**
	 * ==========================================================
	 * Append output шага в общий контекст
	 * ==========================================================
	 *
	 * Каждый шаг добавляет свой кусок данных.
	 * Ничего не перезаписывается.
	 */
	public function append(int $processId, string $stepName, array $payload): void
	{
		if (empty($payload)) {
			return;
		}
		
		$this->db->insert('process_context', [
				'process_instance_id' => $processId,
				'step_name'           => $stepName,
				'payload'             => json_encode($payload),
				'created_at'          => (new \DateTime())->format('Y-m-d H:i:s'),
		]);
	}
	
	/**
	 * ==========================================================
	 * ✅ FIX: load()
	 * ==========================================================
	 *
	 * Используется RunProcessStepHandler.
	 *
	 * Возвращает объединённый context процесса.
	 */
	public function load(int $processId): array
	{
		return $this->getMerged($processId);
	}
	
	/**
	 * ==========================================================
	 * Merge всех payload'ов процесса
	 * ==========================================================
	 *
	 * Поздние записи перекрывают ранние.
	 */
	public function getMerged(int $processId): array
	{
		$rows = $this->db->fetchAllAssociative(
				'SELECT payload
             FROM process_context
             WHERE process_instance_id = ?
             ORDER BY id ASC',
				[$processId]
				);
		
		$context = [];
		
		foreach ($rows as $row) {
			$data = json_decode($row['payload'], true) ?? [];
			
			// shallow merge — достаточно для workflow payload
			$context = array_merge($context, $data);
		}
		
		return $context;
	}
	
	/**
	 * ==========================================================
	 * Получить context только до конкретного шага
	 * (полезно для replay/debug)
	 * ==========================================================
	 */
	public function loadUntilStep(int $processId, string $stepName): array
	{
		$rows = $this->db->fetchAllAssociative(
				'SELECT payload
             FROM process_context
             WHERE process_instance_id = ?
               AND id <= (
                    SELECT MAX(id)
                    FROM process_context
                    WHERE process_instance_id = ?
                      AND step_name = ?
               )
             ORDER BY id',
				[$processId, $processId, $stepName]
				);
		
		$ctx = [];
		
		foreach ($rows as $row) {
			$ctx = array_merge(
					$ctx,
					json_decode($row['payload'], true) ?? []
					);
		}
		
		return $ctx;
	}
	
	/**
	 * ==========================================================
	 * Debug helper
	 * ==========================================================
	 */
	public function dumpRaw(int $processId): array
	{
		return $this->db->fetchAllAssociative(
				'SELECT *
             FROM process_context
             WHERE process_instance_id = ?
             ORDER BY id',
				[$processId]
				);
	}
}