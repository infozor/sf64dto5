<?php

namespace App\Message;

/**
 * Сообщение запуска шага процесса.
 *
 * sourceJobId — идентификатор родительского job'а,
 * из которого был порождён данный шаг.
 */
final class RunProcessStepMessage
{
	public function __construct(public int $processId, public string $stepName, public array $input = [], public ?int $sourceJobId = null)
	{
	}
}