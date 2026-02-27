<?php

namespace App\ModuleProcess\Orchestrator;

final class StepContext
{
	public function __construct(public readonly int $processId, public array $data)
	{
	}
	public function get(string $key, mixed $default = null): mixed
	{
		return $this->data[$key] ?? $default;
	}
	public function set(string $key, mixed $value): void
	{
		$this->data[$key] = $value;
	}
}