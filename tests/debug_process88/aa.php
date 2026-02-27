<?php

//$output = exec('php -dxdebug.start_with_request=yes D:\site2_sf\symfony7\sf64dto5\bin/console app:process:debug 88 call_api_a x1');

$jsonData = json_encode(['a' => '123']);

// Для Windows cmd
$command = 'php -dxdebug.start_with_request=yes D:\site2_sf\symfony7\sf64dto5\bin/console app:process:debug 88 call_api_a "' . addslashes($jsonData) . '"';
$output = exec($command);