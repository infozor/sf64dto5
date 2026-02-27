<?php

function run()
{
	$jsonData = json_encode(['doc' => '123']);
	$command = 'php -dxdebug.start_with_request=yes D:\site2_sf\symfony7\sf64dto5\bin/console app:process:debug 88 generate_doc "' . addslashes($jsonData) . '"';
	$output = exec($command);
	return $output;
}


$output = run();
echo $output;
