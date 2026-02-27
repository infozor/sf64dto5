<?php

function run()
{
	$command = 'php -dxdebug.start_with_request=yes D:\site2_sf\symfony7\sf64dto5\bin/console app:scheduler:run"';
	$output = exec($command);
	return $output;
}


$output = run();
echo $output;
