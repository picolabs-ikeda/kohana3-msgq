<?php defined('SYSPATH') or die('No direct access allowed.');

return array(
	'default'	=>	array(
		'type'			=> 'redis',	// redis | activemq
		'connection'	=> array(
			'hostname'	=> '127.0.0.1',
			'port'		=> '6379,
		),
		'queue'			=> 'queue_name',
	),
	'searchlog'	=>	array(
		'type'			=> 'activemq',	// redis | activemq
		'connection'	=> array(
			'hostname'	=> '192.168.1.10',
			'port'		=> '61613',
		),
		'queue'			=> '/queue/searchlog',
	),
	'bannerlog'	=>	array(
		'type'			=> 'activemq',	// redis | activemq
		'connection'	=> array(
			'hostname'	=> '192.168.1.10',
			'port'		=> '61613',
		),
		'queue'			=> '/queue/bannerlog',
	),
	'distribute'	=>	array(
		'type'			=> 'redis',	// redis | activemq
		'connection'	=> array(
			'hostname'	=> '127.0.0.1',
			'port'		=> '6379,
		),
		'queue'			=> 'distribute',
	),
);

