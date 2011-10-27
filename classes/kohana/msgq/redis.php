<?php defined('SYSPATH') or die('No direct access allowed.');

/*
 * redis.inc - Redis Pub/Sub driver
 *
 * @package     Kohana/MsgQ
 * @category    Base
 * @author      T.Ikeda @ PicoLabs
 * @copyright   (c) 2011 PicoLabs, Inc.
 */

class MsgQ_Redis_Exception extends Exception {}

class Kohana_MsgQ_Redis extends MsgQ {
	protected	$_setted_timeout;

	/**
	 * Connect to Redis Engine.
	 *
	 * @return	MsgQ_Redis instance
	 * @throw	MsgQ_Exception
	 */
	public function connect() {
		if ($this->_engine) return $this;

		extract($this->_config['connection'] + array(
				'hostname'		=> '127.0.0.1',
				'port'			=> 6379,
				'persistent'	=> FALSE,
				'timeout'		=> 30,
				'options'		=> array(
					Redis::OPT_SERIALIZER =>
								Redis::SERIALIZER_PHP,
				),
			));

		try {
			$this->_engine = new Redis();

			if (!empty($this->_config['connection']['options'])) {
				foreach ($this->_config['connection']['options'] as
								$key => $val) {
					$this->_engine->setOption($key, $val);
				}
			}

			if ($persistent) {
				$this->_engine->pconnect($hostname, $port, $timeout);
			}
			else {
				$this->_engine->connect($hostname, $port, $timeout);
			}
		} catch (RedisException $e) {
			unset($this->_engine);
			$this->_engine = null;

			throw new MsgQ_Exception(
					'Connection failed: :message',
					array(':message', $e->getMessage()),
					$e->getCode());
		}

		return $this;
	}

	/**
	 * Disconnect from Redis Engine.
	 *
	 * @return	boolean
	 * @throw	MsgQ_Exception
	 */
	public function disconnect() {
		if (is_resource($this->_engine)) {
			$this->_engine->close();
			unset($this->_engine);
			$this->_engine = NULL;
		}
		parent::disconnect();
		return TRUE;
	}
	
	/**
	 * Subscribe to Message Queue / Channel.
	 *
	 * @param	mixed		callback function
	 * @param	string		queue / channel name.
	 * @param	array		runtime options
	 * @return	this instance
	 * @throw	MsgQ_Exception
	 *
	 * callback function:
	 * params :	MsgQ	$instance
	 *			string	$channel
	 *			string	$workload
	 * return : TRUE  -- continue message dispatching.
	 *			FALSE -- break dispatch loop after socket timeout.
	 */

	private		$_subscribe_repeats;
	private		$_receive_callback;

	public function subscribe($callback,
							  $queue = NULL, $options = NULL) {
		$this->_receive_callback = $callback;
		if (!$queue) $queue = $this->_config['queue'];

		if (($timeout = ini_get('default_socket_timeout')) != FALSE) {
			$this->_setted_timeout = intval($timeout);
		}
/* see #About LOOP Break
		ini_set('default_socket_timeout', FALSE);
*/
		ini_set('default_socket_timeout', 30);

		try {
			$this->connect();
		} catch (MsgQ_Exception $e) {
			throw $e;
			return FALSE;
		}

		$this->_subscribe_repeats = TRUE;
		while ($this->_subscribe_repeats) {
			try {
				$this->_engine->ping();
			} catch (RedisException $e) {
				try {
					$this->connect();
				} catch (MsgQ_Exception $e) {
					$this->_subscribe_repeats = FALSE;
					continue;
				}
			}

			try {
				$this->_engine->subscribe(
								array($queue),
								array($this, 'message_receiver'));
			} catch (Exception $e) {
				$code = $e->getCode();
				if ($code != 0) {
					// read connection fail(=timeout) はcode:0っぽい
					ini_set('default_socket_timeout',
											$this->_setted_timeout);

					$this->_subscribe_repeats = FALSE;

					throw new MsgQ_Exception(
							"Can't subscribe to [:queue]. : :message",
							array(
								':queue'	=> Debug::vars($queue),
								':message'	=> $e->getMessage(),
							), $code);
				}
			}
		}
		return $this;
	}

	/**
	 * Unsubscribe from Message Queue / Channel.
	 *
	 * @param	string		queue / channel name.
	 * @param	array		runtime options
	 * @return	this instance
	 * @throw	MsgQ_Exception
	 */
	public function unsubscribe($queue = null, $options = NULL) {
		if (!$queue) $queue = $this->_config['queue'];

		$this->_subscribe_repeats = FALSE;
		$this->_receive_callback = null;

		try {
			$this->_engine->unsubscribe($queue);
		} catch (KVS_Exception $e) {
			throw new MsgQ_Exception(
						"Can't unsubscribe from :queue . : :message",
						array(
							':queue'	=> $queue,
							':message'	=> $e->getMessage(),
						), $e->getCode());
		}
		return $this;
	}

	/**
	 * Send message to Queue / Channel.
	 *
	 * @param	string		workload
	 * @param	string		queue / channel name.
	 * @param	array		runtime options
	 * @return	this instance
	 * @throw	MsgQ_Exception
	 */
	public function send($workload, $queue = NULL, $options = NULL) {
		if (!$queue) $queue = $this->_config['queue'];

		try {
			$this->_engine->publish($queue, $workload);
		} catch (KVS_Exception $e) {
			throw new MsgQ_Exception(
						"Can't send message(:workload) to ':queue'.".
						": :error",
						array(
							':queue'	=> $queue,
							':workload'	=> $workload,
							':error'	=> $e->getMessage(),
						), $e->getCode());
		}
		return $this;
	}

	/**
	 * Message receiver & buffer
	 *
	 * @param	object		redis instance
	 * @param	string		queue / channel name.
	 * @param	string		workload message
	 * @param	array		runtime options
	 * @throw	MsgQ_Exception
	 * @return	string
	 */
	public function message_receiver($redis, $queue, $message) {
		if (!isset($this->_receive_callback)) {
			throw new MsgQ_Exception(
						"Workload receiver UNINITIALIZED!!!");
		}

		$callback = $this->_receive_callback;
		if (is_array($callback) &&
			!method_exists($callback[0], $callback[1])) {
			throw new MsgQ_Exception(
						"Receiver method(:method) Not Found!!!",
						array(':method' => $callback[1]));
		}

		$status = call_user_func_array($callback, array(
										$this, $queue, $message));
/**
#About LOOP Break

現状の phpredis はsubscribe()内部から呼ばれるユーザメソッドの戻り値を
見ておらず、socket_read() ループから明示的に抜けるメソッドも提供されて
いない。なおかつ、ここで Exception を throw しても extension 内部で
無視されてしまうため、呼び出し元メソッドに戻すことができない。

今回は苦肉の作として socket_timeout をあえて30秒程度とし、
sock_read() そのものがタイムアウトするようにした。

通常の無通信状態でもsock_read()がタイムアウトしてしまうが、ループ継続
フラグがTRUEであるため再び subscribe() する(その前に念のため
ping() -> connect()を試行している)。

明示的にループを抜けたい場合、ループ継続フラグをFALSEに落として
unsubscribe() した後にタイムアウトを待つ。

こうすることで re-connect() されず、結果として subscribe() メソッドは
終了する。

**/
		if (!$status) {
			$this->unsubscribe();
		}
	}
}
?>
