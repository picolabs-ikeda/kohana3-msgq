<?php defined('SYSPATH') or die('No direct access allowed.');

/*
 * 2011/10/25 発見
 *
 * Redis のsubscribe()は "wait" する!!!
 *
 * なので、モジュールの作りをRedisお作法に乗っ取る形
 * つまり subscribe() -> データが来たらコールバック
 * という形に ActiveMQ ドライバを書き換える必要がある!!!
 *
 * あとRedisのsubscribe()はタイムアウトするのでini_setが必要。
 *
 * Example:
 *
 * [Publisher]
 * 	  $msgq = MsgQ::instance("publisher");
 *	  $msgq->connect()->send($workload);
 *
 * [Subscriber]
 *    function workload_receiver($msgqobj, $queuename, $workload) {
 *                  :
 *                  :
 *    }
 *
 *    $msgq = MsgQ::instance("subscriber");
 *    $msgq->connect()->subscribe("workload_receiver");
 *
 */

/**
 *
 * MsgQ connection wrapper/helper.
 *
 * @package		Kohana/MsgQ
 * @category	Base
 * @author		T.Ikeda @ PicoLabs
 * @copyright	(c) 2011 PicoLabs, Inc.
 */
abstract class Kohana_MsgQ {
	/**
	 * @var	string	Default instance name
	 */
	public static $default = 'default';

	/**
	 * @var	array	MsgQ instances
	 */
	public static $instances = array();

	/**
	 * Get a singleton MsgQ instance.
	 *
	 * @param	string		instance name
	 * @param	array		configuration parameters
	 * @return	MsgQ instance
	 */
	public static function instance($name = NULL,
									array $config = NULL) {
		if ($name === NULL) {
			$name = MsgQ::$default;
		}

		if (!isset(MsgQ::$instances[$name])) {
			if ($config === NULL) {
				$config = Kohana::$config->load('msgq')->$name;
			}

			if (!isset($config['type'])) {
				throw new Kohana_Exception(
						'MsgQ type not defined in :name configuration',
						array(':name' => $name));
			}

			$driver = 'MsgQ_'. ucfirst($config['type']);
			new $driver($name, $config);
		}

		return MsgQ::$instances[$name];
	}

	protected	$_instance;
	protected	$_engine;
	protected	$_config;

	/**
	 * Stores the MsgQ configuration locally and name the instance.
	 *
	 * [IMPORTANT!!] This method can't be accessed directly,
	 * you MUST use [MsgQ::instance].
	 *
	 * @return	void
	 */
	protected function __construct($name, array $config) {
		$this->_instance = $name;
		$this->_config = $config;

		MsgQ::$instances[$name] = $this;
	}

	/**
	 * Disconnect from the MsgQ when the object is destroyed.
	 *
	 *     // Destory the MsgQ instance
	 *     unset(MsgQ::$instances[(string)$db], $db);
	 * [IMPORTANT!!] Calling "unset($db)" is NOT enough to destroy
	 * the MsgQ, as it will still be stored in "MsgQ::$instances".
	 *
	 * @return	void
	 */
	final public function __destruct() {
		$this->disconnect();
	}

	/**
	 * Returns the MsgQ instance name.
	 *
	 * @return	string
	 */
	final public function __toString() {
		return $this->_instance;
	}

	/**
	 * Connect to the MsgQ.
	 *
	 * @throws	MsgQ_Exception
	 * @return	void
	 */
	abstract public function connect();

	/**
	 * Disconnect from the MsgQ.
	 *
	 * @return boolean
	 */
	public function disconnect() {
		unset(MsgQ::$instances[$this->_instance]);

		return TRUE;
	}

	/** --------------------------------------------------------
	 * Common Methods.
	 */

	/**
	 * Subscribe to Message Queue / Channel.
	 *
	 * @param	string		queue
	 * @param	array		runtime options
	 * @throws	MsgQ_Exception
	 * @return	mixed
	 */
	abstract public function subscribe($callback,
									   $queue = NULL,
									   $options = NULL);

	/**
	 * Unsubscribe from Message Queue / Channel.
	 *
	 * @param	string		queue
	 * @param	array		runtime options
	 * @throws	MsgQ_Exception
	 * @return	mixed
	 */
	abstract public function unsubscribe($queue = NULL,
										 $options = NULL);

	/**
	 * Send / Publish a message string to Message Queue / Channel
	 *
	 * @param	string		queue
	 * @param	string		message
	 * @param	array		runtime options
	 * @throws	MsgQ_Exception
	 * @return	mixed
	 */
	abstract public function send($message,
								  $queue = NULL,
								  $options = NULL);

	/** --------------------------------------------------------
	 * Magic Methods.
	 */
	public function __call($name, $arguments) {
		if (!method_exists($this->_engine, $name)) {
			throw new MsgQ_Exception(
					'Unknown method :name on :driver driver.',
					array(':name'	=> $name,
						  ':driver'	=> get_class($this->_engine),
				));
		}

		if (!is_array($arguments)) {
			$arguments = array($arguments);
		}
		try {
			return call_user_func_array(
									array($this->_engine, $name),
									$arguments);
		} catch (Exception $e) {
			throw new MsgQ_Exception(':error',
					array(':error'	=> $e->getMessage()),
					$e->getCode());
		}
	}
}
