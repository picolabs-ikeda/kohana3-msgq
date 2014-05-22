<?php defined('SYSPATH') or die('No direct access allowed.');

/*
 * activemq.inc - ActiveMQ driver
 *
 * @package     Kohana/MsgQ
 * @category    Base
 * @author      T.Ikeda @ PicoLabs
 * @copyright   (c) 2011 PicoLabs, Inc.
 */

class Kohana_MsgQ_ActiveMQ extends MsgQ {
	const	DEFAULT_ITERATE = 30;

	protected $_readedMessage = null;

	/**
	 * Constructor
	 *
	 * @param	string	instance name
	 * @param	array	configuration parameters
	 * @return	void
	 */
	protected function __construct($name, array $config) {
		// Stomp 拡張のロード確認
		if (!extension_loaded('stomp')) {
			throw new MsgQ_Exception('Stomp Extension not loaded!');
		}
		parent::_construct($name, $config);

		if (!isset($this->_config['iterate']) ||
			intval($this->_config['iterate']) <= 0) {
			$this->_config['iterate'] = self::DEFAULT_ITERATE;
		}

	}

	/**
	 * Connect to ActiveMQ Engine.
	 *
	 * @return	MsgQ_ActiveMQ instance
	 * @throw	MsgQ_Exception
	 */
	public function connect() {
		$uri = $this->getServerUri();
		try {
			$this->_engine = new \Stomp($uri);
		} catch (StompException $e) {
			throw new MsgQ_Exception(
					'Connection failed: :message',
					array(':message', $e->getMessage()),
					$e->getCode());
		}
		return $this;
	}

	/**
	 * Disconnect from ActiveMQ Engine.
	 *
	 * @return	boolean
	 * @throw	MsgQ_Exception
	 */
	public function disconnect() {
		unset($this->_engine);
		$this->_engine = null;
		parent::disconnect();

		return TRUE;
	}
	
	/**
	 * Get a ActiveMQ Engine URI.
	 *
	 * @return	string		connection uri
	 */
	public function getServerUri() {
		return "tcp://". $this->_config['connection']['host'] .
							":". $this->_config['connection']['port'];
	}

	/**
	 * Subscribe to Message Queue / Channel.
	 *
	 * @param	mixed		callback function
	 * @param	string		queue / channel name.
	 * @param	array		runtime options
	 * @return	this instance
	 * @throw	MsgQ_Exception
	 */
	public function subscribe($callback,
							  $queue = NULL, $options = NULL) {
		$transactionId = NULL;
		if ($options && is_array($options)) {
			extract($options + array(
										'transactionId'	=> NULL,
									));
		}
		if (!$queue) $queue = $this->_config['queue'];
		$headers = $transactionId ?
						array('transaction' => $transactionId) : NULL;

		try {
			$this->_engine->subscribe($queue, $headers);
		} catch (StompException $e) {
			throw new MsgQ_Exception(
						"Can't subscribe to :queue . : :message",
						array(
							':queue'	=> $queue,
							':message'	=> $e->getMessage(),
						), $e->getCode());
		}

		$this->_dispatch($callback, $queue, $options);
	}

	private function _iterate($sleepMicroSeconds) {
		usleep($sleepMicroSeconds);
		if (function_exists('gc_collect_cycles')) {
			gc_collect_cycles();
		}
		return TRUE;
	}

	private function _dispatch($callback,
							   $queue = NULL, $options = NULL) {
		if (!$queue) $queue = $this->_config['queue'];

		// sleep seconds -> microseconds;
		$iteratesec = $this->_config['iterate'] * 1000000;

		$is_continue = TRUE;
		while ($is_continue) {
			if (!$this->_engine->hasFrame()) {
				$this->_iterate($iteratesec);
				continue;
			}

			try {
				$frame = $this->_engine->readFrame();
			} catch (Exception $e) {
				$is_continue = FALSE;
				break;
			}

			try {
				$status = call_user_func_array($callback, array(
												$this, queue, $frame));
				$this->_engine->ack($frame);
			} catch (Exception $e) {
				$this->_engine->rollback($frame);
			}

			if ($status === FALSE) {
				$is_continue = FALSE;
				break;
			}
		}
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
		$transactionId = NULL;
		if ($options && is_array($options)) {
			extract($options + array(
										'transactionId'	=> NULL,
									));
		}
		if (!$queue) $queue = $this->_config['queue'];
		$headers = $transactionId ?
						array('transaction' => $transactionId) : NULL;

		try {
			$this->_engine->unsubscribe($queue, $headers);
		} catch (StompException $e) {
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
	 * @param	string		message payload
	 * @param	string		queue / channel name.
	 * @param	array		runtime options
	 * @return	this instance
	 * @throw	MsgQ_Exception
	 */
	public function send($message, $queue = NULL, $options = NULL) {
		$transactionId = NULL;
		if ($options && is_array($options)) {
			extract($options + array(
										'transactionId'	=> NULL,
									));
		}
		if (!$queue) $queue = $this->_config['queue'];
		$headers = $transactionId ?
						array('transaction' => $transactionId) : NULL;

		try {
			$this->_engine->send($queue, $message, $headers);
		} catch (StompException $e) {
			throw new MsgQ_Exception(
						"Can't send message to :queue . : :message",
						array(
							':queue'	=> $queue,
							':message'	=> $e->getMessage(),
						), $e->getCode());
		}
		return $this;
	}
}
?>
