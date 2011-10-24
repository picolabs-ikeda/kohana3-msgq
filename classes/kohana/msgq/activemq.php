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
			$this->_connection = new \Stomp($uri);
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
		unset($this->_connection);
		$this->_connection = null;
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
	 * @param	string		queue / channel name.
	 * @param	array		runtime options
	 * @return	this instance
	 * @throw	MsgQ_Exception
	 */
	public function subscribe($queue = NULL, $options = NULL) {
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
			$this->_connection->subscribe($queue, $headers);
		} catch (StompException $e) {
			throw new MsgQ_Exception(
						"Can't subscribe to :queue . : :message",
						array(
							':queue'	=> $queue,
							':message'	=> $e->getMessage(),
						), $e->getCode());
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
			$this->_connection->unsubscribe($queue, $headers);
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
			$this->_connection->send($queue, $message, $headers);
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

	/**
	 * Check a message in message to Queue / Channel.
	 *
	 * @param	string		queue / channel name.
	 * @param	array		runtime options
	 * @throw	MsgQ_Exception
	 * @return	boolean
	 */
	public function hasMessage($queue = NULL, $options = NULL) {
	/*
		$transactionId = NULL;
		if ($options && is_array($options)) {
			extract($options + array(
										'transactionId'	=> NULL,
									));
		}
		$headers = $transactionId ?
						array('transaction' => $transactionId) : NULL;
	*/
		if (!$queue) $queue = $this->_config['queue'];
		try {
			$status = $this->_connection->hasFrame();
		} catch (StompException $e) {
			throw new MsgQ_Exception(
						"Can't send message to :queue . : :message",
						array(
							':queue'	=> $queue,
							':message'	=> $e->getMessage(),
						), $e->getCode());
			return FALSE;
		}
		return $status;
	}

	/**
	 * Read a message from message to Queue / Channel.
	 *
	 * @param	string		queue / channel name.
	 * @param	array		runtime options
	 * @throw	MsgQ_Exception
	 * @return	string
	 */
	public function readMessage($queue = NULL, $options = NULL) {
/*
		$transactionId = NULL;
		if ($options && is_array($options)) {
			extract($options + array(
										'transactionId'	=> NULL,
									));
		}
		$headers = $transactionId ?
						array('transaction' => $transactionId) : NULL;
*/
		if (!$queue) $queue = $this->_config['queue'];
		try {
			$this->_readedMessage = $this->_connection->readFrame();
		} catch (StompException $e) {
			throw new MsgQ_Exception(
						"Can't send message to :queue . : :message",
						array(
							':queue'	=> $queue,
							':message'	=> $e->getMessage(),
						), $e->getCode());
			return NULL;
		}
		return $this->_readedMessage;
	}

	/**
	 * Remove message from message to Queue / Channel.
	 *
	 * @param	string		queue / channel name.
	 * @param	array		runtime options
	 * @throw	MsgQ_Exception
	 * @return	string
	 */
	public function ack($queue = NULL, $options = NULL) {
		$ack_message = NULL;
		$transactionId = NULL;
		if ($options && is_array($options)) {
			extract($options + array(
										'transactionId'	=> NULL,
										'ack_message'	=> NULL,
									));
		}

		if (!$queue) $queue = $this->_config['queue'];
		if (!$ack_message) $ack_message = $this->_readedMessage;
		try {
			$this->_connection->ack($ack_message, $transactionId);
		} catch (StompException $e) {
			throw new MsgQ_Exception(
						"Can't send message to :queue . : :message",
						array(
							':queue'	=> $queue,
							':message'	=> $e->getMessage(),
						), $e->getCode());
			return NULL;
		}
		return $message;
	}
}
?>
