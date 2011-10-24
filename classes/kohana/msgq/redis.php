<?php defined('SYSPATH') or die('No direct access allowed.');

/*
 * redis.inc - Redis Pub/Sub driver
 *
 * @package     Kohana/MsgQ
 * @category    Base
 * @author      T.Ikeda @ PicoLabs
 * @copyright   (c) 2011 PicoLabs, Inc.
 */

class Kohana_MsgQ_Redis extends MsgQ {
	protected		 $_buffer = null;
	protected static $_bufferSize = 20;

	/**
	 * Connect to Redis Engine.
	 *
	 * @return	MsgQ_Redis instance
	 * @throw	MsgQ_Exception
	 */
	public function connect() {
		try {
			$this->_connection = KVS::instance('msgq',
										$this->_config['connection']);
		} catch (KVS_Exception $e) {
			throw new MsgQ_Exception(
					'Connection failed: :message',
					array(':message', $e->getMessage()),
					$e->getCode());
		}

		$this->_buffer = array();

		return $this;
	}

	/**
	 * Disconnect from Redis Engine.
	 *
	 * @return	boolean
	 * @throw	MsgQ_Exception
	 */
	public function disconnect() {
		$this->_connection->disconnect();
		unset($this->_connection);
		$this->_connection = null;
		parent::disconnect();

		return TRUE;
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
		if (!$queue) $queue = $this->_config['queue'];
		try {
			$this->_connection->subscribe($queue,
							array($this, 'message_receiver'));
		} catch (KVS_Exception $e) {
			throw new MsgQ_Exception(
						"Can't subscribe to :queue . : :message",
						array(
							':queue'	=> $queue,
							':message'	=> $e->getMessage(),
						), $e->getCode());
		}

		if (isset($this->_buffer[$queue])) {
			unset($this->_buffer[$queue]);
		}
		$this->_buffer[$queue] = new Queue($this->_queueSize);
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
		try {
			$this->_connection->unsubscribe($queue);
		} catch (KVS_Exception $e) {
			throw new MsgQ_Exception(
						"Can't unsubscribe from :queue . : :message",
						array(
							':queue'	=> $queue,
							':message'	=> $e->getMessage(),
						), $e->getCode());
		}
		if (isset($this->_buffer[$queue])) {
			unset($this->_buffer[$queue]);
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
		if (!$queue) $queue = $this->_config['queue'];
		try {
			$this->_connection->publish($queue, $message);
		} catch (KVS_Exception $e) {
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
		if (!$queue) $queue = $this->_config['queue'];

		return ! $this->_queue[$queue]->isEmpty();
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
		if (!$queue) $queue = $this->_config['queue'];
		return $this->_queue[$queue]->get();
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
		// not implement...
		return TRUE;
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
	protected function message_receiver($redis, $queue, $message) {
		if (!isset($this->_buffer[$queue])) {
			throw new MsgQ_Exception(
						"Internal message buffer UNINITIALIZED!!!");
		}
		$this->_buffer[$queue]->put($message);
	}
}
?>
