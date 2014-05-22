<?php  defined('SYSPATH') or die('No direct access allowed.');

/*
 * queue.php - Queue class
 *
 * @package		Kohana/MsgQ
 * @category	Base
 * @autor		T.Ikeda @ PicoLabs (Convert to Kohana Module)
 * @copyright (c) 2011 PicoLabs, Inc.
 */


/**
* Abstract class Queue
*/
abstract class Kohana_Queue {
	/**
	* @const int	Default size of queue
	*/
	private $_queue;	// Array of queue items
	private	$_current;	// Current size of array


	public function __construct() {
		$this->_queue = Array();
		$this->_current = 0;
		$this->clear();
	}

	public function __destruct() {
		unset($this->_queue);
	}

	public function put(&$queueItem) {
		$this->_current = $this->_queue[] = $queueItem;
		return TRUE;
	}


	public function get() {
		if ($this->isEmpty()) {
			return FALSE;
		}
		$objItem = array_shift($this->_queue);
		$this->_current = count($this->_queue);
		return $objItem;
	}

	public function isEmpty() {
		return ($this->_current <= 0 ? TRUE : FALSE);
	}

	public function clear() {
		unset($this->_queue);
		$this->_queue = Array();
		$this->_current	= 0;
	}
}
?>
