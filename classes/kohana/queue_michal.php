<?php  defined('SYSPATH') or die('No direct access allowed.');

/*
 * queue.php - Queue class
 *
 * @package		Kohana/MsgQ
 * @category	Base
 * @autor		T.Ikeda @ PicoLabs (Convert to Kohana Module)
 * @original	Michal 'Seth' Golebiowski <sethmail at poczta dot fm>
 * @copyright (c) 2011 PicoLabs, Inc.
 * @copyright (c) 2003 Michal 'Seth' Golebiowski <sethmail at poczta dot fm>
 */


/**
* Abstract class Queue
* @version 1.9
*/
abstract class Kohana_Queue {
	/**
	* @const int	Default size of queue
	*/
	const	DEFAULT_SIZE	= 	15;

	private $_queue;	// Array of queue items
	private	$_begin;	// Begin of queue - head
	private	$_end;		// End of queue - tail
	private	$_size;		// Size of array
	private	$_current;	// Current size of array


	public function __construct($size = NULL) {
		if (!$size) $size = self::DEFAULT_SIZE;

		$this->_queue = Array();
		$this->_size  = $size;
		$this->clear();
	}

	public function __destruct() {
		unset($this->_queue);
	}

	public function put(&$queueItem) {
		if ($this->_current >= $this->_size) {
			return FALSE;
		}

		if ($this->_end == $this->_size - 1) {
			$this->_end = 0;
		} else {
			$this->_end++;
		}

		$this->_queue[$this->_end] = $queueItem;
		$this->_current++;

		return TRUE;
	}


	public function get() {
		if ($this->isEmpty()) {
			return FALSE;
		}

		$objItem = $this->_queue[$this->_begin];

		if ($this->_begin == $this->_size - 1) {
			$this->_begin = 0;
		} else {
			$this->_begin++;
		}

		$this->_current--;
		return $objItem;
	}

	public function isEmpty() {
		return ($this->_current == 0 ? TRUE : FALSE);
	}

	public function clear() {
		$this->_current	= 0;
		$this->_begin	= 0;
		$this->_end		= $this->_size - 1;
	}

	public function extend($newsize) {
		if ($newsize < $this->_size) {
			return FALSE;
		}

		$this->_size = $newsize;
		return $this->_size;
	}
}
?>
