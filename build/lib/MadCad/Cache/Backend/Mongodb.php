<?php
/**
 *
 * NOTICE OF LICENSE
 *
 * This source file is subject to the Academic Free License (AFL 3.0)
 * that is bundled with this package in the file LICENSE_AFL.txt.
 * It is also available through the world-wide-web at this URL:
 * http://opensource.org/licenses/afl-3.0.php
 * If you did not receive a copy of the license and are unable to
 * obtain it through the world-wide-web, please send an email
 * to license@magentocommerce.com so we can send you a copy immediately.
 *
 * DISCLAIMER
 *
 * Do not edit or add to this file if you wish to upgrade Magento to newer
 * versions in the future. If you wish to customize Magento for your
 * needs please refer to http://www.magentocommerce.com for more information.
 *
 * @category  MadCad
 * @package   MadCad_Cache
 * @copyright Copyright (c) 2012 mad-cad.net. (http://mad-cad.net)
 * @license   http://opensource.org/licenses/afl-3.0.php  Academic Free License (AFL 3.0)
 */

class MadCad_Cache_Backend_Mongodb extends Zend_Cache_Backend implements Zend_Cache_Backend_ExtendedInterface
{
    const DEFAULT_DATABASE = 'cache';

    /**
     * Mongo Server connection.
     *
     * var Mongo
     **/
    private $_mongoServer = false;

    /**
     * Database Connection.
     */
    private $_database;

    /**
     * Available options
     *
     * ====> (int) automatic_vacuum_factor :
     * - Disable / Tune the automatic vacuum process
     * - The automatic vacuum process defragment the database file (and make it smaller)
     *   when a clean() or delete() is called
     *     0               => no automatic vacuum
     *     1               => systematic vacuum (when delete() or clean() methods are called)
     *     x (integer) > 1 => automatic vacuum randomly 1 times on x clean() or delete()
     *
     * @var array Available options
     */
    protected $_options = array(
        'automatic_vacuum_factor' => 0,
    );

    /**
     * Constructor
     *
     * @param  array $options associative array of options
     * @throws Zend_Cache_Exception
     * @return void
     */
    public function __construct(array $options = array())
    {
        if (!extension_loaded('mongo')) {
            Zend_Cache::throwException('The mongoDB extension must be loaded for using this backend !');
        }
        parent::__construct($options);
        $this->_getConnection($options);
    }

    protected function _getConnection(array $options = array())
    {
        if (!$this->_mongoServer) {
            if (array_key_exists('server', $options)) {
                $server = $options['server'];
                $this->_mongoServer = new Mongo($server);
            } else {
                $this->_mongoServer = new Mongo();
            }

            $databaseName = self::DEFAULT_DATABASE;
            if (array_key_exists('database', $options)) {
                $database = $options['database'];
                $databaseName = $database;
            }
            $this->_database = $this->_mongoServer->{$databaseName};
            $this->_database->cache->ensureIndex(
                'id',
                array(
                    'unique' => true,
                )
            );
        }
    }

    /**
     * Test if a cache is available for the given id and (if yes) return it (false else)
     *
     * @param  string  $id                     cache id
     * @param  boolean $doNotTestCacheValidity if set to true, the cache validity won't be tested
     * @return string cached datas (or false)
     */
    public function load($id, $doNotTestCacheValidity = false)
    {
        $findSelect = array('id' => $id);
        if (!$doNotTestCacheValidity) {
            $findSelect['$or'] = array(
                array(
                    'expire_time' => 0,
                ),
                array(
                    'expire_time' => array(
                        '$gte' => time(),
                    )
                )
            );
        }

        $tmp = $this->_database->cache->findOne($findSelect);
        if (is_array($tmp)) {
            return utf8_decode($tmp['data']);
        }
        return false;
    }

    /**
     * Test if a cache is available or not (for the given id)
     *
     * @param  string $id cache id
     * @return mixed false (a cache is not available) or "last modified" timestamp (int) of the available cache record
     */
    public function test($id)
    {
        if ($this->load($id)) {
            return true;
        }
        return false;
    }

    /**
     * Save some string datas into a cache record
     *
     * Note : $data is always "string" (serialization is done by the
     * core not by the backend)
     *
     * @param string $data datas to cache
     * @param string $id cache id
     * @param array $tags array of strings, the cache record will be tagged by each string entry
     * @param int $specificLifetime if != false, set a specific lifetime for this cache record (null => infinite lifetime)
     * @return boolean true if no problem
     */
    public function save($data, $id, $tags = array(), $specificLifetime = false)
    {
        $lifetime = $this->getLifetime($specificLifetime);
        $time     = time();
        $expire   = ($lifetime === 0 || $lifetime === null) ? 0 : $time + $lifetime;

        $obj = array(
            'id' => $id,
            // 'data' => new MongoBinData($data),
            'data' => utf8_encode($data),
            'tags' => $tags,
            'expire_time' => $expire,
            'lastModified' => $time,
        );
        $this->remove($id);
        $result = $this->_database->cache->insert($obj);
        return $result;
    }

    /**
     * Remove a cache record
     *
     * @param  string $id cache id
     * @return boolean true if no problem
     */
    public function remove($id)
    {
        $return = $this->_database->cache->remove(array('id' => $id));
        $this->_automaticVacuum();
        return $return;
    }

    /**
     * Clean some cache records
     *
     * Available modes are :
     * 'all' (default)  => remove all cache entries ($tags is not used)
     * 'old'            => unsupported
     * 'matchingTag'    => unsupported
     * 'notMatchingTag' => unsupported
     * 'matchingAnyTag' => unsupported
     *
     * @param  string $mode clean mode
     * @param  array  $tags array of tags
     * @throws Zend_Cache_Exception
     * @return boolean true if no problem
     */
    public function clean($mode = Zend_Cache::CLEANING_MODE_ALL, $tags = array())
    {
        switch ($mode) {
            case Zend_Cache::CLEANING_MODE_ALL:
                $this->_database->cache->remove();
                return true;
                break;
            case Zend_Cache::CLEANING_MODE_OLD:
                $findSelect = array(
                    'expire_time' => array(
                        '$gt' => 0,
                        '$lt' => time(),
                    )
                );
                return $this->_database->cache->remove($findSelect);
                break;
            case Zend_Cache::CLEANING_MODE_MATCHING_TAG:
                $ids = $this->getIdsMatchingTags($tags);
                $result = true;
                foreach ($ids as $id) {
                    $result = $this->remove($id) && $result;
                }
                return $result;
                break;
            case Zend_Cache::CLEANING_MODE_NOT_MATCHING_TAG:
                $ids = $this->getIdsNotMatchingTags($tags);
                $result = true;
                foreach ($ids as $id) {
                    $result = $this->remove($id) && $result;
                }
                return $result;
                break;
            case Zend_Cache::CLEANING_MODE_MATCHING_ANY_TAG:
                $ids = $this->getIdsMatchingAnyTags($tags);
                $result = true;
                foreach ($ids as $id) {
                    $result = $this->remove($id) && $result;
                }
                return $result;
                break;
            default:
                break;
        }
        return false;
    }

    /**
     * Return the filling percentage of the backend storage
     *
     * @throws Zend_Cache_Exception
     * @return int integer between 0 and 100
     */
    public function getFillingPercentage()
    {
        return 1;
    }

    /**
     * Return an array of stored tags
     *
     * @return array array of stored tags (string)
     */
    public function getTags()
    {
        $tags = $this->_database->command(array("distinct" => "cache", "key" => "tags"));
        return $tags['values'];
    }

    /**
     * Return an array of stored cache ids which match given tags
     *
     * In case of multiple tags, a logical AND is made between tags
     *
     * @param array $tags array of tags
     * @return array array of matching cache ids (string)
     */
    public function getIdsMatchingTags($tags = array())
    {
        $findSelect = array(
            'tags' => $tags,
        );
        $cursor = $this->_database->cache->find($findSelect);
        $res = array();
        foreach ($cursor as $info) {
            $res[] = $info['id'];
        }
        return $res;
    }

    /**
     * Return an array of stored cache ids which don't match given tags
     *
     * In case of multiple tags, a logical OR is made between tags
     *
     * @param array $tags array of tags
     * @return array array of not matching cache ids (string)
     */
    public function getIdsNotMatchingTags($tags = array())
    {
        $findSelect = array(
            'tags' => array(
                '$nin' => $tags
            ),
        );
        $cursor = $this->_database->cache->find($findSelect);
        $res = array();
        foreach ($cursor as $info) {
            $res[] = $info['id'];
        }
        return $res;
    }

    /**
     * Return an array of stored cache ids which match any given tags
     *
     * In case of multiple tags, a logical AND is made between tags
     *
     * @param array $tags array of tags
     * @return array array of any matching cache ids (string)
     */
    public function getIdsMatchingAnyTags($tags = array())
    {
        $findSelect = array(
            'tags' => array(
                '$in' => $tags,
            ),
        );
        $cursor = $this->_database->cache->find($findSelect);
        $res = array();
        foreach ($cursor as $info) {
            $res[] = $info['id'];
        }
        return $res;
    }

    /**
     * Return an array of stored cache ids
     *
     * @return array array of stored cache ids (string)
     */
    public function getIds()
    {
        $res = array();
        $cursor = $this->_database->cache->find(array(), array("id" => 1));
        foreach ($cursor as $info) {
            $res[] = $info['id'];
        }
        return $res;
    }

    /**
     * Return an array of metadatas for the given cache id
     *
     * The array must include these keys :
     * - expire : the expire timestamp
     * - tags : a string array of tags
     * - mtime : timestamp of last modification time
     *
     * @param string $id cache id
     * @return array array of metadatas (false if the cache id is not found)
     */
    public function getMetadatas($id)
    {
        $findSelect = array('id' => $id);

        $tmp = $this->_database->cache->findOne($findSelect);
        if (is_array($tmp)) {
            return array(
                'expire' => $tmp['expire_time'],
                'tags' => $tmp['tags'],
                'mtime' => $tmp['lastModified'],
            );
        }
        return false;
    }

    /**
     * Give (if possible) an extra lifetime to the given cache id
     *
     * @param string $id cache id
     * @param int $extraLifetime
     * @return boolean true if ok
     */
    public function touch($id, $extraLifetime)
    {
        $findSelect = array('id' => $id);

        $tmp = $this->_database->cache->findOne($findSelect);
        if (is_array($tmp)) {
            $this->_database->cache->update(
                $findSelect,
                array(
                    '$set' => array(
                        'expire_time' => $tmp['expire_time'] + $extraLifetime,
                        'lastModified' => time(),
                    )
                )
            );
            return true;
        }
        return false;
    }

    /**
     * Return an associative array of capabilities (booleans) of the backend
     *
     * The array must include these keys :
     * - automatic_cleaning (is automating cleaning necessary)
     * - tags (are tags supported)
     * - expired_read (is it possible to read expired cache records
     *                 (for doNotTestCacheValidity option for example))
     * - priority does the backend deal with priority when saving
     * - infinite_lifetime (is infinite lifetime can work with this backend)
     * - get_list (is it possible to get the list of cache ids and the complete list of tags)
     *
     * @return array associative of with capabilities
     */
    public function getCapabilities()
    {
        return array(
            'automatic_cleaning' => true,
            'tags' => true,
            'expired_read' => true,
            'priority' => false,
            'infinite_lifetime' => true,
            'get_list' => true
        );
    }

    /**
     * Deal with the automatic vacuum process
     *
     * @return void
     */
    private function _automaticVacuum()
    {
        if ($this->_options['automatic_vacuum_factor'] > 0) {
            $rand = rand(1, $this->_options['automatic_vacuum_factor']);
            if ($rand == 1) {
                var_dump("do vacuum");
                $this->_database->repair();
            }
        }
    }
}
