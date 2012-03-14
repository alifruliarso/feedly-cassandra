package com.feedly.cassandra.dao;

/**
 * Used to indicate filter result when performing filter-on-reads of values fetched during an index find operation.
 * @author kireet
 * @see IValueFilter
 */
/**
 * @author kireet
 *
 */
enum EFilterResult {
    /**
     * value passed filtering and should be included.
     */
    PASS, 
    /**
     * value was filtered and should be excluded.
     */
    FAIL, 
    /**
     * value was filtered and should be excluded. Additionally the index record is stale.
     */
    FAIL_STALE}