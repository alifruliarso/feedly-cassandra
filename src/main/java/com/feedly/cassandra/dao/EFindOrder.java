package com.feedly.cassandra.dao;

/**
 * When doing a range find, rows can be returned in sorted order. Unless strictly required, using NONE should be used as it will result in
 * better performance (avoids sort).
 * 
 * @author kireet
 * @see FindBetweenOptions
 */
public enum EFindOrder
{
    /**
     * order results in ascending index order.
     */
    ASCENDING, 
    /**
     * order results in descending index order.
     */
    DESCENDING,
    /**
     * do not order results
     */
    NONE;
}
