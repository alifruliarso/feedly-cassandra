package com.feedly.cassandra.dao;

enum EColumnFilterStrategy
{
    /**
     * retrieve all columns
     */
    UNFILTERED, 
    /**
     * retrieve a range of columns
     */
    RANGE,
    /**
     * retrive a specific set of columns
     */
    INCLUDES;
}
