package com.feedly.cassandra.dao;

/**
 * Used to create a counter column in a mapped cassandra entity. Cassandra forces counter columns to live in their own column family however
 * the mapping library allows for counters to be intermingled with normal columns for ease of use and clarity. In most cases, they behave
 * similarly to other columns. The exceptions are
 * <ul>
 * <li>They may not be included in general collections of the form Map&lt;String, Object> or List&lt;Object>, including unmapped columns. 
 * They may be included in narrowed collections of the form Map&lt;String, CounterColumn> or List&lt;CounterColumn>.</li>
 * <li>Due to synchronization issues, counter columns can not be indexed.</li>
 * <li>TTL is not supported for counter columns.</li>
 * </ul>
 *   
 * <p>
 * The key method is {@link #setIncrement(Long)}. This sets the value that is used to modify the counter in cassandra when the column is saved.
 * 
 * @author kireet
 */
public final class CounterColumn
{
    private Long _stored;
    private Long _increment;
    
    public CounterColumn()
    {
        
    }
    
    public CounterColumn(long incr)
    {
        _increment = incr;
    }
    
    CounterColumn(Long stored, Long incr)
    {
        _stored = stored;
        _increment = incr;
    }
    
    /**
     * Get the counter value stored in cassandra. Note this may or may not be accurate as the method makes no attempt to refresh the stored
     * counter value. 
     * @return the last loaded counter value.
     */
    public Long getStored()
    {
        return _stored;
    }

    //package protected - should only be set by library when loading the counter value
    void setStored(Long stored)
    {
        _stored = stored;
    }
    
    /**
     * Get the counter increment.
     * @return the value by which to change the counter.
     */
    public long getIncrement()
    {
        return _increment == null ? 0 : _increment;
    }

    /**
     * Set the counter increment
     * @param increment the value by which to change the counter.
     */
    public void setIncrement(long increment)
    {
        _increment = increment;
    }

    /**
     * indicates whether a value is available. I.e., has a value been loaded from cassandra.
     * @return the counter status
     */
    boolean dirty()
    {
        return _increment != null;
    }
    
    /**
     * indicates whether a value is available. I.e., has a value been loaded from cassandra.
     * @return the counter status
     */
    public boolean available()
    {
        return _stored != null;
    }
    
    /**
     * Gets the modified counter value. Note this may or may not be accurate as the method makes no attempt to refresh the stored counter
     * value. 
     * @return the modified counter value using the last loaded value and current increment.
     */
    public long value()
    {
        if(_stored == null)
            throw new IllegalStateException("stored counter value not loaded");
        
        return _stored + (_increment == null ? 0 : _increment);
    }

    void reset()
    {
        _increment = null;
    }
    
    private boolean nullSafeEquals(Object o1, Object o2)
    {
        if(o1 == o2)
            return true;
        
        if(o1 == null || o2 == null)
            return false;
                    
        return o1.equals(o2);
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof CounterColumn)
        {
            CounterColumn other = (CounterColumn) obj;
            return nullSafeEquals(_increment, other._increment) && nullSafeEquals(_stored, other._stored);
        }
        
        return false;
    }
    
    @Override
    public String toString()
    {
        return String.format("%s (%d)", _stored, _increment);
    }

}
