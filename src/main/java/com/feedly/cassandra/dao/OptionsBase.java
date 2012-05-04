package com.feedly.cassandra.dao;

import com.feedly.cassandra.EConsistencyLevel;

public class OptionsBase implements Cloneable
{
    private EConsistencyLevel _consistencyLevel;

    public EConsistencyLevel getConsistencyLevel()
    {
        return _consistencyLevel;
    }

    public void setConsistencyLevel(EConsistencyLevel consistencyLevel)
    {
        _consistencyLevel = consistencyLevel;
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException
    {
        return super.clone();
    }
}
