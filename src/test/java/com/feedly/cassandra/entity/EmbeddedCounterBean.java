package com.feedly.cassandra.entity;

import java.lang.reflect.Field;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.dao.CounterColumn;

@ColumnFamily(name="embedded", compressed=false)
public class EmbeddedCounterBean implements Cloneable
{
    @Column(name="c")
    private CounterColumn counterVal;
    
    public CounterColumn getCounterVal()
    {
        return counterVal;
    }

    public void setCounterVal(CounterColumn counterVal)
    {
        this.counterVal = counterVal;
    }

    @Override
    public String toString()
    {
        StringBuilder b = new StringBuilder();
        for(Field f : getClass().getDeclaredFields())
        {
            if(f.getName().startsWith("__"))
                continue;

            try
            {
                b.append(f.getName() + ":" + f.get(this)).append(" ");
            }
            catch(Exception e)
            {
                return "error";
            }
        }
        
        return b.toString();
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof EmbeddedCounterBean)
            return EntityUtils.beanFieldsEqual(this, obj);

        return false;
    }
}
