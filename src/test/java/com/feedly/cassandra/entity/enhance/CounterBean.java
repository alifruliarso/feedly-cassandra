package com.feedly.cassandra.entity.enhance;

import java.lang.reflect.Field;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.anno.RowKey;
import com.feedly.cassandra.dao.CounterColumn;
import com.feedly.cassandra.entity.EntityUtils;

@ColumnFamily(name="counter", compressed=false)
public class CounterBean implements Cloneable, Comparable<CounterBean>
{
    @RowKey
    private Long rowKey;
    
    @Column(name="c")
    private CounterColumn counterVal;
    
    public Long getRowKey()
    {
        return rowKey;
    }

    public void setRowKey(Long rowKey)
    {
        this.rowKey = rowKey;
    }

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
        if(obj instanceof CounterBean)
            return EntityUtils.beanFieldsEqual(this, obj);

        return false;
    }

    @Override
    public int compareTo(CounterBean o)
    {
        return rowKey.compareTo(o.rowKey);
    }

    @Override
    public Object clone() throws CloneNotSupportedException
    {
        CounterBean clone = (CounterBean) super.clone();
               
        return clone;
    }
}
