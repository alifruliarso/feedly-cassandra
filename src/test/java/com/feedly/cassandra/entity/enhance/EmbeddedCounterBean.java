package com.feedly.cassandra.entity.enhance;

import java.lang.reflect.Field;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.EmbeddedEntity;
import com.feedly.cassandra.dao.CounterColumn;
import com.feedly.cassandra.entity.EntityUtils;

@EmbeddedEntity
public class EmbeddedCounterBean
{
    @Column(name="s")
    private String strProp;

    @Column(name="c")
    private CounterColumn counterProp;

    public String getStrProp()
    {
        return strProp;
    }

    public void setStrProp(String strProp)
    {
        this.strProp = strProp;
    }
    
    public CounterColumn getCounterProp()
    {
        return counterProp;
    }

    public void setCounterProp(CounterColumn counterProp)
    {
        this.counterProp = counterProp;
    }

    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof EmbeddedCounterBean)
            return EntityUtils.beanFieldsEqual(this, obj);

        return false;
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

}
