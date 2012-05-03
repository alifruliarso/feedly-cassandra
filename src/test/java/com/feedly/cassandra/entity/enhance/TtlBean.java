package com.feedly.cassandra.entity.enhance;

import java.lang.reflect.Field;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.anno.RowKey;
import com.feedly.cassandra.entity.EntityUtils;

@ColumnFamily(name="ttlbean", ttl=3)
public class TtlBean
{
    @RowKey
    private Long rowKey;
    
    @Column(name="s1", ttl=1) 
    private String strVal1;

    @Column(name="s2", ttl=5)
    private String strVal2;

    @Column(name="s3")
    private String strVal3;

    public Long getRowKey()
    {
        return rowKey;
    }

    public void setRowKey(Long rowKey)
    {
        this.rowKey = rowKey;
    }

    public String getStrVal1()
    {
        return strVal1;
    }

    public void setStrVal1(String strVal1)
    {
        this.strVal1 = strVal1;
    }

    public String getStrVal2()
    {
        return strVal2;
    }

    public void setStrVal2(String strVal2)
    {
        this.strVal2 = strVal2;
    }

    public String getStrVal3()
    {
        return strVal3;
    }

    public void setStrVal3(String strVal3)
    {
        this.strVal3 = strVal3;
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof TtlBean)
            return EntityUtils.beanFieldsEqual(this, obj);

        return false;
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
}
