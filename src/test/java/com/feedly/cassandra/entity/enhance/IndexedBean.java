package com.feedly.cassandra.entity.enhance;

import java.lang.reflect.Field;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.anno.Index;
import com.feedly.cassandra.anno.Indexes;
import com.feedly.cassandra.anno.RowKey;
import com.feedly.cassandra.entity.EntityUtils;

@ColumnFamily(name="indexedbean")
@Indexes({@Index(props = {"strVal", "longVal"}), @Index(props = {"strVal2", "longVal"})})
public class IndexedBean implements Comparable<IndexedBean>
{
    @RowKey
    private Long rowKey;
    
    @Column(col="c")
    private Character charVal;
    
    @Column(col="s", hashIndexed = true)
    private String strVal;
    
    @Column(col="s2")
    private String strVal2;
    
    @Column(hashIndexed=true)
    private int intVal;

    @Column
    private int intVal2;

    @Column(rangeIndexed=true)
    private Long longVal;
    
    public Long getRowKey()
    {
        return rowKey;
    }

    public void setRowKey(Long rowKey)
    {
        this.rowKey = rowKey;
    }

    public String getStrVal()
    {
        return strVal;
    }

    public void setStrVal(String strVal)
    {
        this.strVal = strVal;
    }

    public int getIntVal()
    {
        return intVal;
    }

    public void setIntVal(int intVal)
    {
        this.intVal = intVal;
    }

    public Long getLongVal()
    {
        return longVal;
    }

    public void setLongVal(Long longVal)
    {
        this.longVal = longVal;
    }
    
    public Character getCharVal()
    {
        return charVal;
    }

    public void setCharVal(Character cVal)
    {
        this.charVal = cVal;
    }

    public int getIntVal2()
    {
        return intVal2;
    }

    public void setIntVal2(int intVal2)
    {
        this.intVal2 = intVal2;
    }

    public String getStrVal2()
    {
        return strVal2;
    }

    public void setStrVal2(String strVal2)
    {
        this.strVal2 = strVal2;
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
        if(obj instanceof IndexedBean)
            return EntityUtils.beanFieldsEqual(this, obj);

        return false;
    }

    @Override
    public int compareTo(IndexedBean o)
    {
        return rowKey.compareTo(o.rowKey);
    }

}
