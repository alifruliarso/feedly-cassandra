package com.feedly.cassandra.entity.enhance;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.anno.RowKey;
import com.feedly.cassandra.anno.UnmappedColumnHandler;
import com.feedly.cassandra.entity.EntityUtils;

@ColumnFamily(name="sample2", compressed=false, forceCompositeColumns=true)
public class SampleBean2 implements Cloneable, Comparable<SampleBean2>
{
    @RowKey
    private Long rowKey;
    
    @Column(name="s")
    private String strVal;
    
    @Column
    private int intVal;

    @Column(ttl=2)
    private int ttlVal;
    
    @Column(name="l")
    private long longVal;

    @Column
    private float floatVal;

    @Column(name="d")
    private double doubleVal;
    
    @Column
    private char charVal;
    
    @Column(hashIndexed=true)
    private boolean boolVal;

    @Column
    private Date dateVal;

    @Column
    private ESampleEnum sampleEnum;

    private int notSaved; //transient
    
    @UnmappedColumnHandler
    Map<String, Object> unmapped;
    
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

    public long getLongVal()
    {
        return longVal;
    }

    public void setLongVal(long longVal)
    {
        this.longVal = longVal;
    }

    public float getFloatVal()
    {
        return floatVal;
    }

    public void setFloatVal(float floatVal)
    {
        this.floatVal = floatVal;
    }

    public double getDoubleVal()
    {
        return doubleVal;
    }

    public void setDoubleVal(double doubleVal)
    {
        this.doubleVal = doubleVal;
    }

    public boolean getBoolVal()
    {
        return boolVal;
    }

    public void setBoolVal(boolean boolVal)
    {
        this.boolVal = boolVal;
    }

    public Date getDateVal()
    {
        return dateVal;
    }

    public void setDateVal(Date dateVal)
    {
        this.dateVal = dateVal;
    }

    public int getNotSaved()
    {
        return notSaved;
    }

    public void setNotSaved(int notSaved)
    {
        this.notSaved = notSaved;
    }

    public char getCharVal()
    {
        return charVal;
    }

    public void setCharVal(char charVal)
    {
        this.charVal = charVal;
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
        if(obj instanceof SampleBean2)
            return EntityUtils.beanFieldsEqual(this, obj);

        return false;
    }

    @Override
    public int compareTo(SampleBean2 o)
    {
        return rowKey.compareTo(o.rowKey);
    }

    public Map<String, Object> getUnmapped()
    {
        return unmapped;
    }

    public void setUnmapped(Map<String, Object> unmapped)
    {
        this.unmapped = unmapped;
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException
    {
        SampleBean2 clone = (SampleBean2) super.clone();
        clone.unmapped = new HashMap<String, Object>(unmapped);
        clone.dateVal = (Date) dateVal.clone();
               
        return clone;
    }

    public ESampleEnum getSampleEnum()
    {
        return sampleEnum;
    }

    public void setSampleEnum(ESampleEnum sampleEnum)
    {
        this.sampleEnum = sampleEnum;
    }

    public int getTtlVal()
    {
        return ttlVal;
    }

    public void setTtlVal(int ttlVal)
    {
        this.ttlVal = ttlVal;
    }
}
