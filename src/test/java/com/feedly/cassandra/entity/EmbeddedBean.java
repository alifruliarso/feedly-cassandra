package com.feedly.cassandra.entity;

import java.lang.reflect.Field;
import java.util.Date;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;

@ColumnFamily(name="embedded", compressed=false)
public class EmbeddedBean implements Cloneable
{
    @Column(name="s")
    private String strVal;
    
    @Column
    private int intVal;
    
    @Column(name="l")
    private long longVal;

    @Column
    private float floatVal;

    @Column(name="d")
    private double doubleVal;
    
    @Column
    private char charVal;
    
    @Column
    private boolean boolVal;

    @Column
    private Date dateVal;

    private int notSaved; //transient
    
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
        if(obj instanceof EmbeddedBean)
            return EntityUtils.beanFieldsEqual(this, obj);

        return false;
    }
}
