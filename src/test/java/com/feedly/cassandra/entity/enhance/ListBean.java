package com.feedly.cassandra.entity.enhance;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.anno.RowKey;
import com.feedly.cassandra.entity.EntityUtils;

@ColumnFamily(name="listbean", compressed=true, compressionAlgo="DeflateCompressor", compressionChunkLength=8)
public class ListBean implements Comparable<ListBean>, Cloneable
{
    @RowKey
    private Long rowkey;
    
    @Column
    private String strProp1;
    
    @Column(hashIndexed=true)
    private String strProp;
    
    @Column(name="l")
    private List<Object> listProp;

    public Long getRowkey()
    {
        return rowkey;
    }

    public void setRowkey(Long rowkey)
    {
        this.rowkey = rowkey;
    }

    public String getStrProp1()
    {
        return strProp1;
    }

    public void setStrProp1(String strProp1)
    {
        this.strProp1 = strProp1;
    }

    public String getStrProp()
    {
        return strProp;
    }

    public void setStrProp(String strProp)
    {
        this.strProp = strProp;
    }

    public List<Object> getListProp()
    {
        return listProp;
    }

    public void setListProp(List<Object> listProp)
    {
        this.listProp = listProp;
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof ListBean)
            return EntityUtils.beanFieldsEqual(this, obj);

        return false;
    }

    @Override
    public int compareTo(ListBean o)
    {
        return rowkey.compareTo(o.rowkey);
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException
    {
        ListBean clone = (ListBean) super.clone();
        clone.listProp = new ArrayList<Object>(listProp);
        
        return clone;
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
