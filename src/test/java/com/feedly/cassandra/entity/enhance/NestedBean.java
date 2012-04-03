package com.feedly.cassandra.entity.enhance;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.anno.RowKey;
import com.feedly.cassandra.entity.EntityUtils;

@ColumnFamily(name="nestedbean")
public class NestedBean implements Comparable<NestedBean>
{
    @RowKey
    private Long rowkey;
    
    @Column
    private Map<String, List<String>> mapOfListProp;

    @Column
    private Map<String, Map<Integer, Integer>> mapOfMapProp;

    @Column
    private Map<String, List<Map<String, Date>>> mapOfListOfMapProp;

    @Column
    private List<Map<String, String>> listOfMapProp;

    @Column
    private List<List<Double>> listOfListProp;
    
    @Column
    private List<Map<Long, List<Double>>> listOfMapOfListProp;

    public Long getRowkey()
    {
        return rowkey;
    }

    public void setRowkey(Long rowkey)
    {
        this.rowkey = rowkey;
    }

    public Map<String, List<String>> getMapOfListProp()
    {
        return mapOfListProp;
    }

    public void setMapOfListProp(Map<String, List<String>> mapOfListProp)
    {
        this.mapOfListProp = mapOfListProp;
    }

    public Map<String, Map<Integer, Integer>> getMapOfMapProp()
    {
        return mapOfMapProp;
    }

    public void setMapOfMapProp(Map<String, Map<Integer, Integer>> mapOfMapProp)
    {
        this.mapOfMapProp = mapOfMapProp;
    }

    public Map<String, List<Map<String, Date>>> getMapOfListOfMapProp()
    {
        return mapOfListOfMapProp;
    }

    public void setMapOfListOfMapProp(Map<String, List<Map<String, Date>>> mapOfListOfMapProp)
    {
        this.mapOfListOfMapProp = mapOfListOfMapProp;
    }

    public List<Map<String, String>> getListOfMapProp()
    {
        return listOfMapProp;
    }

    public void setListOfMapProp(List<Map<String, String>> listOfMapProp)
    {
        this.listOfMapProp = listOfMapProp;
    }

    public List<List<Double>> getListOfListProp()
    {
        return listOfListProp;
    }

    public void setListOfListProp(List<List<Double>> listOfListProp)
    {
        this.listOfListProp = listOfListProp;
    }

    public List<Map<Long, List<Double>>> getListOfMapOfListProp()
    {
        return listOfMapOfListProp;
    }

    public void setListOfMapOfListProp(List<Map<Long, List<Double>>> listOfMapOfListProp)
    {
        this.listOfMapOfListProp = listOfMapOfListProp;
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof NestedBean)
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
    
    @Override
    public int compareTo(NestedBean o)
    {
        return rowkey.compareTo(o.rowkey);
    }

}
