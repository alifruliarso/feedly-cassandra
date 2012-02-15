package com.feedly.cassandra.bean.enhance;

import java.util.SortedMap;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.anno.RowKey;
import com.feedly.cassandra.bean.BeanUtils;

@ColumnFamily(name="sortedmapbean")
public class SortedMapBean implements Comparable<SortedMapBean>
{
    @RowKey
    private Long rowkey;
    
    @Column
    private String strProp1;
    
    @Column
    private String strProp;
    
    @Column
    private SortedMap<String, Object> mapProp;

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

    public SortedMap<String, Object> getMapProp()
    {
        return mapProp;
    }

    public void setMapProp(SortedMap<String, Object> mapProp)
    {
        this.mapProp = mapProp;
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof SortedMapBean)
            return BeanUtils.beanFieldsEqual(this, obj);

        return false;
    }

    @Override
    public int compareTo(SortedMapBean o)
    {
        return rowkey.compareTo(o.rowkey);
    }

}
