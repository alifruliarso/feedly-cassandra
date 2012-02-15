package com.feedly.cassandra.bean.enhance;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.anno.RowKey;
import com.feedly.cassandra.anno.UnmappedColumnHandler;
import com.feedly.cassandra.bean.BeanUtils;

@ColumnFamily(name="mapbean")
public class MapBean implements Comparable<MapBean>, Cloneable
{
    @RowKey
    private Long rowkey;
    
    @Column
    private String strProp1;
    
    @Column
    private String strProp;
    
    @Column
    private Map<String, Object> mapProp;

    @UnmappedColumnHandler(StringSerializer.class)
    private Map<String, String> unmapped;

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

    public Map<String, Object> getMapProp()
    {
        return mapProp;
    }

    public void setMapProp(Map<String, Object> mapProp)
    {
        this.mapProp = mapProp;
    }

    public Map<String, String> getUnmapped()
    {
        return unmapped;
    }

    public void setUnmapped(Map<String, String> unmapped)
    {
        this.unmapped = unmapped;
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof MapBean)
            return BeanUtils.beanFieldsEqual(this, obj);

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
    public int compareTo(MapBean o)
    {
        return rowkey.compareTo(o.rowkey);
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException
    {
        MapBean clone = (MapBean) super.clone();
        clone.setMapProp(new HashMap<String, Object>(mapProp));
        
        return clone;
    }
}
