package com.feedly.cassandra.entity.enhance;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.anno.RowKey;
import com.feedly.cassandra.dao.CounterColumn;
import com.feedly.cassandra.entity.EntityUtils;

@ColumnFamily(name="parentcounterbean")
public class ParentCounterBean implements Comparable<ParentCounterBean>, Cloneable
{
    @RowKey
    private Long rowkey;
    
    @Column(name="l")
    private List<EmbeddedCounterBean> listProp;

    @Column(name="m")
    private Map<String, EmbeddedCounterBean> mapProp;

    @Column(name="s", hashIndexed=true)
    private String strProp;

    @Column(name="c")
    private CounterColumn counterProp;

    @Column(name="e")
    private EmbeddedCounterBean embeddedProp;
    
    public List<EmbeddedCounterBean> getListProp()
    {
        return listProp;
    }

    public void setListProp(List<EmbeddedCounterBean> listProp)
    {
        this.listProp = listProp;
    }

    public Map<String, EmbeddedCounterBean> getMapProp()
    {
        return mapProp;
    }

    public void setMapProp(Map<String, EmbeddedCounterBean> mapProp)
    {
        this.mapProp = mapProp;
    }

    public String getStrProp()
    {
        return strProp;
    }

    public void setStrProp(String strProp)
    {
        this.strProp = strProp;
    }

    public Long getRowkey()
    {
        return rowkey;
    }

    public void setRowkey(Long rowkey)
    {
        this.rowkey = rowkey;
    }

    public EmbeddedCounterBean getEmbeddedProp()
    {
        return embeddedProp;
    }

    public void setEmbeddedProp(EmbeddedCounterBean embeddedProp)
    {
        this.embeddedProp = embeddedProp;
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
        if(obj instanceof ParentCounterBean)
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
    
    @Override
    public int compareTo(ParentCounterBean o)
    {
        return rowkey.compareTo(o.rowkey);
    }

}
