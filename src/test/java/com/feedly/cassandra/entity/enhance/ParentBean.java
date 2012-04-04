package com.feedly.cassandra.entity.enhance;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.anno.RowKey;
import com.feedly.cassandra.entity.EntityUtils;

@ColumnFamily(name="parentbean")
public class ParentBean implements Comparable<ParentBean>
{
    @RowKey
    private Long rowkey;
    
    @Column(name="l")
    private List<EmbeddedBean> listProp;

    @Column(name="m")
    private Map<String, EmbeddedBean> mapProp;

    @Column(name="s")
    private String strProp;

    @Column(name="e")
    private EmbeddedBean embeddedProp;
    
    public List<EmbeddedBean> getListProp()
    {
        return listProp;
    }

    public void setListProp(List<EmbeddedBean> listProp)
    {
        this.listProp = listProp;
    }

    public Map<String, EmbeddedBean> getMapProp()
    {
        return mapProp;
    }

    public void setMapProp(Map<String, EmbeddedBean> mapProp)
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

    public EmbeddedBean getEmbeddedProp()
    {
        return embeddedProp;
    }

    public void setEmbeddedProp(EmbeddedBean embeddedProp)
    {
        this.embeddedProp = embeddedProp;
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof ParentBean)
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
    public int compareTo(ParentBean o)
    {
        return rowkey.compareTo(o.rowkey);
    }
    

}
