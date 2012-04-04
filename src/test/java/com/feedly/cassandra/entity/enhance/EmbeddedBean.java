package com.feedly.cassandra.entity.enhance;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.EmbeddedEntity;
import com.feedly.cassandra.anno.UnmappedColumnHandler;
import com.feedly.cassandra.entity.EntityUtils;

@EmbeddedEntity
public class EmbeddedBean
{
    @Column(name="l")
    private List<Integer> listProp;

    @Column(name="m")
    private Map<String, Integer> mapProp;

    @Column(name="s")
    private String strProp;

    @Column(name="d")
    private double doubleProp;

    @UnmappedColumnHandler
    Map<String, Object> unmappedHandler;
    
    public List<Integer> getListProp()
    {
        return listProp;
    }

    public void setListProp(List<Integer> listProp)
    {
        this.listProp = listProp;
    }

    public Map<String, Integer> getMapProp()
    {
        return mapProp;
    }

    public void setMapProp(Map<String, Integer> mapProp)
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
    
    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof EmbeddedBean)
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

    public Map<String, Object> getUnmappedHandler()
    {
        return unmappedHandler;
    }

    public void setUnmappedHandler(Map<String, Object> unmappedHandler)
    {
        this.unmappedHandler = unmappedHandler;
    }

    public double getDoubleProp()
    {
        return doubleProp;
    }

    public void setDoubleProp(double doubleProp)
    {
        this.doubleProp = doubleProp;
    }

}
