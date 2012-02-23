package com.feedly.cassandra.entity.enhance;

import java.lang.reflect.Field;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.anno.RowKey;
import com.feedly.cassandra.entity.EntityUtils;
import com.feedly.cassandra.entity.TestPartitioner;

@ColumnFamily(name="pib")
public class PartitionedIndexBean implements Comparable<PartitionedIndexBean>
{
    @RowKey
    private Long rowKey;

    @Column(rangeIndexed=true, rangeIndexPartitioner=TestPartitioner.class)
    private Long partitionedValue;
    
    public Long getRowKey()
    {
        return rowKey;
    }

    public void setRowKey(Long rowKey)
    {
        this.rowKey = rowKey;
    }

    public Long getPartitionedValue()
    {
        return partitionedValue;
    }

    public void setPartitionedValue(Long partitionedValue)
    {
        this.partitionedValue = partitionedValue;
    }
    
    
    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof PartitionedIndexBean)
            return EntityUtils.beanFieldsEqual(this, obj);

        return false;
    }

    @Override
    public int compareTo(PartitionedIndexBean o)
    {
        return rowKey.compareTo(o.rowKey);
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException
    {
        PartitionedIndexBean clone = (PartitionedIndexBean) super.clone();
        
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
