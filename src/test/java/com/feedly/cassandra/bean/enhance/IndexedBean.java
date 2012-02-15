package com.feedly.cassandra.bean.enhance;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.anno.RowKey;

@ColumnFamily(name="indexedbean")
public class IndexedBean
{
    @RowKey
    private Long rowKey;
    
    @Column(col="s", indexed = true)
    private String strVal;
    
    @Column(indexed=true)
    private int intVal;

    @Column
    private long longVal;
    
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
}
