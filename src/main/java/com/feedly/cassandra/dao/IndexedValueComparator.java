package com.feedly.cassandra.dao;

import java.util.Comparator;
import java.util.List;

class IndexedValueComparator<V> implements Comparator<IndexedValue<V>>
{
    private final int _sortAsc;
    
    public IndexedValueComparator(boolean sortAsc)
    {
        _sortAsc = sortAsc ? 1 : -1;
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public int compare(IndexedValue<V> o1, IndexedValue<V> o2)
    {
        List<Object> vals1 = o1.getIndexValues();
        List<Object> vals2 = o2.getIndexValues();
        
        int size1 = vals1.size();
        int size2 = vals2.size();

        for(int i = 0; i < size1; i++)
        {
            if(i == size2)
                return _sortAsc;
            
            Comparable cmp1 = (Comparable) vals1.get(i);
            Comparable cmp2 = (Comparable) vals2.get(i);
            
            int result = cmp1.compareTo(cmp2);
            if(result != 0)
                return _sortAsc * result;
        }
        
        return _sortAsc * (size1 - size2);
    }

}
