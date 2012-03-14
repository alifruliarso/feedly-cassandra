package com.feedly.cassandra.dao;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;

import com.feedly.cassandra.IKeyspaceFactory;
import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;
import com.feedly.cassandra.entity.PropertyMetadata;

/*
 * used to fetch data using native cassandra indexes. Lazy loading is supported.
 */
public class HashIndexFindHelper<K, V> extends LoadHelper<K, V>
{
    HashIndexFindHelper(EntityMetadata<V> meta, IKeyspaceFactory factory)
    {
        super(meta, factory);
    }

    private V uniqueValue(Collection<V> values)
    {
        if(values == null || values.isEmpty())
            return null;
        
        if(values.size() > 1)
            throw new IllegalStateException("non-unique value");
        
        return values.iterator().next();
    }
    
    public V find(V template, FindOptions options, IndexMetadata index)
    {
        return uniqueValue(mfind(template, options, index));
    }
    

    public Collection<V> mfind(V template, FindOptions options, IndexMetadata index)
    {
        switch(options.getColumnFilterStrategy())
        {
            case UNFILTERED:
                return bulkFindByIndexPartial(template, null, null, null, null, options.getMaxRows(), index);
            case RANGE:
                byte[] startCol = propertyName(options.getStartColumn());
                byte[] endCol = propertyName(options.getEndColumn());
                return bulkFindByIndexPartial(template, startCol, endCol, null, null, options.getMaxRows(), index);
            case INCLUDES:
                return mfind(template, options.getIncludes(), options.getExcludes(), options.getMaxRows(), index);
        }
        
        throw new IllegalStateException(); //never happens
    }

    private Collection<V> mfind(V template, Set<? extends Object> includes, Set<String> excludes, int maxRows, IndexMetadata index)
    {
        if(includes != null && excludes != null)
            throw new IllegalArgumentException("either includes or excludes should be specified, not both");
        
        if(includes != null && excludes != null)
            throw new IllegalArgumentException("either includes or excludes should be specified, not both");
        
        List<byte[]> colNames = new ArrayList<byte[]>();
        List<PropertyMetadata> fullCollectionProperties = derivePartialColumns(colNames, includes, excludes);

        return bulkFindByIndexPartial(template, null, null, colNames, fullCollectionProperties, maxRows, index);
        
    }

    private Collection<V> bulkFindByIndexPartial(V template, 
                                                 byte[] startBytes, 
                                                 byte[] endBytes, 
                                                 List<byte[]> colNames,
                                                 List<PropertyMetadata> fullCollectionProperties,
                                                 int maxRows, 
                                                 IndexMetadata index)
    {
        PropertyMetadata pm = index.getIndexedProperties().get(0); //must be exactly 1
                
        Object propVal = invokeGetter(pm, template);
        if(propVal == null)
            throw new IllegalArgumentException("null values not supported for hash indexes");
        
        IndexedSlicesQuery<byte[], byte[], byte[]> query = HFactory.createIndexedSlicesQuery(_keyspaceFactory.createKeyspace(), SER_BYTES, SER_BYTES, SER_BYTES);
        query.setColumnFamily(_entityMeta.getFamilyName());
        query.setRowCount(CassandraDaoBase.ROW_RANGE_SIZE);
        query.addEqualsExpression(pm.getPhysicalNameBytes(), serialize(propVal, false, pm.getSerializer()));

        if(colNames != null)
            query.setColumnNames(colNames);
        else
            query.setRange(startBytes, endBytes, false, CassandraDaoBase.COL_RANGE_SIZE);

        return new LazyLoadedCollection(query, 
                                        endBytes, 
                                        fullCollectionProperties, 
                                        new EqualityValueFilter<V>(_entityMeta, template, false, index), 
                                        maxRows,
                                        index);
    }
    
    @SuppressWarnings("unchecked")
    private byte[] fetchBatch(IndexedSlicesQuery<byte[],byte[],byte[]> query, 
                               byte[] startRowKey, 
                               byte[] endColBytes, 
                               K lastKey, 
                               int maxRows, 
                               List<V> values, 
                               List<PropertyMetadata> fullCollectionProperties)
    {
        PropertyMetadata keyMeta = _entityMeta.getKeyMetadata();
        int fetchRowCount = Math.min(maxRows, CassandraDaoBase.ROW_RANGE_SIZE);
        query.setRowCount(fetchRowCount);

        if(startRowKey != null)
            query.setStartKey(startRowKey);

        OrderedRows<byte[],byte[],byte[]> rows = query.execute().get();
        List<K> keys = new ArrayList<K>();
        
        K first = null, last = null;
        /*
         * the last key of the previous range and the first key of the current range may overlap
         */
        for(Row<byte[], byte[], byte[]> row : rows)
        {
            K key = (K) keyMeta.getSerializer().fromBytes(row.getKey());

            if(lastKey != null)
            {
                if(key.equals(lastKey))
                {
                    lastKey = null;
                    continue;
                }

                lastKey = null;
            }
            

            V value = fromColumnSlice(key, null, keyMeta, row.getKey(), null, row.getColumnSlice(), endColBytes);
            
            keys.add(key);
            values.add(value);
            
            if(first == null)
                first = key;
            
            last = key;
        }

        _logger.debug("range {} - {}", first, last);

        if(fullCollectionProperties != null && !values.isEmpty())
        {
            _logger.debug("adding full collections to {} values: ({})", values.size(), fullCollectionProperties);
            addFullCollectionProperties(keys, values, fullCollectionProperties);
        }
        
        int cnt = rows.getCount();
                
        byte[] lastKeyBytes = cnt == 0 ? null : rows.getList().get(cnt - 1).getKey();
        int nonNull = 0;
        for(int i = values.size() - 1; i >= 0; i--)
        {
            if(values.get(i) == null) 
                values.remove(i);
            else 
                nonNull++;
        }

        _logger.info("{} rows, {} values, ({} non null) fetched", new Object[] {cnt, values.size(), nonNull});

        return lastKeyBytes;
    }
    
        
    private class LazyLoadedIterator implements Iterator<V>
    {
        private final LazyLoadedCollection _parent;
        private final IndexedSlicesQuery<byte[],byte[],byte[]> _query;
        private final IValueFilter<V> _filter;
        private final IndexMetadata _index;
        private byte[] _nextStartKey;
        private final byte[] _endCol;
        private int _remRows; //remaining rows left to fetch, based on max set by user and if the last batch fetched was maximal
        private List<V> _current;
        private Iterator<V> _currentIter;
        private V _next;
        private List<PropertyMetadata> _fullCollectionProperties;
        private int _iteratedCount = 0;
        
        public LazyLoadedIterator(LazyLoadedCollection parent,
                                  List<V> first,
                                  IndexedSlicesQuery<byte[],byte[],byte[]> query, 
                                  byte[] nextStartKey, 
                                  byte[] endCol, 
                                  List<PropertyMetadata> fullCollectionProperties, 
                                  IValueFilter<V> filter,
                                  int maxRows,
                                  IndexMetadata index)
        {
            _parent = parent;
            _filter = filter;
            _index = index;
            _current = first;
            _currentIter = first.iterator();
            _next = _currentIter.next();
            _query = query;
            _nextStartKey = nextStartKey;
            _fullCollectionProperties = fullCollectionProperties;
            
            if(first.size() < CassandraDaoBase.ROW_RANGE_SIZE)
                _remRows = 0;
            else
                _remRows = maxRows - first.size();
            
            _endCol = endCol;
        }

        
        @Override
        public boolean hasNext()
        {
            return _next != null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public V next()
        {
            if(_next == null)
                throw new NoSuchElementException();
            
            _iteratedCount++;
            V rv = _next;
            
            V next = null;
            
            while(next == null)
            {
                while(_currentIter.hasNext())
                {
                    next = _currentIter.next();
                    if(_filter.isFiltered(indexedValue(next, _index)) == EFilterResult.PASS)
                        break;
                    else
                        _logger.debug("filtered {}", next);
                }
                
                if(next == null) 
                {
                    if(_remRows > 0) //fetch next batch
                    {
                        PropertyMetadata keyMeta = _entityMeta.getKeyMetadata();
                        V last = _current.get(_current.size() - 1);
                        K lastKey = (K) invokeGetter(keyMeta, last);
                        _current.clear();
                        _nextStartKey = fetchBatch(_query, _nextStartKey, _endCol, lastKey, _remRows, _current, _fullCollectionProperties);
                        
                        int cnt = _current.size();
                        if(cnt == 0) //get yielded no rows
                        {
                            next = null;
                            _remRows = 0;
                            _logger.debug("empty fetch, no more values");
                            break;
                        }
                        else
                        {
                            if(cnt < CassandraDaoBase.ROW_RANGE_SIZE - 1) //allow for query range boundary duplications
                                _remRows = 0;
                            else
                                _remRows -= cnt;
                            
                            _currentIter = _current.iterator();
                        }
                    }
                    else //no more rows
                    {
                        _logger.debug("remaining rows zero");
                        break;
                    }
                }
            }
            
            if(next == null) //have iterated through all results, cache size
                _parent.setSize(_iteratedCount);
            
            _next = next;
            
            return rv;
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
    
    private IndexedValue<V> indexedValue(V value, IndexMetadata index)
    {
        PropertyMetadata pm = index.getIndexedProperties().get(0);
        return new IndexedValue<V>(Collections.singletonList(invokeGetter(pm, value)), value);
    }
    
    private class LazyLoadedCollection extends AbstractCollection<V>
    {
        private byte[] _nextRowKeyBytes;
        private byte[] _endColBytes;
        private int _maxRows;
        private IndexedSlicesQuery<byte[], byte[], byte[]> _query;
        private final IValueFilter<V> _filter;
        private final IndexMetadata _index;
        private Integer _size;
        private List<V> _all = null; //if it is known all rows have been fetched, this field is set
        private List<V> _first = new ArrayList<V>();
        private final List<PropertyMetadata> _fullCollectionProperties;
        
        @SuppressWarnings("unchecked")
        public LazyLoadedCollection(IndexedSlicesQuery<byte[], byte[], byte[]> query, 
                                    byte[] endColBytes, 
                                    List<PropertyMetadata> fullCollectionProperties,
                                    IValueFilter<V> filter,
                                    int maxRows,
                                    IndexMetadata index)
        {
            _query = query;
            _endColBytes = endColBytes;
            _maxRows = maxRows;
            _filter = filter;
            _index = index;
            _fullCollectionProperties = fullCollectionProperties;
            K lastKey = null;
            while(true) //loop until unfiltered rows are found
            {
                _nextRowKeyBytes = fetchBatch(query, _nextRowKeyBytes, endColBytes, lastKey, maxRows, _first, _fullCollectionProperties);
                if(_first.isEmpty())
                    break;
                
                Iterator<V> iter = _first.iterator();
                if(!_first.isEmpty())
                    lastKey = (K) invokeGetter(_entityMeta.getKeyMetadata(), _first.get(_first.size() - 1));
                while(iter.hasNext())
                {
                    V next = iter.next();
                    if(filter.isFiltered(indexedValue(next, index)) != EFilterResult.PASS)
                        iter.remove();
                }
                
                if(!_first.isEmpty())
                    break;
            }

            if(_first.size() == maxRows || _first.isEmpty()) //loaded all rows or none exist
            {
                _all = _first;
                
            }
        }
        
        //override, don't want to invoke size, just to check if empty
        @Override
        public boolean isEmpty()
        {
            return _first.isEmpty();
        }
        
        void setSize(int size)
        {
            _size = size;
        }
        
        //may aggressively fetch and retain all values, use with caution
        @Override
        public int size()
        {
            if(_size != null)
                return _size;
            if(_all == null)
            {
                Iterator<V> iter = iterator();
                _all = new ArrayList<V>();
                while(iter.hasNext())
                    _all.add(iter.next());
            }
            
            return _all.size();
        }
        
        @Override
        public java.util.Iterator<V> iterator()
        {
            if(_all != null)
                return _all.iterator();
            
            return new LazyLoadedIterator(this, _first, _query, _nextRowKeyBytes, _endColBytes, _fullCollectionProperties, _filter, _maxRows, _index);
        }
    }
}
