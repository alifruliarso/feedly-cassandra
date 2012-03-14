package com.feedly.cassandra.dao;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedly.cassandra.IKeyspaceFactory;
import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;
import com.feedly.cassandra.entity.PropertyMetadata;

/*
 * used to fetch data using custom secondary indexes. Lazy loading is supported.
 */
public class RangeIndexFindHelper<K, V> extends LoadHelper<K, V>
{
    private static final Logger _logger = LoggerFactory.getLogger(RangeIndexFindHelper.class.getName());
    
    private final IndexedValueComparator<V> SORT_ASC = new IndexedValueComparator<V>(true);  
    private final IndexedValueComparator<V> SORT_DESC = new IndexedValueComparator<V>(false);  
    
    private final GetHelper<K, V> _getHelper;
    private final IStaleIndexValueStrategy _staleValueStrategy;
    RangeIndexFindHelper(EntityMetadata<V> meta, IKeyspaceFactory factory, IStaleIndexValueStrategy staleValueStrategy)
    {
        super(meta, factory);
        _getHelper = new GetHelper<K, V>(meta, factory);
        _staleValueStrategy = staleValueStrategy;
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
        RangeIndexQueryResult<K> result = findKeys(template, template, EFindOrder.NONE, options.getMaxRows(), index);
        
        IValueFilter<V> filter = new EqualityValueFilter<V>(_entityMeta, template, true, index);
        return new LazyLoadedCollection(result, filter, options, EFindOrder.NONE, index);
    }
    

    public Collection<V> mfindBetween(V startTemplate, V endTemplate, FindBetweenOptions options, IndexMetadata index)
    {
        RangeIndexQueryResult<K> result = findKeys(startTemplate, endTemplate, options.getRowOrder(), options.getMaxRows(), index);
        
        IValueFilter<V> f = new RangeValueFilter<V>(_entityMeta, startTemplate, endTemplate, index);
        return new LazyLoadedCollection(result, f, options, options.getRowOrder(), index);
    }


    private List<IndexedValue<V>> filterValues(List<StaleIndexValue> indexValues, List<V> values, IValueFilter<V> filter, IndexMetadata index)
    {
        List<StaleIndexValue> filtered = null;
        List<IndexedValue<V>> rv = new ArrayList<IndexedValue<V>>();
        int excludedCnt = 0;
        
        for(int i = values.size() - 1; i >= 0; i--)
        {
            V value = values.get(i);

            if(value == null)
            {
                if(filtered == null)
                    filtered = new ArrayList<StaleIndexValue>();

                filtered.add(indexValues.get(i));
            }
            else
            {
                IndexedValue<V> idxValue = indexedValue(value, index);
                EFilterResult result = filter.isFiltered(idxValue);
                
                if(result == EFilterResult.FAIL_STALE)
                {
                    if(filtered == null)
                        filtered = new ArrayList<StaleIndexValue>();

                    filtered.add(indexValues.get(i));
                }
                else if(result == EFilterResult.PASS)
                    rv.add(idxValue);
                else
                    excludedCnt++;
            }
        }
        
        if(filtered != null)
        {
            _staleValueStrategy.handle(_entityMeta, index, filtered);
            _logger.info("filtered {} stale values from index [{}]. {} excluded, retained {}", 
                         new Object[] { filtered.size(), index, excludedCnt, rv.size() });
        }
        else
            _logger.info("no stale rows filtered from index [{}]. {} excluded. retained {}", 
                         new Object[] {index, excludedCnt, rv.size()});
        
        return rv;
    }

    private IndexedValue<V> indexedValue(V v, IndexMetadata index)
    {
        List<PropertyMetadata> indexedProperties = index.getIndexedProperties();
        List<Object> indexValues = new ArrayList<Object>(indexedProperties.size());
        
        for(PropertyMetadata pm : indexedProperties)
        {
            Object propVal = invokeGetter(pm, v);
            if(propVal == null)
                break;
            
            indexValues.add(propVal);
        }
        return new IndexedValue<V>(indexValues, v);
    }

    private List<Object> indexValues(V template, IndexMetadata index) 
    {
        BitSet dirty = asEntity(template).getModifiedFields();
        Set<PropertyMetadata> modified = new HashSet<PropertyMetadata>();
        List<Object> propValues = new ArrayList<Object>();
        for(int i = dirty.nextSetBit(0); i>= 0; i = dirty.nextSetBit(i+1))
            modified.add(_entityMeta.getProperties().get(i));
        
        for(PropertyMetadata pm : index.getIndexedProperties())
        {
            if(modified.contains(pm))
                propValues.add(invokeGetter(pm, template));
            else
                break;
        }
        
        return propValues;
    }
    
    /*
     * index column family structure
     * row key:   idx_id:partition key 
     * column:    index value:rowkey
     * value:     meaningless
     */
    private RangeIndexQueryResult<K> findKeys(V startTemplate, V endTemplate, EFindOrder rowOrder, int maxKeys, IndexMetadata index)
    {
        List<Object> startPropVals = indexValues(startTemplate, index);
        List<Object> endPropVals;
        List<List<Object>> indexPartitions;
        if(startTemplate == endTemplate)
        {
            endPropVals = startPropVals;
            indexPartitions = index.getIndexPartitioner().partitionValue(startPropVals);
        }
        else
        {
            endPropVals = indexValues(endTemplate, index);
            indexPartitions = index.getIndexPartitioner().partitionRange(startPropVals, endPropVals);            
        }
        
        _logger.trace("reading from partitions {}", indexPartitions);
        List<DynamicComposite> rowKeys = new ArrayList<DynamicComposite>();
        for(List<Object> partition : indexPartitions)
        {
            DynamicComposite rowKey = new DynamicComposite();
            rowKey.add(index.id());
            for(Object pval :partition)
                rowKey.add(pval);
            rowKeys.add(rowKey);
        }

        DynamicComposite startCol = new DynamicComposite(startPropVals);
        startCol.setEquality(ComponentEquality.EQUAL);
        DynamicComposite endCol = new DynamicComposite(endPropVals);
        endCol.setEquality(ComponentEquality.GREATER_THAN_EQUAL);
        
        return fetchInitialBatch(rowKeys.toArray(new DynamicComposite[rowKeys.size()]), startCol, endCol, rowOrder, index);
    }
    
    @SuppressWarnings("unchecked")
    private RangeIndexQueryResult<K> fetchInitialBatch(DynamicComposite[] partitionKeys, 
                                                       DynamicComposite startCol,
                                                       DynamicComposite endCol,
                                                       EFindOrder colOrder, 
                                                       IndexMetadata index)
    {
        RangeIndexQueryResult<K> rv = new RangeIndexQueryResult<K>();
        List<K> currentKeys = rv.getCurrentKeys();
        List<StaleIndexValue> currentValues = rv.getCurrentValues();
        List<RangeIndexQueryPartitionResult> partitionResults = rv.getPartitionResults();
        
        if(colOrder == EFindOrder.NONE) //can attempt to find all rows at once
        {
            MultigetSliceQuery<DynamicComposite,DynamicComposite,byte[]> multiGetQuery =
                    HFactory.createMultigetSliceQuery(_keyspaceFactory.createKeyspace(), SER_COMPOSITE, SER_COMPOSITE, SER_BYTES);
            
            multiGetQuery.setKeys(partitionKeys);
            multiGetQuery.setColumnFamily(_entityMeta.getIndexFamilyName());
            Rows<DynamicComposite,DynamicComposite,byte[]> indexRows;
            multiGetQuery.setRange(startCol, endCol, false, CassandraDaoBase.COL_RANGE_SIZE);
            indexRows = multiGetQuery.execute().get();

            boolean hasMore = false;
            rv.setPartitionPos(partitionKeys.length - 1); //init to no additional results
            int partitionIdx = 0;

            for(Row<DynamicComposite, DynamicComposite, byte[]> row : indexRows)
            {
                RangeIndexQueryPartitionResult partitionResult = new RangeIndexQueryPartitionResult();
                partitionResult.setPartitionKey(row.getKey());

                List<HColumn<DynamicComposite,byte[]>> columns = row.getColumnSlice().getColumns();
                for(HColumn<DynamicComposite, byte[]> col : columns)
                {
                    currentKeys.add((K) col.getName().get(col.getName().size()-1));
                    currentValues.add(new StaleIndexValue(row.getKey(), col.getName(), col.getClock()));
                }

                
                if(columns.size() == CassandraDaoBase.COL_RANGE_SIZE)
                {
                    partitionResult.setHasMore(true);
                    if(!hasMore)
                    {
                        rv.setPartitionPos(partitionIdx);
                        hasMore = true;
                    }
                    
                    DynamicComposite start = columns.get(CassandraDaoBase.COL_RANGE_SIZE - 1).getName();
                    start.setEquality(ComponentEquality.GREATER_THAN_EQUAL);
                    partitionResult.setStartCol(start);
                    partitionResult.setEndCol(endCol);
                }
                else
                {
                    partitionResult.setStartCol(startCol);
                    partitionResult.setEndCol(endCol);
                    partitionResult.setHasMore(false);
                }
                
                partitionResults.add(partitionResult);
                partitionIdx++;
                
                if(!hasMore)
                    rv.setPartitionPos(partitionIdx);
            }
        }
        else 
        {
            /*
             * ordered results requested, need to proceed through the partitions in order, this means a multi get cannot be done...
             */
            
            if(colOrder == EFindOrder.DESCENDING)
            {
                partitionKeys = partitionKeys.clone();
                ArrayUtils.reverse(partitionKeys);
            }
            
            boolean foundResults = false;
            
            /*
             * find the first partition with some results and fetch them, set the remaining partitions up for a later fetch
             */
            for(int i = 0; i < partitionKeys.length; i++)
            {
                if(!foundResults)
                {
                    RangeIndexQueryPartitionResult pr = new RangeIndexQueryPartitionResult();
                    pr.setPartitionKey(partitionKeys[i]);
                    pr.setHasMore(true);
                    pr.setStartCol(startCol);
                    pr.setEndCol(endCol);
                    
                    int fetchCnt = fetchFromPartition(rv, pr, colOrder, 0);
                    foundResults = fetchCnt > 0;
                    
                    if(foundResults)
                        partitionResults.add(pr);
                }
                else
                {
                    RangeIndexQueryPartitionResult partitionResult = new RangeIndexQueryPartitionResult();
                    partitionResult.setPartitionKey(partitionKeys[i]);
                    partitionResult.setHasMore(true);
                    partitionResult.setStartCol(startCol);
                    partitionResult.setEndCol(endCol);
                    partitionResults.add(partitionResult);
                }
            }
        }

        _logger.info("initial fetch: index [{}] {} - {}, found {} keys", new Object[]{ index, startCol, endCol, rv.getCurrentKeys().size() });  

        return rv;
    }
    
    private void fetchBatch(RangeIndexQueryResult<K> result, int maxRows, EFindOrder order, IndexMetadata index)
    {
        if(!result.hasMore())
            return;
        
        result.getCurrentKeys().clear();
        result.getCurrentValues().clear();

        int numPartitions = result.getPartitionResults().size();
        List<RangeIndexQueryPartitionResult> partitionResults = result.getPartitionResults();
        
        int fetchCnt = 0;
        for(int i = result.getPartitionPos(); i < numPartitions; i++)
        {
            RangeIndexQueryPartitionResult p = partitionResults.get(i);
            if(p.hasMore())
            {
                fetchCnt = fetchFromPartition(result, p, order, i);
                
                if(fetchCnt > 0)
                    break;
            }
        }
        
        if(fetchCnt == 0)
        {
            //no results
            result.setPartitionPos(numPartitions);
            result.setCurrentKeys(Collections.<K>emptyList());
            result.setCurrentValues(Collections.<StaleIndexValue>emptyList());
        }

    }



    private List<HColumn<DynamicComposite, byte[]>> executeSliceQuery(DynamicComposite partitionKey,
                                                                      DynamicComposite startCol,
                                                                      DynamicComposite endCol,
                                                                      EFindOrder colOrder)
    {
        SliceQuery<DynamicComposite,DynamicComposite,byte[]> query =
                HFactory.createSliceQuery(_keyspaceFactory.createKeyspace(), SER_COMPOSITE, SER_COMPOSITE, SER_BYTES);
        
        query.setKey(partitionKey);
        query.setColumnFamily(_entityMeta.getIndexFamilyName());
        
        if(colOrder == EFindOrder.DESCENDING)
            query.setRange(endCol, startCol, true, CassandraDaoBase.COL_RANGE_SIZE);
        else
            query.setRange(startCol, endCol, false, CassandraDaoBase.COL_RANGE_SIZE);
            
        List<HColumn<DynamicComposite,byte[]>> columns = query.execute().get().getColumns();
        return columns;
    }
    
    @SuppressWarnings("unchecked")
    private int fetchFromPartition(RangeIndexQueryResult<K> result,
                                   RangeIndexQueryPartitionResult p, 
                                   EFindOrder order,
                                   int pos)
    {
        List<HColumn<DynamicComposite,byte[]>> columns = executeSliceQuery(p.getPartitionKey(), 
                                                                           p.getStartCol(), 
                                                                           p.getEndCol(), 
                                                                           order); 
    
        if(!columns.isEmpty())
        {
            List<K> keys = result.getCurrentKeys();
            List<StaleIndexValue> values = result.getCurrentValues();
            for(HColumn<DynamicComposite, byte[]> col : columns)
            {
                keys.add((K) col.getName().get(col.getName().size()-1));
                values.add(new StaleIndexValue(p.getPartitionKey(), col.getName(), col.getClock()));
            }
    
            if(order == EFindOrder.DESCENDING)
            {
                DynamicComposite end = columns.get(0).getName();
                end.setEquality(ComponentEquality.LESS_THAN_EQUAL);
                p.setEndCol(end);
            }
            else
            {
                DynamicComposite start = columns.get(columns.size() - 1).getName();
                start.setEquality(ComponentEquality.GREATER_THAN_EQUAL);
                p.setStartCol(start);
            }
    
            boolean hasMore = columns.size() == CassandraDaoBase.COL_RANGE_SIZE;

            _logger.info("fetched {} keys from partition[{}] ({}), has more == {}", 
                         new Object[] {columns.size(), pos, p.getPartitionKey(), hasMore});

            p.setHasMore(hasMore);
            result.setPartitionPos(hasMore ? pos : pos+1);
            
        }
    
        return columns.size();
    }

    private List<V> toRows(RangeIndexQueryResult<K> result, FindOptions options, EFindOrder order, IValueFilter<V> filter, IndexMetadata index)
    {
        if(options.getColumnFilterStrategy() == EColumnFilterStrategy.INCLUDES)
        {
            Set<Object> partialProperties = new HashSet<Object>(partialProperties(options.getIncludes(), options.getExcludes()));
            for(PropertyMetadata pm : index.getIndexedProperties())
                partialProperties.add(pm.getName());
            
            try
            {
                options = (FindOptions) options.clone();
                options.setIncludes(partialProperties);
                options.setExcludes(null);
            }
            catch(CloneNotSupportedException ex)
            {
                throw new RuntimeException(ex);
            }
        }

        List<V> rows = _getHelper.mget(result.getCurrentKeys(), null, options);
        
        
        
        List<IndexedValue<V>> values = filterValues(result.getCurrentValues(), rows, filter, index);
        rows.clear();
        
        if(!values.isEmpty())
        {
            if(order == EFindOrder.ASCENDING)
                Collections.sort(values, SORT_ASC);
            else if(order == EFindOrder.DESCENDING)
                Collections.sort(values, SORT_DESC);

            for(IndexedValue<V> v : values)
                rows.add(v.getValue());
        }
        
        return rows;
    }
    
//    private class IndexValuesComparator implements Comparable
    
    private class LazyLoadedIterator implements Iterator<V>
    {
        private int _remRows; //remaining rows left to fetch, based on max set by user and if the last batch fetched was maximal
        private List<V> _current;
        private Iterator<V> _currentIter;
        private V _next;
        private int _iteratedCnt = 0;
        private final FindOptions _options;
        private final RangeIndexQueryResult<K> _result;
        private final EFindOrder _order;
        private final IndexMetadata _index;
        private final IValueFilter<V> _filter;
        private final LazyLoadedCollection _parent;
        
        @SuppressWarnings("unchecked")
        public LazyLoadedIterator(LazyLoadedCollection parent, 
                                  List<V> first,
                                  RangeIndexQueryResult<K> result,
                                  IValueFilter<V> filter,
                                  FindOptions options,
                                  EFindOrder order,
                                  IndexMetadata index)
        {
            _parent = parent;
            _current = first;
            _currentIter = first.iterator();
            _next = _currentIter.next();
            _filter = filter;
            _options = options;
            _order = order;
            _index = index;
            
            try
            {
                _result = (RangeIndexQueryResult<K>) result.clone();
            }
            catch(CloneNotSupportedException ex)
            {
                throw new RuntimeException(ex);//should never happen
            }
            
            if(_result.hasMore())
                _remRows = options.getMaxRows() - first.size();
            else
                _remRows = 0;
        }


        @Override
        public boolean hasNext()
        {
            return _next != null;
        }

        @Override
        public V next()
        {
            if(_next == null)
                throw new NoSuchElementException();
            
            V rv = _next;
            
            if(_currentIter.hasNext())
                _next = _currentIter.next();
            else if(_remRows == 0)
            {
                _next = null;
            }
            else //fetch next batch
            {
                List<V> rows = Collections.emptyList();
                while(rows.isEmpty() && _result.hasMore())
                {
                    fetchBatch(_result, _options.getMaxRows(), _order, _index);
                    rows = toRows(_result, _options, _order, _filter, _index);
                }
                
                if(rows.size() >= _remRows)
                {
                    if(rows.size() > _remRows) //trim
                    {
                        if(_order == EFindOrder.DESCENDING)
                            rows = rows.subList(rows.size() - _remRows, rows.size());
                        else
                            rows = rows.subList(0, _remRows);
                        
                        rows = new ArrayList<V>(rows); //detach from original list
                    }
                }
                
                _remRows = Math.max(0, _remRows - rows.size());
                
                _current = rows;
                
                if(_current.isEmpty())
                {
                    _current = null;
                    _currentIter = null;
                    _next = null;
                }
                else
                {
                    _currentIter = _current.iterator();
                    _next = _currentIter.next();
                }
            }
            
            _iteratedCnt++;
            if(_next == null)
                _parent.setSize(_iteratedCnt); //we have a row count, notify parent so subsequent calls to size() don't have to fetch all rows
            
            return rv;
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
    
    
    private class LazyLoadedCollection extends AbstractCollection<V>
    {
        private RangeIndexQueryResult<K> _result;
        private IValueFilter<V> _filter;
        private FindOptions _options;
        private EFindOrder _order;
        private IndexMetadata _index;
        private List<V> _all = null; //if it is known all rows have been fetched, this field is set
        private List<V> _first;
        private int _size = -1;
        
        public LazyLoadedCollection(RangeIndexQueryResult<K> result,
                                    IValueFilter<V> filter,
                                    FindOptions options,
                                    EFindOrder order,
                                    IndexMetadata index)
        {
            _result = result;
            _filter = filter;
            _options = options;
            _order = order;
            _index = index;
            int maxRows = _options.getMaxRows();
            
            List<V> rows = toRows(result, options, order, filter, index);

            while(rows.isEmpty() && result.hasMore())
            {
                fetchBatch(result, options.getMaxRows(), order, index);
                rows = toRows(result, options, order, filter, index);
            }
            
            if(!result.hasMore() || result.getCurrentKeys().size() >= maxRows)
            {
                if(rows.size() >= maxRows)
                {
                    if(rows.size() > maxRows) //trim
                    {
                        if(order == EFindOrder.DESCENDING)
                            rows = rows.subList(rows.size() - maxRows, rows.size());
                        else
                            rows = rows.subList(0, maxRows);
                        
                        rows = new ArrayList<V>(rows); //detach from original list
                    }
                }
                
                _all = rows;
                _first = rows;
            }
            else
                _first = rows; //will need to iterate...
            
        }

        //override, don't want to invoke size, just to check if empty
        @Override
        public boolean isEmpty()
        {
            return _first.isEmpty();
        }
        
        //can aggressively fetch and retain all values, use with caution
        @Override
        public int size()
        {
            if(_size >= 0)
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
            
            return new LazyLoadedIterator(this, _first, _result, _filter, _options, _order, _index);
        }
        
        void setSize(int size)
        {
            _size = size;
        }
    }

}
