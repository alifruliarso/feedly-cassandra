package com.feedly.cassandra.dao;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import me.prettyprint.cassandra.serializers.AsciiSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import org.junit.Before;
import org.junit.Test;

import com.feedly.cassandra.PersistenceManager;
import com.feedly.cassandra.entity.ByteIndicatorSerializer;
import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.EntityUtils;
import com.feedly.cassandra.entity.IndexMetadata;
import com.feedly.cassandra.entity.TestPartitioner;
import com.feedly.cassandra.entity.enhance.CompositeIndexedBean;
import com.feedly.cassandra.entity.enhance.IEnhancedEntity;
import com.feedly.cassandra.entity.enhance.IndexedBean;
import com.feedly.cassandra.entity.enhance.ListBean;
import com.feedly.cassandra.entity.enhance.MapBean;
import com.feedly.cassandra.entity.enhance.PartitionedIndexBean;
import com.feedly.cassandra.entity.enhance.SampleBean;
import com.feedly.cassandra.entity.enhance.SortedMapBean;
import com.feedly.cassandra.test.CassandraServiceTestBase;

@SuppressWarnings({"unchecked", "rawtypes"})
public class CassandraDaoBaseTest extends CassandraServiceTestBase
{
    PersistenceManager _pm;
    SampleBeanDao _dao;
    MapBeanDao _mapDao;
    SortedMapBeanDao _sortedMapDao;
    ListBeanDao _listDao;
    IndexedBeanDao _indexedDao;
    CompositeIndexedBeanDao _compositeIndexedDao;
    RecordingStrategy _indexedStrategy, _compositeStrategy;
    
    @Before
    public void before()
    {
        _pm = new PersistenceManager();
        _dao = new SampleBeanDao();
        _dao.setKeyspaceFactory(_pm);
        _dao.init();
        
        _mapDao = new MapBeanDao();
        _mapDao.setKeyspaceFactory(_pm);
        _mapDao.init();
        
        _sortedMapDao = new SortedMapBeanDao();
        _sortedMapDao.setKeyspaceFactory(_pm);
        _sortedMapDao.init();
        
        _listDao = new ListBeanDao();
        _listDao.setKeyspaceFactory(_pm);
        _listDao.init();
        
        _indexedDao = new IndexedBeanDao();
        _indexedDao.setKeyspaceFactory(_pm);
        _indexedStrategy = new RecordingStrategy();
        _indexedDao.setStaleValueIndexStrategy(_indexedStrategy);
        _indexedDao.init();
        
        _compositeIndexedDao = new CompositeIndexedBeanDao();
        _compositeIndexedDao.setKeyspaceFactory(_pm);
        _compositeStrategy = new RecordingStrategy();
        _compositeIndexedDao.setStaleValueIndexStrategy(_compositeStrategy);
        _compositeIndexedDao.init();
        
        configurePersistenceManager(_pm);
        
        _pm.setPackagePrefixes(new String[] {SampleBean.class.getPackage().getName()});
        _pm.init();
    }

    
    private void assertBean(String msg, SampleBean bean, ColumnSlice<String, byte[]> columnSlice)
    {
        assertEquals(msg, 8 + (bean.getUnmapped() == null ? 0 : bean.getUnmapped().size()), columnSlice.getColumns().size());
        
        for(HColumn<String, byte[]> col : columnSlice.getColumns())
        {
            String n = col.getName();
            if(n.equals("boolVal"))
                assertColumn(msg, bean.getBoolVal(), false, col);
            else if(n.equals("charVal"))
                assertColumn(msg, bean.getCharVal(), false, col);
            else if(n.equals("dateVal"))
                assertColumn(msg, bean.getDateVal(), false, col);
            else if(n.equals("d"))
                assertColumn(msg, bean.getDoubleVal(), false, col);
            else if(n.equals("floatVal"))
                assertColumn(msg, bean.getFloatVal(), false, col);
            else if(n.equals("intVal"))
                assertColumn(msg, bean.getIntVal(), false, col);
            else if(n.equals("l"))
                assertColumn(msg, bean.getLongVal(), false, col);
            else if(n.equals("s"))
                assertColumn(msg, bean.getStrVal(), false, col);
            else
                assertColumn(bean.getUnmapped().get(n), true, col);
        }
    }

    private void assertColumn(Object expected, boolean indicatorExpected, HColumn<?, byte[]> col)
    {
        assertColumn(null,  expected, indicatorExpected, col);
    }
    
    private void assertColumn(String message, Object expected, boolean indicatorExpected, HColumn<?, byte[]> col)
    {
        byte[] value = col.getValue(); 
        
        if(indicatorExpected)
        {
            assertEquals(message, ByteIndicatorSerializer.INDICATOR_BYTES.get(expected.getClass()), col.getValue()[0]);
            value = new byte[col.getValue().length - 1];
            System.arraycopy(col.getValue(), 1, value, 0, value.length);
        }
        assertEquals(message, expected, EntityUtils.getSerializer(expected.getClass()).fromBytes(value));
    }
    
    /*
     * begin test cases
     */

    @Test
    public void testSimplePut()
    {
        int numBeans = 5;
        List<SampleBean> beans = new ArrayList<SampleBean>();
        
        for(int i = 0; i < numBeans; i++)
        {
            SampleBean bean = new SampleBean();
            bean.setRowKey(new Long(i));
            bean.setBoolVal(i%2 == 0);
            bean.setCharVal((char) ('a' + i));
            bean.setDateVal(new Date(System.currentTimeMillis() + 60000*i));
            bean.setDoubleVal(i * .1);
            bean.setFloatVal(i / .5f);
            bean.setIntVal(i);
            bean.setLongVal(-i);
            bean.setStrVal("str-" + i);
            bean.setUnmapped(new HashMap<String, Object>());
            for(int j = 0; j <= i; j++)
                bean.getUnmapped().put("unmapped-" + j, j);
            beans.add(bean);
        }
        
        for(SampleBean bean : beans)
        {
            _dao.put(bean);
            assertTrue(((IEnhancedEntity) bean).getModifiedFields().isEmpty());
            assertFalse(((IEnhancedEntity) bean).getUnmappedFieldsModified());
            SliceQuery<Long,String,byte[]> query = HFactory.createSliceQuery(keyspace, LongSerializer.get(), AsciiSerializer.get(), BytesArraySerializer.get());
            query.setKey(bean.getRowKey());
            query.setColumnFamily("sample");
            query.setRange("", "", false, 100);
            ColumnSlice<String, byte[]> columnSlice = query.execute().get();
            
            assertBean("beans[" + bean.getIntVal() + "]", bean, columnSlice);
        }
        
        SampleBean bean0 = beans.get(0);
        //make sure only dirty fields are saved
        bean0.setStrVal("updated");
        bean0.setDoubleVal(100.0);
        bean0.setUnmapped((Map) Collections.singletonMap("unmapped-0", 100));
        IEnhancedEntity bean = (IEnhancedEntity) bean0;
        bean.getModifiedFields().clear(7);
        bean.setUnmappedFieldsModified(false);
        
        _dao.put(bean0);
        bean0.setStrVal("str-0");
        bean0.setUnmapped((Map) Collections.singletonMap("unmapped-0", 0));

        SliceQuery<Long,String,byte[]> query = HFactory.createSliceQuery(keyspace, LongSerializer.get(), AsciiSerializer.get(), BytesArraySerializer.get());
        query.setKey(bean0.getRowKey());
        query.setColumnFamily("sample");
        query.setRange("", "", false, 100);

        assertBean("dirty test", bean0, query.execute().get());
    }


    
    @Test
    public void testCollectionPut() throws Exception
    {
        /*
         * Map
         */

        MapBean mapBean = new MapBean();
        mapBean.setRowkey(10L);

        mapBean.setMapProp(new HashMap<String, Object>());
        mapBean.getMapProp().put("longMapProp", 100L);
        mapBean.getMapProp().put("strMapProp", "strMapProp-val");
        mapBean.setStrProp("strProp-val");
        mapBean.setStrProp1("strProp1-val");
        mapBean.setUnmapped((Map)Collections.singletonMap("unmapped-1", "val1"));
        _mapDao.put(mapBean);
        
        DynamicCompositeSerializer dcs = new DynamicCompositeSerializer();
        SliceQuery<Long, DynamicComposite, byte[]> query = HFactory.createSliceQuery(keyspace, LongSerializer.get(), dcs, BytesArraySerializer.get());
        query.setKey(mapBean.getRowkey());
        query.setColumnFamily("mapbean");
        
        query.setRange(new DynamicComposite(), new DynamicComposite(), false, 100);

        ColumnSlice<DynamicComposite, byte[]> slice = query.execute().get();
        
        
        assertEquals(5, slice.getColumns().size());
        assertColumn(mapBean.getStrProp(), false, slice.getColumnByName(new DynamicComposite("strProp")));
        assertColumn(mapBean.getStrProp1(), false, slice.getColumnByName(new DynamicComposite("strProp1")));
        assertColumn(mapBean.getMapProp().get("strMapProp"), true, slice.getColumnByName(new DynamicComposite("mapProp", "strMapProp")));
        assertColumn(mapBean.getMapProp().get("longMapProp"), true, slice.getColumnByName(new DynamicComposite("mapProp", "longMapProp")));
        
        //try serializing using unmapped handler with specified serializer 
        assertColumn(mapBean.getUnmapped().get("unmapped-1"), false, slice.getColumnByName(new DynamicComposite("unmapped-1")));
        
        //null out a value and resave
        mapBean.getMapProp().put("strMapProp", null);
        mapBean.setUnmapped(Collections.<String, String>singletonMap("unmapped-1", null));
        _mapDao.put(mapBean);
        
        slice = query.execute().get();
        
        assertEquals(3, slice.getColumns().size());
        assertColumn(mapBean.getStrProp(), false, slice.getColumnByName(new DynamicComposite("strProp")));
        assertColumn(mapBean.getStrProp1(), false, slice.getColumnByName(new DynamicComposite("strProp1")));
        assertColumn(mapBean.getMapProp().get("longMapProp"), true, slice.getColumnByName(new DynamicComposite("mapProp", "longMapProp")));

        /*
         * SortedMap
         */
        SortedMapBean sortedMapBean = new SortedMapBean();
        sortedMapBean.setRowkey(10L);
        
        sortedMapBean.setMapProp(new TreeMap<String, Object>());
        sortedMapBean.getMapProp().put("longMapProp", 100L);
        sortedMapBean.getMapProp().put("strMapProp", "strMapProp-val");
        sortedMapBean.setStrProp("strProp-val");
        sortedMapBean.setStrProp1("strProp1-val");
        
        _sortedMapDao.put(sortedMapBean);
        
        query = HFactory.createSliceQuery(keyspace, LongSerializer.get(), dcs, BytesArraySerializer.get());
        query.setKey(sortedMapBean.getRowkey());
        query.setColumnFamily("sortedmapbean");
        
        query.setRange(new DynamicComposite(), new DynamicComposite(), false, 100);
        
        slice = query.execute().get();
        
        
        assertEquals(4, slice.getColumns().size());
        assertColumn(sortedMapBean.getStrProp(), false, slice.getColumnByName(new DynamicComposite("strProp")));
        assertColumn(sortedMapBean.getStrProp1(), false, slice.getColumnByName(new DynamicComposite("strProp1")));
        assertColumn(sortedMapBean.getMapProp().get("strMapProp"), true, slice.getColumnByName(new DynamicComposite("mapProp", "strMapProp")));
        assertColumn(sortedMapBean.getMapProp().get("longMapProp"), true, slice.getColumnByName(new DynamicComposite("mapProp", "longMapProp")));
        
        //null out a value and resave
        sortedMapBean.getMapProp().put("strMapProp", null);
        _sortedMapDao.put(sortedMapBean);
        
        slice = query.execute().get();
        
        assertEquals(3, slice.getColumns().size());
        assertColumn(sortedMapBean.getStrProp(), false, slice.getColumnByName(new DynamicComposite("strProp")));
        assertColumn(sortedMapBean.getStrProp1(), false, slice.getColumnByName(new DynamicComposite("strProp1")));
        assertColumn(sortedMapBean.getMapProp().get("longMapProp"), true, slice.getColumnByName(new DynamicComposite("mapProp", "longMapProp")));

        /*
         * List
         */
        
        ListBean bean = new ListBean();
        bean.setRowkey(10L);
        
        bean.setListProp(new ArrayList<Object>());
        bean.getListProp().add("strMapProp-val");
        bean.getListProp().add(100L);
        bean.setStrProp("strProp-val");
        bean.setStrProp1("strProp1-val");
        
        _listDao.put(bean);
        
        query = HFactory.createSliceQuery(keyspace, LongSerializer.get(), dcs, BytesArraySerializer.get());
        query.setKey(bean.getRowkey());
        query.setColumnFamily("listbean");
        
        query.setRange(new DynamicComposite(), new DynamicComposite(), false, 100);
        
        slice = query.execute().get();
        
        
        assertEquals(4, slice.getColumns().size());
        assertColumn(bean.getStrProp(), false, slice.getColumnByName(new DynamicComposite("strProp")));
        assertColumn(bean.getStrProp1(), false, slice.getColumnByName(new DynamicComposite("strProp1")));
        assertColumn(bean.getListProp().get(0), true, slice.getColumnByName(new DynamicComposite("listProp", 0)));
        assertColumn(bean.getListProp().get(1), true, slice.getColumnByName(new DynamicComposite("listProp", 1)));
        
        //null out a value and resave
        bean.getListProp().set(0, null);
        _listDao.put(bean);
        
        slice = query.execute().get();
        
        assertEquals(3, slice.getColumns().size());
        assertColumn(bean.getStrProp(), false, slice.getColumnByName(new DynamicComposite("strProp")));
        assertColumn(bean.getStrProp1(), false, slice.getColumnByName(new DynamicComposite("strProp1")));
        assertColumn(bean.getListProp().get(1), true, slice.getColumnByName(new DynamicComposite("listProp", 0)));
    }

    
    @Test
    public void testSimpleGet()
    {
        int numBeans = 5;
        List<SampleBean> beans = new ArrayList<SampleBean>();
        
        for(int i = 0; i < numBeans; i++)
        {
            SampleBean bean = new SampleBean();
            bean.setRowKey(new Long(i));
            bean.setBoolVal(i%2 == 0);
            bean.setCharVal((char) ('a' + i));
            bean.setDateVal(new Date(System.currentTimeMillis() + 60000*i));
            bean.setDoubleVal(i * .1);
            bean.setFloatVal(i / .5f);
            bean.setIntVal(i);
            bean.setLongVal(-i);
            bean.setStrVal("str-" + i);
            
            bean.setUnmapped(new HashMap<String, Object>());
            for(int j = 0; j <= 100; j++)
                bean.getUnmapped().put("unmapped-" + j, "val-" + i + "-" + j);
            
            beans.add(bean);
        }
        
        for(SampleBean bean : beans)
            _dao.put(bean);
        
        for(SampleBean bean : beans)
        {
            SampleBean loaded = _dao.get(bean.getRowKey());
            assertTrue(((IEnhancedEntity) loaded).getModifiedFields().isEmpty());
            assertFalse(((IEnhancedEntity) loaded).getUnmappedFieldsModified());
            assertEquals(bean, loaded);
            assertNotNull(loaded.getUnmapped());
            assertFalse(loaded.getUnmapped().isEmpty());
            
        }
        
        SampleBean bean0 = beans.get(0);
        //test null update
        bean0.setStrVal(null);
        bean0.setLongVal(2000);
        _dao.put(bean0);
        assertEquals(bean0, _dao.get(bean0.getRowKey()));

        
        //test partial
        
        SampleBean partial = new SampleBean();
        partial.setRowKey(1000L);
        partial.setStrVal("hello");
        _dao.put(partial);
        assertEquals(partial, _dao.get(partial.getRowKey()));
        
        //test non-existent
        assertNull(_dao.get(-5L));
    }
    
    @Test
    public void testSimpleMget()
    {
        int numBeans = 5;
        List<SampleBean> beans = new ArrayList<SampleBean>();
        List<Long> keys = new ArrayList<Long>();
        
        for(int i = 0; i < numBeans; i++)
        {
            SampleBean bean = new SampleBean();
            bean.setRowKey(new Long(i));
            bean.setBoolVal(i%2 == 0);
            bean.setCharVal((char) ('a' + i));
            bean.setDateVal(new Date(System.currentTimeMillis() + 60000*i));
            bean.setDoubleVal(i * .1);
            bean.setFloatVal(i / .5f);
            bean.setIntVal(i);
            bean.setLongVal(-i);
            bean.setStrVal("str-" + i);
            
            beans.add(bean);
            keys.add(beans.get(i).getRowKey());
            
            bean.setUnmapped(new HashMap<String, Object>());
            for(int j = 0; j <= 100; j++)
                bean.getUnmapped().put("unmapped-" + j, "val-" + i + "-" + j);
        }
        
        _dao.mput(beans);

        List<Long> keyList = new ArrayList<Long>(keys);
        keyList.add(-5L); //non-existent
        List<SampleBean> actual = new ArrayList<SampleBean>( _dao.mget(keyList) );

        Collections.sort(actual);
        assertEquals(beans.size(), actual.size());
        
        for(int i = beans.size() - 1; i >= 0; i--)
        {
            SampleBean loaded = actual.get(i);
            assertEquals("bean[" + i + "]", beans.get(i), loaded);

            assertTrue(((IEnhancedEntity) loaded).getModifiedFields().isEmpty());
        }
    }

    @Test
    public void testMapGet()
    {
        int numBeans = 5;
        List<MapBean> beans = new ArrayList<MapBean>();
        List<Long> keys = new ArrayList<Long>();
        
        for(int i = 0; i < numBeans; i++)
        {
            MapBean bean = new MapBean();
            bean.setRowkey(new Long(i));
            bean.setStrProp("str-" + i);
            keys.add(bean.getRowkey());
            bean.setUnmapped(new HashMap<String, String>());
            for(int j = 0; j <= 50; j++)
                bean.getUnmapped().put("unmapped-" + j, "val-" + i + "-" + j);

            bean.setMapProp(new HashMap<String, Object>());
            for(int j = 50; j <= 100; j++)
                bean.getMapProp().put("propval-" + j, "val-" + i + "-" + j);

            beans.add(bean);
        }
        
        _mapDao.mput(beans);
        
        for(MapBean bean : beans)
        {
            MapBean loaded = _mapDao.get(bean.getRowkey());
            assertTrue(((IEnhancedEntity) loaded).getModifiedFields().isEmpty());
            assertFalse(((IEnhancedEntity) loaded).getUnmappedFieldsModified());
            assertEquals(bean, loaded);
            assertNotNull(loaded.getUnmapped());
            assertFalse(loaded.getUnmapped().isEmpty());
            
        }
        
        
        //bulk load
        List<MapBean> loaded = new ArrayList<MapBean>( _mapDao.mget(keys));
        Collections.sort(loaded);
        for(MapBean bean : beans)
        {
            assertTrue(((IEnhancedEntity) bean).getModifiedFields().isEmpty());
        }
        assertEquals(beans, loaded);

        MapBean bean0 = beans.get(0);
        //test null update
        for(int i = 50; i < 75; i++)
            bean0.getMapProp().put("propval-" + i, null);
        assertEquals(51, bean0.getMapProp().size()); //sanity check we are overwriting properties to null
        
        _mapDao.put(bean0);

        Iterator<Entry<String, Object>> iter = bean0.getMapProp().entrySet().iterator();
        while(iter.hasNext())
        {
            if(iter.next().getValue() == null)
                iter.remove();
        }
        
        assertEquals(bean0, _mapDao.get(bean0.getRowkey()));
    }
    
    @Test
    public void testSortedMapGet()
    {
        int numBeans = 5;
        List<SortedMapBean> beans = new ArrayList<SortedMapBean>();
        List<Long> keys = new ArrayList<Long>();
        
        for(int i = 0; i < numBeans; i++)
        {
            SortedMapBean bean = new SortedMapBean();
            bean.setRowkey(new Long(i));
            bean.setStrProp("str-" + i);
            keys.add(bean.getRowkey());
            
            bean.setMapProp(new TreeMap<String, Object>());
            for(int j = 50; j <= 100; j++)
                bean.getMapProp().put("propval-" + j, "val-" + i + "-" + j);
            
            beans.add(bean);
        }
        
        _sortedMapDao.mput(beans);
        
        for(SortedMapBean bean : beans)
        {
            SortedMapBean loaded = _sortedMapDao.get(bean.getRowkey());
            assertTrue(((IEnhancedEntity) loaded).getModifiedFields().isEmpty());
            assertEquals(bean, loaded);
        }
        
        //bulk load
        List<SortedMapBean> loaded = new ArrayList<SortedMapBean>( _sortedMapDao.mget(keys));
        Collections.sort(loaded);
        for(SortedMapBean bean : beans)
        {
            assertTrue(((IEnhancedEntity) bean).getModifiedFields().isEmpty());
        }
        assertEquals(beans, loaded);

        
        SortedMapBean bean0 = beans.get(0);
        //test null update
        for(int i = 50; i < 75; i++)
            bean0.getMapProp().put("propval-" + i, null);
        assertEquals(51, bean0.getMapProp().size()); //sanity check we are overwriting properties to null
        
        _sortedMapDao.put(bean0);
        
        Iterator<Entry<String, Object>> iter = bean0.getMapProp().entrySet().iterator();
        while(iter.hasNext())
        {
            if(iter.next().getValue() == null)
                iter.remove();
        }
        
        assertEquals(bean0, _sortedMapDao.get(bean0.getRowkey()));
    }

    @Test
    public void testListGet()
    {
        int numBeans = 5;
        List<ListBean> beans = new ArrayList<ListBean>();
        List<Long> keys = new ArrayList<Long>();
        
        for(int i = 0; i < numBeans; i++)
        {
            ListBean bean = new ListBean();
            bean.setRowkey(new Long(i));
            bean.setStrProp("str-" + i);
            keys.add(bean.getRowkey());
            bean.setListProp(new ArrayList<Object>());
            for(int j = 0; j <= 100; j++)
                bean.getListProp().add("val-" + i + "-" + j);
            
            beans.add(bean);
        }
        
        _listDao.mput(beans);
        
        for(ListBean bean : beans)
        {
            ListBean loaded = _listDao.get(bean.getRowkey());
            assertTrue(((IEnhancedEntity) loaded).getModifiedFields().isEmpty());
            assertEquals(bean, loaded);
        }

        //bulk load
        List<ListBean> loaded = new ArrayList<ListBean>( _listDao.mget(keys));
        Collections.sort(loaded);
        for(ListBean bean : beans)
        {
            assertTrue(((IEnhancedEntity) bean).getModifiedFields().isEmpty());
        }
        assertEquals(beans, loaded);

        
        ListBean bean0 = beans.get(0);
        //test null update
        for(int i = 0; i < 50; i++)
            bean0.getListProp().set(i, null);
        
        _listDao.put(bean0);
        
        Iterator<Object> iter = bean0.getListProp().iterator();
        while(iter.hasNext())
        {
            if(iter.next() == null)
                iter.remove();
        }
        
        assertEquals(bean0, _listDao.get(bean0.getRowkey()));
    }

    @Test
    public void testGetPartial() throws Exception
    {
        int numBeans = 5;
        List<SampleBean> beans = new ArrayList<SampleBean>();
        List<Long> keys = new ArrayList<Long>();
        
        for(int i = 0; i < numBeans; i++)
        {
            SampleBean bean = new SampleBean();
            bean.setRowKey(new Long(i));
            bean.setBoolVal(i%2 == 0);
            bean.setCharVal((char) ('a' + i));
            bean.setDateVal(new Date(System.currentTimeMillis() + 60000*i));
            bean.setDoubleVal(i * .1);
            bean.setFloatVal(i / .5f);
            bean.setIntVal(i);
            bean.setLongVal(-i);
            bean.setStrVal("str-" + i);
            
            bean.setUnmapped(new HashMap<String, Object>());
            for(int j = 0; j <= 20; j++)
                bean.getUnmapped().put("unmapped-" + j, "val-" + i + "-" + j);//place them between fixed properties
            
            beans.add(bean);
            keys.add(bean.getRowKey());
        }

        _dao.mput(beans);


        for(SampleBean saved : beans)
        {
            SampleBean expected = (SampleBean) saved.clone();
            
            expected.setBoolVal(false); //false is default value for boolean
            expected.setUnmapped(null); //can't efficiently do exclusions and include unmapped columns right now as c* ranges are inclusive
            SampleBean actual = _dao.get(saved.getRowKey(), null, null, Collections.singleton("boolVal"));
            
            assertEquals(expected, actual);
            
            expected = new SampleBean();
            expected.setRowKey(saved.getRowKey());
            expected.setCharVal(saved.getCharVal());
            TreeMap<String, Object> unmapped = new TreeMap<String, Object>(saved.getUnmapped());
            for(int i = saved.getUnmapped().size()/2; i >= 0; i--)
                unmapped.remove(unmapped.firstEntry().getKey());
            
            expected.setUnmapped(unmapped);
            
            Set<String> props = new HashSet<String>(unmapped.keySet());
            props.add("charVal");
            
            actual = _dao.get(saved.getRowKey(), null, props, null);
            assertEquals(expected, actual);
            
            expected.setIntVal(saved.getIntVal());
            _dao.get(saved.getRowKey(), actual, Collections.singleton("intVal"), null); //update the bean
            
            assertEquals(expected, actual);
        }
        
    }
    
    @Test
    public void testGetPartialRange() throws Exception
    {
        int numBeans = 5;
        List<SampleBean> beans = new ArrayList<SampleBean>();
        List<Long> keys = new ArrayList<Long>();
        
        for(int i = 0; i < numBeans; i++)
        {
            SampleBean bean = new SampleBean();
            bean.setRowKey(new Long(i));
            bean.setBoolVal(i%2 == 0);
            bean.setCharVal((char) ('a' + i));
            bean.setDateVal(new Date(System.currentTimeMillis() + 60000*i));
            bean.setDoubleVal(i * .1);
            bean.setFloatVal(i / .5f);
            bean.setIntVal(i);
            bean.setLongVal(-i);
            bean.setStrVal("str-" + i);
            
            bean.setUnmapped(new HashMap<String, Object>());
            for(int j = 0; j <= 20; j++)
                bean.getUnmapped().put("unmapped-" + j, "val-" + i + "-" + j);
            
            beans.add(bean);
            keys.add(bean.getRowKey());
        }
        
        _dao.mput(beans);
        List<SampleBean> bulkActuals = _dao.mget(keys, null, "c", "cv");
        List<SampleBean> actuals = new ArrayList<SampleBean>();
        List<SampleBean> expecteds = new ArrayList<SampleBean>();
        Collections.sort(bulkActuals);
        
        for(int i = 0; i < beans.size(); i++)
        {
            SampleBean saved = beans.get(i);
            SampleBean expected = new SampleBean();
            
            expected.setRowKey(saved.getRowKey());
            expected.setCharVal(saved.getCharVal());
            expected.setUnmapped(null); //when using exclude, unmapped properties are ignored
            actuals.add(_dao.get(saved.getRowKey(), null, "c", "cv"));
            
            assertEquals(expected, actuals.get(i));
            assertEquals(expected, bulkActuals.get(i));
            
            expecteds.add(expected);
        }

        bulkActuals = _dao.mget(keys, bulkActuals, "d", "daa");
        for(int i = 0; i < beans.size(); i++)
        {
            SampleBean saved = beans.get(i);
            SampleBean expected = expecteds.get(i);
            
            actuals.set(i, _dao.get(saved.getRowKey(), actuals.get(i), "d", "daa"));//includes double's physical name
            expected.setDoubleVal(saved.getDoubleVal());
            assertEquals(expected, actuals.get(i));
            assertEquals(expected, bulkActuals.get(i));
        }

        bulkActuals = _dao.mget(keys, bulkActuals, "unmapped-10", "unmapped-19");

        for(int i = 0; i < beans.size(); i++)
        {
            SampleBean saved = beans.get(i);
            SampleBean expected = expecteds.get(i);
            
            expected.setUnmapped(new HashMap<String, Object>());
            for(int j = 10; j < 20; j++)
                expected.getUnmapped().put("unmapped-" + j, saved.getUnmapped().get("unmapped-" + j));

            actuals.set(i, _dao.get(saved.getRowKey(), actuals.get(i), "unmapped-10", "unmapped-19"));
            assertEquals(expected, actuals.get(i));
            assertEquals(expected, bulkActuals.get(i));
        }
    }
    

    @Test
    public void testGetPartialCollection() throws Exception
    {
        int numBeans = 5;
        List<MapBean> mapBeans = new ArrayList<MapBean>();
        List<ListBean> listBeans = new ArrayList<ListBean>();
        List<Long> keys = new ArrayList<Long>();
        
        for(int i = 0; i < numBeans; i++)
        {
            ListBean lbean = new ListBean();
            lbean.setRowkey(new Long(i));
            lbean.setStrProp("str-" + i);
            lbean.setStrProp1("str1-" + i);
            lbean.setListProp(new ArrayList<Object>());
            for(int j = 0; j <= 200; j++)
                lbean.getListProp().add(i*1000 + j);

            MapBean mbean = new MapBean();
            mbean.setRowkey(new Long(i));
            mbean.setStrProp("str-" + i);
            mbean.setStrProp1("str1-" + i);
            mbean.setMapProp(new HashMap<String, Object>());
            for(int j = 0; j <= 200; j++)
                mbean.getMapProp().put("key-" + j, j);

            mapBeans.add(mbean);
            listBeans.add(lbean);
            keys.add(lbean.getRowkey());
        }

        _listDao.mput(listBeans);
        _mapDao.mput(mapBeans);

        List<ListBean> bulkListActuals = _listDao.mget(keys, null, null, Collections.singleton("strProp1"));
        assertEquals(numBeans, bulkListActuals.size());
        Collections.sort(bulkListActuals);

        for(int i = 0; i < listBeans.size(); i++)
        {
            ListBean saved = listBeans.get(i);
            ListBean expected = (ListBean) saved.clone();
            
            expected.setStrProp1(null); 
            ListBean actual = _listDao.get(saved.getRowkey(), null, null, Collections.singleton("strProp1"));
            
            assertEquals(expected, actual);
            assertEquals(expected, bulkListActuals.get(i));
        }
        
        
        bulkListActuals = _listDao.mget(keys, null, Collections.singleton(new CollectionProperty("listProp", 100)), null);
        assertEquals(numBeans, bulkListActuals.size());
        Collections.sort(bulkListActuals);
        for(int i = 0; i < listBeans.size(); i++)
        {
            ListBean saved = listBeans.get(i);
            /*
             * load a single value from within a collection
             */
            ListBean actual = _listDao.get(saved.getRowkey(), null, Collections.singleton(new CollectionProperty("listProp", 100)), null);
            
            ListBean expected = new ListBean();
            expected.setRowkey(saved.getRowkey());
            expected.setListProp(new ArrayList<Object>());
            for(int j = 0; j < 100; j++)
                expected.getListProp().add(null);
            
            expected.getListProp().add(saved.getListProp().get(100));
            assertEquals(expected, actual);
            assertEquals(expected, bulkListActuals.get(i));
        }
        
        List<MapBean> bulkMapActuals = _mapDao.mget(keys, null, null, Collections.singleton("strProp1"));
        assertEquals(numBeans, bulkMapActuals.size());
        Collections.sort(bulkMapActuals);
        for(int i = 0; i < mapBeans.size(); i++)
        {
            MapBean saved = mapBeans.get(i);
            MapBean expected = (MapBean) saved.clone();
            
            expected.setStrProp1(null); 
            MapBean actual = _mapDao.get(saved.getRowkey(), null, null, Collections.singleton("strProp1"));
            
            assertEquals(expected, actual);
            assertEquals(expected, bulkMapActuals.get(i));
        }
        
        
        Entry<String, Object> entry = (Entry<String, Object>) mapBeans.get(0).getMapProp().entrySet().iterator().next();
        bulkMapActuals = _mapDao.mget(keys, null, Collections.singleton(new CollectionProperty("mapProp", entry.getKey())), null);
        assertEquals(numBeans, bulkMapActuals.size());
        Collections.sort(bulkMapActuals);
        for(int i = 0; i < mapBeans.size(); i++)
        {
            MapBean saved = mapBeans.get(i);
            /*
             * load a single value from within a collection
             */
            MapBean actual = _mapDao.get(saved.getRowkey(), null, Collections.singleton(new CollectionProperty("mapProp", entry.getKey())), null);
            
            MapBean expected = new MapBean();
            expected.setRowkey(saved.getRowkey());
            expected.setMapProp(Collections.singletonMap(entry.getKey(), entry.getValue()));
            
            assertEquals(expected, actual);
            assertEquals(expected, bulkMapActuals.get(i));
        }
    }
    
    @Test
    public void testGetPartialCollectionRange() throws Exception
    {
        int numBeans = 5;
        List<MapBean> mapBeans = new ArrayList<MapBean>();
        List<ListBean> listBeans = new ArrayList<ListBean>();
        List<Long> keys = new ArrayList<Long>();
        
        for(int i = 0; i < numBeans; i++)
        {
            ListBean lbean = new ListBean();
            lbean.setRowkey(new Long(i));
            lbean.setStrProp("str-" + i);
            lbean.setStrProp1("str1-" + i);
            lbean.setListProp(new ArrayList<Object>());
            for(int j = 0; j <= 200; j++)
                lbean.getListProp().add(i*1000 + j);

            MapBean mbean = new MapBean();
            mbean.setRowkey(new Long(i));
            mbean.setStrProp("str-" + i);
            mbean.setStrProp1("str1-" + i);
            mbean.setMapProp(new HashMap<String, Object>());
            for(int j = 0; j <= 200; j++)
                mbean.getMapProp().put("key-" + j + "-" + i, i*1000 + j);

            mapBeans.add(mbean);
            listBeans.add(lbean);
            keys.add(lbean.getRowkey());
        }

        _listDao.mput(listBeans);
        _mapDao.mput(mapBeans);
        

        /*
         * lists
         */
        //do the same test using bulk API
        List<ListBean> bulkListActuals = _listDao.mget(keys, null, "strProp1", "strProp1");
        assertEquals(numBeans, bulkListActuals.size());
        Collections.sort(bulkListActuals);
        List<ListBean> singleListActuals = new ArrayList<ListBean>();
        for(int i = 0; i < listBeans.size(); i++)
        {
            ListBean saved = listBeans.get(i);
            ListBean expected = new ListBean();
            expected.setRowkey(saved.getRowkey());
            expected.setStrProp1(saved.getStrProp1()); 
            
            singleListActuals.add(_listDao.get(saved.getRowkey(), null, "strProp1", "strProp1"));

            assertEquals(expected, singleListActuals.get(i));
            assertEquals(expected, bulkListActuals.get(i));
        }

        _listDao.mget(keys, bulkListActuals, new CollectionProperty("listProp", 25),  new CollectionProperty("listProp", 175));
        assertEquals(numBeans, bulkListActuals.size());

        for(int i = 0; i < listBeans.size(); i++)
        {
            ListBean saved = listBeans.get(i);

            ListBean expected = new ListBean();
            expected.setRowkey(saved.getRowkey());
            expected.setStrProp1(saved.getStrProp1()); 
            expected.setListProp(new ArrayList<Object>());
            for(int j = 0; j <= 175; j++)
                expected.getListProp().add(j < 25 ? null : saved.getListProp().get(j));

            _listDao.get(saved.getRowkey(), singleListActuals.get(i), new CollectionProperty("listProp", 25),  new CollectionProperty("listProp", 175));
            
            assertEquals(expected, singleListActuals.get(i));
            assertEquals(expected, bulkListActuals.get(i));
        }
        
        /*
         * maps
         */
        
        //do the same test using bulk API
        List<MapBean> bulkMapActuals = _mapDao.mget(keys, null, "strProp1", "strProp1");
        List<MapBean> mapActuals = new ArrayList<MapBean>();
        List<MapBean> expectedMaps = new ArrayList<MapBean>();
        
        assertEquals(numBeans, bulkMapActuals.size());
        Collections.sort(bulkMapActuals);
        
        for(int i = 0; i < mapBeans.size(); i++)
        {
            MapBean saved = mapBeans.get(i);
            MapBean expected = new MapBean();
            expected.setRowkey(saved.getRowkey());
            expected.setStrProp1(saved.getStrProp1()); 

            MapBean actual = _mapDao.get(saved.getRowkey(), null, "strProp1", "strProp1");
            
            assertEquals(expected, actual);
            assertEquals(expected, bulkMapActuals.get(i));

            mapActuals.add(actual);
            expectedMaps.add(expected);
        }
        
        
        /*
         * load a range from within a collection
         */
        String start = "key-100", end = "key-201";
        bulkMapActuals = _mapDao.mget(keys, mapActuals, new CollectionProperty("mapProp", start), new CollectionProperty("mapProp", end));
        assertEquals(numBeans, bulkMapActuals.size());
        for(int i = 0; i < mapBeans.size(); i++)
        {
            MapBean saved = mapBeans.get(i);
            MapBean expected = expectedMaps.get(i);
            mapActuals.set(i, _mapDao.get(saved.getRowkey(), mapActuals.get(i), new CollectionProperty("mapProp", start), new CollectionProperty("mapProp", end)));
            
            expected.setRowkey(saved.getRowkey());
            expected.setMapProp(new HashMap<String, Object>());
            for(Object o : saved.getMapProp().entrySet())
            {
                Map.Entry<String, Object> e = (Map.Entry<String, Object>) o;
                String k = e.getKey();
                if(k.compareTo(start) >= 0 && k.compareTo(end) <= 0)
                    expected.getMapProp().put(k, e.getValue());
                    
            }
            
            assertEquals(expected, mapActuals.get(i));
            assertEquals(expected, bulkMapActuals.get(i));
        }
    }

    @Test
    public void testFind() throws Exception
    {
        int numBeans = CassandraDaoBase.ROW_RANGE_SIZE+1;//force dao to do multiple ranges
        List<IndexedBean> idxBeans = new ArrayList<IndexedBean>();
        List<CompositeIndexedBean> cIdxBeans = new ArrayList<CompositeIndexedBean>();
        for(int i = 0; i < numBeans; i++)
        {
            IndexedBean idxBean = new IndexedBean();
            idxBean.setRowKey(new Long(i));
            idxBean.setCharVal('a');
            idxBean.setIntVal(i/10);
            idxBean.setLongVal(i/10);
            idxBean.setStrVal("strval");
            
            idxBeans.add(idxBean);

            CompositeIndexedBean cIdxBean = new CompositeIndexedBean();
            cIdxBean.setRowKey(new Long(i));
            cIdxBean.setCharVal('a');
            cIdxBean.setIntVal(i/10);
            cIdxBean.setLongVal(i/10);
            cIdxBean.setStrVal("strval");
            
            cIdxBeans.add(cIdxBean);
        }
        
        _indexedDao.mput(idxBeans);
        _compositeIndexedDao.mput(cIdxBeans);
        
        IndexedBean idxTmpl = new IndexedBean();
        idxTmpl.setIntVal(5);
        List<IndexedBean> idxActuals = new ArrayList<IndexedBean>(_indexedDao.mfind(idxTmpl));

        Collections.sort(idxActuals);
        assertEquals(idxBeans.subList(50, 60), idxActuals);
        for(IndexedBean idxBean : idxActuals)
            assertTrue(((IEnhancedEntity) idxBean).getModifiedFields().isEmpty());

        CompositeIndexedBean cIdxTmpl = new CompositeIndexedBean();
        cIdxTmpl.setIntVal(5);
        List<CompositeIndexedBean> cIdxActuals = new ArrayList<CompositeIndexedBean>(_compositeIndexedDao.mfind(cIdxTmpl));

        Collections.sort(cIdxActuals);
        assertEquals(cIdxBeans.subList(50, 60), cIdxActuals);
        for(CompositeIndexedBean cIdxBean : cIdxActuals)
            assertTrue(((IEnhancedEntity) cIdxBean).getModifiedFields().isEmpty());

        
        idxTmpl = new IndexedBean();
        idxTmpl.setStrVal("strval");
        idxActuals = new ArrayList<IndexedBean>(_indexedDao.mfind(idxTmpl));

        Collections.sort(idxActuals);
        assertEquals(idxBeans, idxActuals);
        for(IndexedBean idxBean : idxActuals)
            assertTrue(((IEnhancedEntity) idxBean).getModifiedFields().isEmpty());

        
        cIdxTmpl = new CompositeIndexedBean();
        cIdxTmpl.setStrVal("strval");
        cIdxActuals = new ArrayList<CompositeIndexedBean>(_compositeIndexedDao.mfind(cIdxTmpl));
        
        Collections.sort(cIdxActuals);
        assertEquals(cIdxBeans, cIdxActuals);
        for(CompositeIndexedBean cIdxBean : cIdxActuals)
            assertTrue(((IEnhancedEntity) cIdxBean).getModifiedFields().isEmpty());
    }
    
    @Test
    public void testFindPartialRange() throws Exception
    {
        int numBeans = CassandraDaoBase.ROW_RANGE_SIZE+1;//force dao to do multiple ranges
        List<IndexedBean> idxBeans = new ArrayList<IndexedBean>();
        List<CompositeIndexedBean> cIdxBeans = new ArrayList<CompositeIndexedBean>();
        for(int i = 0; i < numBeans; i++)
        {
            IndexedBean idxBean = new IndexedBean();
            idxBean.setRowKey(new Long(i));
            idxBean.setCharVal('c');
            idxBean.setIntVal(i/10);
            idxBean.setLongVal(i);
            idxBean.setStrVal("strval");
            
            idxBeans.add(idxBean);

            CompositeIndexedBean cIdxBean = new CompositeIndexedBean();
            cIdxBean.setRowKey(new Long(i));
            cIdxBean.setCharVal('c');
            cIdxBean.setIntVal(i/10);
            cIdxBean.setLongVal(i);
            cIdxBean.setStrVal("strval");
            
            cIdxBeans.add(cIdxBean);
        }
        
        _indexedDao.mput(idxBeans);
        _compositeIndexedDao.mput(cIdxBeans);
        
        IndexedBean idxTmpl = new IndexedBean();
        idxTmpl.setIntVal(5);
        List<IndexedBean> idxActuals = new ArrayList<IndexedBean>(_indexedDao.mfind(idxTmpl, "longVal", "t"));
        List<IndexedBean> idxExpecteds = new ArrayList<IndexedBean>();

        Collections.sort(idxActuals);
        for(int i = 50; i < 60; i++)
        {
            IndexedBean idxBean = new IndexedBean();
            idxBean.setRowKey(idxBeans.get(i).getRowKey());
            idxBean.setLongVal(idxBeans.get(i).getLongVal());
            idxBean.setStrVal(idxBeans.get(i).getStrVal());
            
            idxExpecteds.add(idxBean);
        }
        
        assertEquals(idxExpecteds, idxActuals);
        for(IndexedBean idxBean : idxActuals)
            assertTrue(((IEnhancedEntity) idxBean).getModifiedFields().isEmpty());

        CompositeIndexedBean cIdxTmpl = new CompositeIndexedBean();
        cIdxTmpl.setIntVal(5);
        List<CompositeIndexedBean> cIdxActuals = new ArrayList<CompositeIndexedBean>(_compositeIndexedDao.mfind(cIdxTmpl, "longVal", "t"));
        List<CompositeIndexedBean> cIdxExpecteds = new ArrayList<CompositeIndexedBean>();

        for(int i = 50; i < 60; i++)
        {
            CompositeIndexedBean idxBean = new CompositeIndexedBean();
            idxBean.setRowKey(idxBeans.get(i).getRowKey());
            idxBean.setLongVal(idxBeans.get(i).getLongVal());
            idxBean.setStrVal(idxBeans.get(i).getStrVal());
            
            cIdxExpecteds.add(idxBean);
        }
        
        Collections.sort(cIdxActuals);
        assertEquals(cIdxExpecteds, cIdxActuals);
        for(CompositeIndexedBean cIdxBean : cIdxActuals)
            assertTrue(((IEnhancedEntity) cIdxBean).getModifiedFields().isEmpty());
        
        /*
         * try collection properties
         */
        List<ListBean> listBeans = new ArrayList<ListBean>();
        List<Object> coll = new ArrayList<Object>();
        for(int i = 0; i < 20; i++)
            coll.add(i);
        
        for(int i = 0; i < numBeans; i++)
        {
            ListBean bean = new ListBean();
            bean.setRowkey(new Long(i));
            bean.setStrProp("s" + i/10);
            bean.setStrProp1("s2");
            bean.setListProp(new ArrayList<Object>());
            bean.setListProp(coll);
            
            listBeans.add(bean);
        }
        _listDao.mput(listBeans);

        ListBean lTmpl = new ListBean();
        lTmpl.setStrProp("s5");
        int startIdx = 4, endIdx = 15;
        List<ListBean> listActuals = new ArrayList<ListBean>( _listDao.mfind(lTmpl, new CollectionProperty("listProp", startIdx), new CollectionProperty("listProp", endIdx)) );
        Collections.sort(listActuals);
        assertEquals(10, listActuals.size());
        
        List<Object> expectedColl = new ArrayList<Object>();
        for(int i = 0; i <= endIdx; i++)
            expectedColl.add(i < startIdx ? null : i);
        
        for(int i = 50; i < 60; i++)
        {
            ListBean expected = new ListBean();
            expected.setRowkey(listBeans.get(i).getRowkey());
            expected.setListProp(expectedColl);
            assertEquals(expected, listActuals.get(i-50));
        }
    }
    
    @Test
    public void testFindPartial() throws Exception
    {
        int numBeans = CassandraDaoBase.ROW_RANGE_SIZE+1;//force dao to do multiple ranges
        List<IndexedBean> idxBeans = new ArrayList<IndexedBean>();
        List<CompositeIndexedBean> cIdxBeans = new ArrayList<CompositeIndexedBean>();
        for(int i = 0; i < numBeans; i++)
        {
            IndexedBean idxBean = new IndexedBean();
            idxBean.setRowKey(new Long(i));
            idxBean.setCharVal('c');
            idxBean.setIntVal(i/10);
            idxBean.setLongVal(i);
            idxBean.setStrVal("strval");
            
            idxBeans.add(idxBean);
            
            CompositeIndexedBean cIdxBean = new CompositeIndexedBean();
            cIdxBean.setRowKey(new Long(i));
            cIdxBean.setCharVal('c');
            cIdxBean.setIntVal(i/10);
            cIdxBean.setLongVal(i);
            cIdxBean.setStrVal("strval");
            
            cIdxBeans.add(cIdxBean);
        }
        
        _indexedDao.mput(idxBeans);
        _compositeIndexedDao.mput(cIdxBeans);
        
        IndexedBean idxTmpl = new IndexedBean();
        idxTmpl.setIntVal(5);
        List<IndexedBean> idxActuals = new ArrayList<IndexedBean>(_indexedDao.mfind(idxTmpl, Collections.singleton("longVal"), null));
        List<IndexedBean> idxExpecteds = new ArrayList<IndexedBean>();
        
        Collections.sort(idxActuals);
        for(int i = 50; i < 60; i++)
        {
            IndexedBean idxBean = new IndexedBean();
            idxBean.setRowKey(idxBeans.get(i).getRowKey());
            idxBean.setLongVal(idxBeans.get(i).getLongVal());
            
            idxExpecteds.add(idxBean);
        }
        
        assertEquals(idxExpecteds, idxActuals);
        for(IndexedBean idxBean : idxActuals)
            assertTrue(((IEnhancedEntity) idxBean).getModifiedFields().isEmpty());
        
        CompositeIndexedBean cIdxTmpl = new CompositeIndexedBean();
        cIdxTmpl.setIntVal(5);
        List<CompositeIndexedBean> cIdxActuals = new ArrayList<CompositeIndexedBean>(_compositeIndexedDao.mfind(cIdxTmpl, null, Collections.singleton("intVal")));
        List<CompositeIndexedBean> cIdxExpecteds = new ArrayList<CompositeIndexedBean>();
        
        for(int i = 50; i < 60; i++)
        {
            CompositeIndexedBean idxBean = new CompositeIndexedBean();
            idxBean.setRowKey(idxBeans.get(i).getRowKey());
            idxBean.setCharVal(idxBeans.get(i).getCharVal());
            idxBean.setLongVal(idxBeans.get(i).getLongVal());
            idxBean.setStrVal(idxBeans.get(i).getStrVal());
            
            cIdxExpecteds.add(idxBean);
        }
        
        Collections.sort(cIdxActuals);
        assertEquals(cIdxExpecteds, cIdxActuals);
        for(CompositeIndexedBean cIdxBean : cIdxActuals)
            assertTrue(((IEnhancedEntity) cIdxBean).getModifiedFields().isEmpty());
        
        /*
         * try collection properties
         */
        List<ListBean> listBeans = new ArrayList<ListBean>();
        List<Object> coll = new ArrayList<Object>();
        for(int i = 0; i < 20; i++)
            coll.add(i);
        
        for(int i = 0; i < numBeans; i++)
        {
            ListBean bean = new ListBean();
            bean.setRowkey(new Long(i));
            bean.setStrProp("s" + i/10);
            bean.setStrProp1("s2");
            bean.setListProp(new ArrayList<Object>());
            bean.setListProp(coll);
            
            listBeans.add(bean);
        }
        _listDao.mput(listBeans);
        
        ListBean lTmpl = new ListBean();
        lTmpl.setStrProp("s5");
        int startIdx = 4, endIdx = 15;
        Set<Object> includes = new HashSet<Object>();
        for(int i = startIdx; i <= endIdx; i++)
            includes.add(new CollectionProperty("listProp", i));
        
        List<ListBean> listActuals = new ArrayList<ListBean>( _listDao.mfind(lTmpl, includes, null) );
        Collections.sort(listActuals);
        assertEquals(10, listActuals.size());
        
        List<Object> expectedColl = new ArrayList<Object>();
        for(int i = 0; i <= endIdx; i++)
            expectedColl.add(i < startIdx ? null : i);
        
        for(int i = 50; i < 60; i++)
        {
            ListBean expected = new ListBean();
            expected.setRowkey(listBeans.get(i).getRowkey());
            expected.setListProp(expectedColl);
            assertEquals(expected, listActuals.get(i-50));
        }
    }


    @Test
    public void testRangeIndexFind() throws Exception
    {
        int numBeans = CassandraDaoBase.ROW_RANGE_SIZE;
        List<IndexedBean> idxBeans = new ArrayList<IndexedBean>();
        List<CompositeIndexedBean> cIdxBeans = new ArrayList<CompositeIndexedBean>();
        for(int i = 0; i < numBeans; i++)
        {
            IndexedBean idxBean = new IndexedBean();
            idxBean.setRowKey(new Long(i));
            idxBean.setCharVal('c');
            idxBean.setIntVal(i);
            idxBean.setLongVal(i/10);
            idxBean.setStrVal("strval");
            
            idxBeans.add(idxBean);

            CompositeIndexedBean cIdxBean = new CompositeIndexedBean();
            cIdxBean.setCharVal('c');
            cIdxBean.setRowKey(new Long(i));
            cIdxBean.setIntVal(i);
            cIdxBean.setLongVal(i/10);
            cIdxBean.setStrVal("strval");
            
            cIdxBeans.add(cIdxBean);
        }
        
        _indexedDao.mput(idxBeans);
        _compositeIndexedDao.mput(cIdxBeans);
        
        IndexedBean idxTmpl = new IndexedBean();
        idxTmpl.setLongVal(5);
        List<IndexedBean> idxActuals = new ArrayList<IndexedBean>(_indexedDao.mfind(idxTmpl));

        Collections.sort(idxActuals);
        assertEquals(idxBeans.subList(50, 60), idxActuals);
        for(IndexedBean idxBean : idxActuals)
            assertTrue(((IEnhancedEntity) idxBean).getModifiedFields().isEmpty());

        CompositeIndexedBean cIdxTmpl = new CompositeIndexedBean();
        cIdxTmpl.setLongVal(5);
        List<CompositeIndexedBean> cIdxActuals = new ArrayList<CompositeIndexedBean>(_compositeIndexedDao.mfind(cIdxTmpl));

        Collections.sort(cIdxActuals);
        assertEquals(cIdxBeans.subList(50, 60), cIdxActuals);
        for(CompositeIndexedBean cIdxBean : cIdxActuals)
            assertTrue(((IEnhancedEntity) cIdxBean).getModifiedFields().isEmpty());

        
        assertEquals(0, _indexedStrategy.records.size());
        assertEquals(0, _compositeStrategy.records.size());
        
        for(IndexedBean bean : idxBeans.subList(50, 55))
            bean.setLongVal(-1);
        
        _indexedDao.mput(idxBeans.subList(50, 55));
        idxActuals = new ArrayList<IndexedBean>(_indexedDao.mfind(idxTmpl));
        Collections.sort(idxActuals);
        assertEquals(idxBeans.subList(55, 60), idxActuals);
        assertEquals(1, _indexedStrategy.records.size());
        
        StaleIndexUpdateRecord record = _indexedStrategy.records.get(0);
        DynamicCompositeSerializer compositeSer = new DynamicCompositeSerializer();

        Set<Long> actualStaleRowKeys = new HashSet<Long>();
        Set<Long> expectedStaleRowKeys = new HashSet<Long>();
        /*
         * validate the clock values
         */
        for(StaleIndexValue stale : record.values)
        {
            actualStaleRowKeys.add((Long) stale.getColumnName().get(1));
            
            SliceQuery<DynamicComposite,DynamicComposite,byte[]> query = 
                    HFactory.createSliceQuery(keyspace, compositeSer, compositeSer, BytesArraySerializer.get());

            query.setKey(stale.getRowKey());
            query.setColumnNames(stale.getColumnName());
            query.setColumnFamily("indexedbean_idx");
            List<HColumn<DynamicComposite,byte[]>> columns = query.execute().get().getColumns();
            assertEquals(1, columns.size());
            assertEquals(stale.getClock(), columns.get(0).getClock());
        }
        assertEquals(5, record.values.size());
        for(IndexedBean i : idxBeans.subList(50, 55))
            expectedStaleRowKeys.add(i.getRowKey());
        
        assertEquals(expectedStaleRowKeys, actualStaleRowKeys);
        
        /*
         * do a between find
         */
        IndexedBean endIdxTmpl = new IndexedBean();
        endIdxTmpl.setLongVal(6);

        idxActuals = new ArrayList<IndexedBean>(_indexedDao.mfindBetween(idxTmpl, endIdxTmpl));
        Collections.sort(idxActuals);
        assertEquals(idxBeans.subList(55, 70), idxActuals);
     
        assertEquals(2, _indexedStrategy.records.size());
        
        record = _indexedStrategy.records.get(1);
        assertEquals(5, record.values.size());

        actualStaleRowKeys.clear();
        for(StaleIndexValue stale : record.values)
            actualStaleRowKeys.add((Long) stale.getColumnName().get(1));
        
        assertEquals(expectedStaleRowKeys, actualStaleRowKeys);
        
        CompositeIndexedBean endCIdxTmpl = new CompositeIndexedBean();
        endCIdxTmpl.setLongVal(6);
        
        cIdxActuals = new ArrayList<CompositeIndexedBean>(_compositeIndexedDao.mfindBetween(cIdxTmpl, endCIdxTmpl));
        Collections.sort(cIdxActuals);
        assertEquals(cIdxBeans.subList(50, 70), cIdxActuals);
    }

    @Test
    public void testRangeFindPartial() throws Exception
    {
        int numBeans = 1 + CassandraDaoBase.COL_RANGE_SIZE * 3;//force dao to do multiple ranges
        List<IndexedBean> idxBeans = new ArrayList<IndexedBean>();
        for(int i = 0; i < numBeans; i++)
        {
            IndexedBean idxBean = new IndexedBean();
            idxBean.setRowKey(new Long(i));
            idxBean.setCharVal('c');
            idxBean.setIntVal(i);
            idxBean.setLongVal(Math.min(i/CassandraDaoBase.COL_RANGE_SIZE, 2));
            idxBean.setStrVal("strval");

            idxBeans.add(idxBean);

        }

        _indexedDao.mput(idxBeans);

        IndexedBean idxTmpl = new IndexedBean();
        idxTmpl.setLongVal(2);
        List<IndexedBean> idxActuals = new ArrayList<IndexedBean>(_indexedDao.mfind(idxTmpl, Collections.singleton("intVal"), null));
        List<IndexedBean> idxExpecteds = new ArrayList<IndexedBean>();

        Collections.sort(idxActuals);
        for(int i = 100; i < 301; i++)
        {
            IndexedBean idxBean = new IndexedBean();
            idxBean.setRowKey(idxBeans.get(i).getRowKey());
            idxBean.setIntVal(idxBeans.get(i).getIntVal());
            idxBean.setLongVal(idxBeans.get(i).getLongVal());

            idxExpecteds.add(idxBean);
        }

        assertEquals(idxExpecteds.subList(100, 201), idxActuals);
        for(IndexedBean idxBean : idxActuals)
            assertTrue(((IEnhancedEntity) idxBean).getModifiedFields().isEmpty());
        
        IndexedBean startIdxTmpl = new IndexedBean();
        startIdxTmpl.setLongVal(1);
        idxActuals = new ArrayList<IndexedBean>(_indexedDao.mfindBetween(startIdxTmpl, idxTmpl, Collections.singleton("intVal"), null));
        Collections.sort(idxActuals);

        assertEquals(idxExpecteds, idxActuals);
        for(IndexedBean idxBean : idxActuals)
            assertTrue(((IEnhancedEntity) idxBean).getModifiedFields().isEmpty());

    }

    @Test
    public void testRangeFindPartialRange() throws Exception
    {
        int numBeans = CassandraDaoBase.ROW_RANGE_SIZE;
        List<IndexedBean> idxBeans = new ArrayList<IndexedBean>();
        for(int i = 0; i < numBeans; i++)
        {
            IndexedBean idxBean = new IndexedBean();
            idxBean.setRowKey(new Long(i));
            idxBean.setCharVal('c');
            idxBean.setIntVal(i);
            idxBean.setLongVal(i/10);
            idxBean.setStrVal("strval");

            idxBeans.add(idxBean);
        }

        _indexedDao.mput(idxBeans);

        IndexedBean idxTmpl = new IndexedBean();
        idxTmpl.setLongVal(5);
        List<IndexedBean> idxActuals = new ArrayList<IndexedBean>(_indexedDao.mfind(idxTmpl, "intVal", "t"));
        List<IndexedBean> idxExpecteds = new ArrayList<IndexedBean>();

        Collections.sort(idxActuals);
        for(int i = 50; i < 70; i++)
        {
            IndexedBean idxBean = new IndexedBean();
            idxBean.setRowKey(idxBeans.get(i).getRowKey());
            idxBean.setIntVal(idxBeans.get(i).getIntVal());
            idxBean.setLongVal(idxBeans.get(i).getLongVal());
            idxBean.setStrVal(idxBeans.get(i).getStrVal());

            idxExpecteds.add(idxBean);
        }

        assertEquals(idxExpecteds.subList(0, 10), idxActuals);
        for(IndexedBean idxBean : idxActuals)
            assertTrue(((IEnhancedEntity) idxBean).getModifiedFields().isEmpty());
        
        IndexedBean endIdxTmpl = new IndexedBean();
        endIdxTmpl.setLongVal(6);
        idxActuals = new ArrayList<IndexedBean>(_indexedDao.mfindBetween(idxTmpl, endIdxTmpl, "intVal", "t"));
        Collections.sort(idxActuals);
        assertEquals(idxExpecteds, idxActuals);
        for(IndexedBean idxBean : idxActuals)
            assertTrue(((IEnhancedEntity) idxBean).getModifiedFields().isEmpty());
    }

    @Test
    public void testRangeCompositeIndex() throws Exception
    {
        int numBeans = 30;
        List<IndexedBean> idxBeans = new ArrayList<IndexedBean>();
        for(int i = 0; i < numBeans; i++)
        {
            IndexedBean idxBean = new IndexedBean();
            idxBean.setRowKey(new Long(i));
            idxBean.setCharVal('c');
            idxBean.setIntVal(i);
            idxBean.setLongVal((i%5)/2);
            idxBean.setStrVal("strval-" + i/5);
            
            idxBeans.add(idxBean);
        }
        
        _indexedDao.mput(idxBeans);
        
        IndexedBean idxTmpl = new IndexedBean();
        idxTmpl.setStrVal("strval-1");
        idxTmpl.setLongVal(1);
        List<IndexedBean> idxActuals = new ArrayList<IndexedBean>(_indexedDao.mfind(idxTmpl));

        Collections.sort(idxActuals);
        assertEquals(idxBeans.subList(7, 9), idxActuals);
        for(IndexedBean idxBean : idxActuals)
            assertTrue(((IEnhancedEntity) idxBean).getModifiedFields().isEmpty());
        
        IndexedBean startTmpl = new IndexedBean(), endTmpl = new IndexedBean();
        startTmpl.setStrVal("strval-1");
        endTmpl.setStrVal("strval-2");
        endTmpl.setLongVal(1L);
        idxActuals = new ArrayList<IndexedBean>(_indexedDao.mfindBetween(startTmpl, endTmpl));

        Collections.sort(idxActuals);
        assertEquals(idxBeans.subList(5, 14), idxActuals);
        for(IndexedBean idxBean : idxActuals)
            assertTrue(((IEnhancedEntity) idxBean).getModifiedFields().isEmpty());
        
        startTmpl.setStrVal("strval-1");
        startTmpl.setLongVal(2L);
        idxActuals = new ArrayList<IndexedBean>(_indexedDao.mfindBetween(startTmpl, endTmpl));

        Collections.sort(idxActuals);
        assertEquals(idxBeans.subList(9, 14), idxActuals);
        for(IndexedBean idxBean : idxActuals)
            assertTrue(((IEnhancedEntity) idxBean).getModifiedFields().isEmpty());
    }
    
    @Test
    public void testIndexPartitioning() throws Exception
    {
        PartitionIndexBeanDao dao = new PartitionIndexBeanDao();
        dao.setKeyspaceFactory(_pm);
        dao.init();
        
        int numBeans = 100;
        List<PartitionedIndexBean> idxBeans = new ArrayList<PartitionedIndexBean>();
        for(long i = 0; i < numBeans; i++)
        {
            PartitionedIndexBean idxBean = new PartitionedIndexBean();
            idxBean.setRowKey(i);
            idxBean.setPartitionedValue(i/10);
            
            idxBeans.add(idxBean);
        }
        dao.mput(idxBeans);
        assertEquals(numBeans, TestPartitioner.partitionHistory().size());
        /*
         * check index table row count
         */
        BytesArraySerializer bas = BytesArraySerializer.get();
        RangeSlicesQuery<byte[],byte[],byte[]> query = HFactory.createRangeSlicesQuery(keyspace, bas, bas, bas);
        query.setKeys(null, null);
        query.setColumnFamily("pib_idx");
        query.setRange(null, null, false, 100);
        
        Iterator<Row<byte[], byte[], byte[]>> iterator = query.execute().get().iterator();
        int cnt = 0;
        while(iterator.hasNext())
        {
            cnt++;
            Row<byte[], byte[], byte[]> row = iterator.next();
            assertEquals(10, row.getColumnSlice().getColumns().size());
        }
        
        assertEquals(numBeans/10, cnt);
        
        TestPartitioner.partitionHistory().clear();
        assertTrue(TestPartitioner.rangePartitionHistory().isEmpty());
        
        /*
         * range find
         */
        PartitionedIndexBean tmpl = new PartitionedIndexBean();
        tmpl.setPartitionedValue(5L);
        List<PartitionedIndexBean> actual = new ArrayList<PartitionedIndexBean>(dao.mfind(tmpl));
        Collections.sort(actual);
        
        assertEquals(idxBeans.subList(50, 60), actual);
        assertEquals(1, TestPartitioner.partitionHistory().size());
        assertEquals(5, TestPartitioner.partitionHistory().get(0).get(0));
        assertTrue(TestPartitioner.rangePartitionHistory().isEmpty());
        
        TestPartitioner.partitionHistory().clear();
        
        /*
         * range between find
         */
        PartitionedIndexBean endTmpl = new PartitionedIndexBean();
        endTmpl.setPartitionedValue(7L);
        actual = new ArrayList<PartitionedIndexBean>(dao.mfindBetween(tmpl, endTmpl));
        Collections.sort(actual);
        
        assertEquals(idxBeans.subList(50, 80), actual);
        assertTrue(TestPartitioner.partitionHistory().isEmpty());
        assertEquals(1, TestPartitioner.rangePartitionHistory().size());
        assertEquals(Arrays.asList(new Long[] {5L,6L,7L}), TestPartitioner.rangePartitionHistory().get(0));
        
        TestPartitioner.partitionHistory().clear();
    }
    
    private class RecordingStrategy implements IStaleIndexValueStrategy 
    {
        List<StaleIndexUpdateRecord> records = new ArrayList<StaleIndexUpdateRecord>();
        
        @Override
        public void handle(EntityMetadata<?> entity, IndexMetadata index, Collection<StaleIndexValue> values)
        {
            StaleIndexUpdateRecord record = new StaleIndexUpdateRecord();
            record.entityMetadata = entity;
            record.values = values;
            record.index = index;
            
            records.add(record);
        }
    }

    private class StaleIndexUpdateRecord 
    {
        EntityMetadata<?> entityMetadata;
        Collection<StaleIndexValue> values;
        IndexMetadata index;
    }

}
