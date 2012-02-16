package com.feedly.cassandra.dao;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Test;

import com.feedly.cassandra.PersistenceManager;
import com.feedly.cassandra.bean.BeanUtils;
import com.feedly.cassandra.bean.ByteIndicatorSerializer;
import com.feedly.cassandra.bean.enhance.IEnhancedBean;
import com.feedly.cassandra.bean.enhance.IndexedBean;
import com.feedly.cassandra.bean.enhance.SampleBean;
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
    
    @Before
    public void before()
    {
        _pm = new PersistenceManager();
        _dao = new SampleBeanDao();
        _dao.setKeyspaceFactory(_pm);
        _mapDao = new MapBeanDao();
        _mapDao.setKeyspaceFactory(_pm);
        _sortedMapDao = new SortedMapBeanDao();
        _sortedMapDao.setKeyspaceFactory(_pm);
        _listDao = new ListBeanDao();
        _listDao.setKeyspaceFactory(_pm);
        _indexedDao = new IndexedBeanDao();
        _indexedDao.setKeyspaceFactory(_pm);
        _compositeIndexedDao = new CompositeIndexedBeanDao();
        _compositeIndexedDao.setKeyspaceFactory(_pm);

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
        assertEquals(message, expected, BeanUtils.getSerializer(expected.getClass()).fromBytes(value));
    }
    
    /*
     * begin test cases
     */

//    @Test
//    public void testSimpleSave()
//    {
//        int numBeans = 5;
//        List<SampleBean> beans = new ArrayList<SampleBean>();
//        
//        for(int i = 0; i < numBeans; i++)
//        {
//            SampleBean bean = new SampleBean();
//            bean.setRowKey(new Long(i));
//            bean.setBoolVal(i%2 == 0);
//            bean.setCharVal((char) ('a' + i));
//            bean.setDateVal(new Date(System.currentTimeMillis() + 60000*i));
//            bean.setDoubleVal(i * .1);
//            bean.setFloatVal(i / .5f);
//            bean.setIntVal(i);
//            bean.setLongVal(-i);
//            bean.setStrVal("str-" + i);
//            bean.setUnmapped(new HashMap<String, Object>());
//            for(int j = 0; j <= i; j++)
//                bean.getUnmapped().put("unmapped-" + j, j);
//            beans.add(bean);
//        }
//        
//        for(SampleBean bean : beans)
//        {
//            _dao.save(bean);
//            assertTrue(((IEnhancedBean) bean).getModifiedFields().isEmpty());
//            assertFalse(((IEnhancedBean) bean).getUnmappedFieldsModified());
//            SliceQuery<Long,String,byte[]> query = HFactory.createSliceQuery(keyspace, LongSerializer.get(), AsciiSerializer.get(), BytesArraySerializer.get());
//            query.setKey(bean.getRowKey());
//            query.setColumnFamily("sample");
//            query.setRange("", "", false, 100);
//            ColumnSlice<String, byte[]> columnSlice = query.execute().get();
//            
//            assertBean("beans[" + bean.getIntVal() + "]", bean, columnSlice);
//        }
//        
//        SampleBean bean0 = beans.get(0);
//        //make sure only dirty fields are saved
//        bean0.setStrVal("updated");
//        bean0.setDoubleVal(100.0);
//        bean0.setUnmapped((Map) Collections.singletonMap("unmapped-0", 100));
//        IEnhancedBean bean = (IEnhancedBean) bean0;
//        bean.getModifiedFields().clear(7);
//        bean.setUnmappedFieldsModified(false);
//        
//        _dao.save(bean0);
//        bean0.setStrVal("str-0");
//        bean0.setUnmapped((Map) Collections.singletonMap("unmapped-0", 0));
//
//        SliceQuery<Long,String,byte[]> query = HFactory.createSliceQuery(keyspace, LongSerializer.get(), AsciiSerializer.get(), BytesArraySerializer.get());
//        query.setKey(bean0.getRowKey());
//        query.setColumnFamily("sample");
//        query.setRange("", "", false, 100);
//
//        assertBean("dirty test", bean0, query.execute().get());
//    }
//
//
//    
//    @Test
//    public void testCollectionSave()
//    {
//        /*
//         * Map
//         */
//
//        MapBean mapBean = new MapBean();
//        mapBean.setRowkey(10L);
//
//        mapBean.setMapProp(new HashMap<String, Object>());
//        mapBean.getMapProp().put("longMapProp", 100L);
//        mapBean.getMapProp().put("strMapProp", "strMapProp-val");
//        mapBean.setStrProp("strProp-val");
//        mapBean.setStrProp1("strProp1-val");
//        mapBean.setUnmapped((Map)Collections.singletonMap("unmapped-1", "val1"));
//        _mapDao.save(mapBean);
//        
//        DynamicCompositeSerializer dcs = new DynamicCompositeSerializer();
//        SliceQuery<Long, DynamicComposite, byte[]> query = HFactory.createSliceQuery(keyspace, LongSerializer.get(), dcs, BytesArraySerializer.get());
//        query.setKey(mapBean.getRowkey());
//        query.setColumnFamily("mapbean");
//        
//        query.setRange(new DynamicComposite(), new DynamicComposite(), false, 100);
//
//        ColumnSlice<DynamicComposite, byte[]> slice = query.execute().get();
//        
//        
//        assertEquals(5, slice.getColumns().size());
//        System.out.println(slice.getColumns());
//        assertColumn(mapBean.getStrProp(), false, slice.getColumnByName(new DynamicComposite("strProp")));
//        assertColumn(mapBean.getStrProp1(), false, slice.getColumnByName(new DynamicComposite("strProp1")));
//        assertColumn(mapBean.getMapProp().get("strMapProp"), true, slice.getColumnByName(new DynamicComposite("mapProp", "strMapProp")));
//        assertColumn(mapBean.getMapProp().get("longMapProp"), true, slice.getColumnByName(new DynamicComposite("mapProp", "longMapProp")));
//        
//        //try serializing using unmapped handler with specified serializer 
//        assertColumn(mapBean.getUnmapped().get("unmapped-1"), false, slice.getColumnByName(new DynamicComposite("unmapped-1")));
//        
//        //null out a value and resave
//        mapBean.getMapProp().put("strMapProp", null);
//        mapBean.setUnmapped(Collections.<String, String>singletonMap("unmapped-1", null));
//        _mapDao.save(mapBean);
//        
//        slice = query.execute().get();
//        
//        assertEquals(3, slice.getColumns().size());
//        assertColumn(mapBean.getStrProp(), false, slice.getColumnByName(new DynamicComposite("strProp")));
//        assertColumn(mapBean.getStrProp1(), false, slice.getColumnByName(new DynamicComposite("strProp1")));
//        assertColumn(mapBean.getMapProp().get("longMapProp"), true, slice.getColumnByName(new DynamicComposite("mapProp", "longMapProp")));
//
//        /*
//         * SortedMap
//         */
//        SortedMapBean sortedMapBean = new SortedMapBean();
//        sortedMapBean.setRowkey(10L);
//        
//        sortedMapBean.setMapProp(new TreeMap<String, Object>());
//        sortedMapBean.getMapProp().put("longMapProp", 100L);
//        sortedMapBean.getMapProp().put("strMapProp", "strMapProp-val");
//        sortedMapBean.setStrProp("strProp-val");
//        sortedMapBean.setStrProp1("strProp1-val");
//        
//        _sortedMapDao.save(sortedMapBean);
//        
//        query = HFactory.createSliceQuery(keyspace, LongSerializer.get(), dcs, BytesArraySerializer.get());
//        query.setKey(sortedMapBean.getRowkey());
//        query.setColumnFamily("sortedmapbean");
//        
//        query.setRange(new DynamicComposite(), new DynamicComposite(), false, 100);
//        
//        slice = query.execute().get();
//        
//        
//        assertEquals(4, slice.getColumns().size());
//        assertColumn(sortedMapBean.getStrProp(), false, slice.getColumnByName(new DynamicComposite("strProp")));
//        assertColumn(sortedMapBean.getStrProp1(), false, slice.getColumnByName(new DynamicComposite("strProp1")));
//        assertColumn(sortedMapBean.getMapProp().get("strMapProp"), true, slice.getColumnByName(new DynamicComposite("mapProp", "strMapProp")));
//        assertColumn(sortedMapBean.getMapProp().get("longMapProp"), true, slice.getColumnByName(new DynamicComposite("mapProp", "longMapProp")));
//        
//        //null out a value and resave
//        sortedMapBean.getMapProp().put("strMapProp", null);
//        _sortedMapDao.save(sortedMapBean);
//        
//        slice = query.execute().get();
//        
//        assertEquals(3, slice.getColumns().size());
//        assertColumn(sortedMapBean.getStrProp(), false, slice.getColumnByName(new DynamicComposite("strProp")));
//        assertColumn(sortedMapBean.getStrProp1(), false, slice.getColumnByName(new DynamicComposite("strProp1")));
//        assertColumn(sortedMapBean.getMapProp().get("longMapProp"), true, slice.getColumnByName(new DynamicComposite("mapProp", "longMapProp")));
//
//        /*
//         * List
//         */
//        
//        ListBean bean = new ListBean();
//        bean.setRowkey(10L);
//        
//        bean.setListProp(new ArrayList<Object>());
//        bean.getListProp().add("strMapProp-val");
//        bean.getListProp().add(100L);
//        bean.setStrProp("strProp-val");
//        bean.setStrProp1("strProp1-val");
//        
//        _listDao.save(bean);
//        
//        query = HFactory.createSliceQuery(keyspace, LongSerializer.get(), dcs, BytesArraySerializer.get());
//        query.setKey(bean.getRowkey());
//        query.setColumnFamily("listbean");
//        
//        query.setRange(new DynamicComposite(), new DynamicComposite(), false, 100);
//        
//        slice = query.execute().get();
//        
//        
//        assertEquals(4, slice.getColumns().size());
//        assertColumn(bean.getStrProp(), false, slice.getColumnByName(new DynamicComposite("strProp")));
//        assertColumn(bean.getStrProp1(), false, slice.getColumnByName(new DynamicComposite("strProp1")));
//        assertColumn(bean.getListProp().get(0), true, slice.getColumnByName(new DynamicComposite("listProp", 0)));
//        assertColumn(bean.getListProp().get(1), true, slice.getColumnByName(new DynamicComposite("listProp", 1)));
//        
//        //null out a value and resave
//        bean.getListProp().set(0, null);
//        _listDao.save(bean);
//        
//        slice = query.execute().get();
//        
//        assertEquals(3, slice.getColumns().size());
//        assertColumn(bean.getStrProp(), false, slice.getColumnByName(new DynamicComposite("strProp")));
//        assertColumn(bean.getStrProp1(), false, slice.getColumnByName(new DynamicComposite("strProp1")));
//        assertColumn(bean.getListProp().get(1), true, slice.getColumnByName(new DynamicComposite("listProp", 0)));
//    }
//
//    
//    @Test
//    public void testSimpleLoad()
//    {
//        int numBeans = 5;
//        List<SampleBean> beans = new ArrayList<SampleBean>();
//        
//        for(int i = 0; i < numBeans; i++)
//        {
//            SampleBean bean = new SampleBean();
//            bean.setRowKey(new Long(i));
//            bean.setBoolVal(i%2 == 0);
//            bean.setCharVal((char) ('a' + i));
//            bean.setDateVal(new Date(System.currentTimeMillis() + 60000*i));
//            bean.setDoubleVal(i * .1);
//            bean.setFloatVal(i / .5f);
//            bean.setIntVal(i);
//            bean.setLongVal(-i);
//            bean.setStrVal("str-" + i);
//            
//            bean.setUnmapped(new HashMap<String, Object>());
//            for(int j = 0; j <= 100; j++)
//                bean.getUnmapped().put("unmapped-" + j, "val-" + i + "-" + j);
//            
//            beans.add(bean);
//        }
//        
//        for(SampleBean bean : beans)
//            _dao.save(bean);
//        
//        for(SampleBean bean : beans)
//        {
//            SampleBean loaded = _dao.load(bean.getRowKey());
//            assertTrue(((IEnhancedBean) loaded).getModifiedFields().isEmpty());
//            assertFalse(((IEnhancedBean) loaded).getUnmappedFieldsModified());
//            assertEquals(bean, loaded);
//            assertNotNull(loaded.getUnmapped());
//            assertFalse(loaded.getUnmapped().isEmpty());
//            
//        }
//        
//        SampleBean bean0 = beans.get(0);
//        //test null update
//        bean0.setStrVal(null);
//        bean0.setLongVal(2000);
//        _dao.save(bean0);
//        assertEquals(bean0, _dao.load(bean0.getRowKey()));
//
//        
//        //test partial
//        
//        SampleBean partial = new SampleBean();
//        partial.setRowKey(1000L);
//        partial.setStrVal("hello");
//        _dao.save(partial);
//        assertEquals(partial, _dao.load(partial.getRowKey()));
//        
//        //test non-existent
//        assertNull(_dao.load(-5L));
//    }
//    
//    @Test
//    public void testSimpleBulkLoad()
//    {
//        int numBeans = 5;
//        List<SampleBean> beans = new ArrayList<SampleBean>();
//        List<Long> keys = new ArrayList<Long>();
//        
//        for(int i = 0; i < numBeans; i++)
//        {
//            SampleBean bean = new SampleBean();
//            bean.setRowKey(new Long(i));
//            bean.setBoolVal(i%2 == 0);
//            bean.setCharVal((char) ('a' + i));
//            bean.setDateVal(new Date(System.currentTimeMillis() + 60000*i));
//            bean.setDoubleVal(i * .1);
//            bean.setFloatVal(i / .5f);
//            bean.setIntVal(i);
//            bean.setLongVal(-i);
//            bean.setStrVal("str-" + i);
//            
//            beans.add(bean);
//            keys.add(beans.get(i).getRowKey());
//            
//            bean.setUnmapped(new HashMap<String, Object>());
//            for(int j = 0; j <= 100; j++)
//                bean.getUnmapped().put("unmapped-" + j, "val-" + i + "-" + j);
//        }
//        
//        _dao.save(beans);
//
//        List<Long> keyList = new ArrayList<Long>(keys);
//        keyList.add(-5L); //non-existent
//        List<SampleBean> actual = new ArrayList<SampleBean>( _dao.bulkLoad(keyList) );
//
//        Collections.sort(actual);
//        assertEquals(beans.size(), actual.size());
//        
//        for(int i = beans.size() - 1; i >= 0; i--)
//        {
//            SampleBean loaded = actual.get(i);
//            assertEquals("bean[" + i + "]", beans.get(i), loaded);
//
//            assertTrue(((IEnhancedBean) loaded).getModifiedFields().isEmpty());
//        }
//    }
//
//    @Test
//    public void testMapLoad()
//    {
//        int numBeans = 5;
//        List<MapBean> beans = new ArrayList<MapBean>();
//        List<Long> keys = new ArrayList<Long>();
//        
//        for(int i = 0; i < numBeans; i++)
//        {
//            MapBean bean = new MapBean();
//            bean.setRowkey(new Long(i));
//            bean.setStrProp("str-" + i);
//            keys.add(bean.getRowkey());
//            bean.setUnmapped(new HashMap<String, String>());
//            for(int j = 0; j <= 50; j++)
//                bean.getUnmapped().put("unmapped-" + j, "val-" + i + "-" + j);
//
//            bean.setMapProp(new HashMap<String, Object>());
//            for(int j = 50; j <= 100; j++)
//                bean.getMapProp().put("propval-" + j, "val-" + i + "-" + j);
//
//            beans.add(bean);
//        }
//        
//        _mapDao.save(beans);
//        
//        for(MapBean bean : beans)
//        {
//            MapBean loaded = _mapDao.load(bean.getRowkey());
//            assertTrue(((IEnhancedBean) loaded).getModifiedFields().isEmpty());
//            assertFalse(((IEnhancedBean) loaded).getUnmappedFieldsModified());
//            assertEquals(bean, loaded);
//            assertNotNull(loaded.getUnmapped());
//            assertFalse(loaded.getUnmapped().isEmpty());
//            
//        }
//        
//        
//        //bulk load
//        List<MapBean> loaded = new ArrayList<MapBean>( _mapDao.bulkLoad(keys));
//        Collections.sort(loaded);
//        for(MapBean bean : beans)
//        {
//            assertTrue(((IEnhancedBean) bean).getModifiedFields().isEmpty());
//        }
//        assertEquals(beans, loaded);
//
//        MapBean bean0 = beans.get(0);
//        //test null update
//        for(int i = 50; i < 75; i++)
//            bean0.getMapProp().put("propval-" + i, null);
//        assertEquals(51, bean0.getMapProp().size()); //sanity check we are overwriting properties to null
//        
//        _mapDao.save(bean0);
//
//        Iterator<Entry<String, Object>> iter = bean0.getMapProp().entrySet().iterator();
//        while(iter.hasNext())
//        {
//            if(iter.next().getValue() == null)
//                iter.remove();
//        }
//        
//        assertEquals(bean0, _mapDao.load(bean0.getRowkey()));
//    }
//    
//    @Test
//    public void testSortedMapLoad()
//    {
//        int numBeans = 5;
//        List<SortedMapBean> beans = new ArrayList<SortedMapBean>();
//        List<Long> keys = new ArrayList<Long>();
//        
//        for(int i = 0; i < numBeans; i++)
//        {
//            SortedMapBean bean = new SortedMapBean();
//            bean.setRowkey(new Long(i));
//            bean.setStrProp("str-" + i);
//            keys.add(bean.getRowkey());
//            
//            bean.setMapProp(new TreeMap<String, Object>());
//            for(int j = 50; j <= 100; j++)
//                bean.getMapProp().put("propval-" + j, "val-" + i + "-" + j);
//            
//            beans.add(bean);
//        }
//        
//        _sortedMapDao.save(beans);
//        
//        for(SortedMapBean bean : beans)
//        {
//            SortedMapBean loaded = _sortedMapDao.load(bean.getRowkey());
//            assertTrue(((IEnhancedBean) loaded).getModifiedFields().isEmpty());
//            assertEquals(bean, loaded);
//        }
//        
//        //bulk load
//        List<SortedMapBean> loaded = new ArrayList<SortedMapBean>( _sortedMapDao.bulkLoad(keys));
//        Collections.sort(loaded);
//        for(SortedMapBean bean : beans)
//        {
//            assertTrue(((IEnhancedBean) bean).getModifiedFields().isEmpty());
//        }
//        assertEquals(beans, loaded);
//
//        
//        SortedMapBean bean0 = beans.get(0);
//        //test null update
//        for(int i = 50; i < 75; i++)
//            bean0.getMapProp().put("propval-" + i, null);
//        assertEquals(51, bean0.getMapProp().size()); //sanity check we are overwriting properties to null
//        
//        _sortedMapDao.save(bean0);
//        
//        Iterator<Entry<String, Object>> iter = bean0.getMapProp().entrySet().iterator();
//        while(iter.hasNext())
//        {
//            if(iter.next().getValue() == null)
//                iter.remove();
//        }
//        
//        assertEquals(bean0, _sortedMapDao.load(bean0.getRowkey()));
//    }
//
//    @Test
//    public void testListLoad()
//    {
//        int numBeans = 5;
//        List<ListBean> beans = new ArrayList<ListBean>();
//        List<Long> keys = new ArrayList<Long>();
//        
//        for(int i = 0; i < numBeans; i++)
//        {
//            ListBean bean = new ListBean();
//            bean.setRowkey(new Long(i));
//            bean.setStrProp("str-" + i);
//            keys.add(bean.getRowkey());
//            bean.setListProp(new ArrayList<Object>());
//            for(int j = 0; j <= 100; j++)
//                bean.getListProp().add("val-" + i + "-" + j);
//            
//            beans.add(bean);
//        }
//        
//        _listDao.save(beans);
//        
//        for(ListBean bean : beans)
//        {
//            ListBean loaded = _listDao.load(bean.getRowkey());
//            assertTrue(((IEnhancedBean) loaded).getModifiedFields().isEmpty());
//            assertEquals(bean, loaded);
//        }
//
//        //bulk load
//        List<ListBean> loaded = new ArrayList<ListBean>( _listDao.bulkLoad(keys));
//        Collections.sort(loaded);
//        for(ListBean bean : beans)
//        {
//            assertTrue(((IEnhancedBean) bean).getModifiedFields().isEmpty());
//        }
//        assertEquals(beans, loaded);
//
//        
//        ListBean bean0 = beans.get(0);
//        //test null update
//        for(int i = 0; i < 50; i++)
//            bean0.getListProp().set(i, null);
//        
//        _listDao.save(bean0);
//        
//        Iterator<Object> iter = bean0.getListProp().iterator();
//        while(iter.hasNext())
//        {
//            if(iter.next() == null)
//                iter.remove();
//        }
//        
//        assertEquals(bean0, _listDao.load(bean0.getRowkey()));
//    }
//
//    @Test
//    public void testPartialLoad() throws Exception
//    {
//        int numBeans = 5;
//        List<SampleBean> beans = new ArrayList<SampleBean>();
//        List<Long> keys = new ArrayList<Long>();
//        
//        for(int i = 0; i < numBeans; i++)
//        {
//            SampleBean bean = new SampleBean();
//            bean.setRowKey(new Long(i));
//            bean.setBoolVal(i%2 == 0);
//            bean.setCharVal((char) ('a' + i));
//            bean.setDateVal(new Date(System.currentTimeMillis() + 60000*i));
//            bean.setDoubleVal(i * .1);
//            bean.setFloatVal(i / .5f);
//            bean.setIntVal(i);
//            bean.setLongVal(-i);
//            bean.setStrVal("str-" + i);
//            
//            bean.setUnmapped(new HashMap<String, Object>());
//            for(int j = 0; j <= 20; j++)
//                bean.getUnmapped().put("unmapped-" + j, "val-" + i + "-" + j);//place them between fixed properties
//            
//            beans.add(bean);
//            keys.add(bean.getRowKey());
//        }
//
//        _dao.save(beans);
////        List<SampleBean> bulkActuals = _dao.loadPartial(keys, null, null, Collections.singleton("boolVal"));
//
//
//        for(SampleBean saved : beans)
//        {
//            SampleBean expected = (SampleBean) saved.clone();
//            
//            expected.setBoolVal(false); //false is default value for boolean
//            expected.setUnmapped(null); //can't efficiently do exclusions and include unmapped columns right now as c* ranges are inclusive
//            SampleBean actual = _dao.loadPartial(saved.getRowKey(), null, null, Collections.singleton("boolVal"));
//            
//            assertEquals(expected, actual);
//            
//            expected = new SampleBean();
//            expected.setRowKey(saved.getRowKey());
//            expected.setCharVal(saved.getCharVal());
//            TreeMap<String, Object> unmapped = new TreeMap<String, Object>(saved.getUnmapped());
//            for(int i = saved.getUnmapped().size()/2; i >= 0; i--)
//                unmapped.remove(unmapped.firstEntry().getKey());
//            
//            expected.setUnmapped(unmapped);
//            
//            Set<String> props = new HashSet<String>(unmapped.keySet());
//            props.add("charVal");
//            
//            actual = _dao.loadPartial(saved.getRowKey(), null, props, null);
//            assertEquals(expected, actual);
//            
//            expected.setIntVal(saved.getIntVal());
//            _dao.loadPartial(saved.getRowKey(), actual, Collections.singleton("intVal"), null); //update the bean
//            
//            assertEquals(expected, actual);
//        }
//        
//    }
//    
//    @Test
//    public void testPartialRangeLoad() throws Exception
//    {
//        int numBeans = 5;
//        List<SampleBean> beans = new ArrayList<SampleBean>();
//        List<Long> keys = new ArrayList<Long>();
//        
//        for(int i = 0; i < numBeans; i++)
//        {
//            SampleBean bean = new SampleBean();
//            bean.setRowKey(new Long(i));
//            bean.setBoolVal(i%2 == 0);
//            bean.setCharVal((char) ('a' + i));
//            bean.setDateVal(new Date(System.currentTimeMillis() + 60000*i));
//            bean.setDoubleVal(i * .1);
//            bean.setFloatVal(i / .5f);
//            bean.setIntVal(i);
//            bean.setLongVal(-i);
//            bean.setStrVal("str-" + i);
//            
//            bean.setUnmapped(new HashMap<String, Object>());
//            for(int j = 0; j <= 20; j++)
//                bean.getUnmapped().put("unmapped-" + j, "val-" + i + "-" + j);
//            
//            beans.add(bean);
//            keys.add(bean.getRowKey());
//        }
//        
//        _dao.save(beans);
//        List<SampleBean> bulkActuals = _dao.bulkLoadPartial(keys, null, "c", "cv");
//        List<SampleBean> actuals = new ArrayList<SampleBean>();
//        List<SampleBean> expecteds = new ArrayList<SampleBean>();
//        Collections.sort(bulkActuals);
//        
//        for(int i = 0; i < beans.size(); i++)
//        {
//            SampleBean saved = beans.get(i);
//            SampleBean expected = new SampleBean();
//            
//            expected.setRowKey(saved.getRowKey());
//            expected.setCharVal(saved.getCharVal());
//            expected.setUnmapped(null); //when using exclude, unmapped properties are ignored
//            actuals.add(_dao.loadPartial(saved.getRowKey(), null, "c", "cv"));
//            
//            assertEquals(expected, actuals.get(i));
//            assertEquals(expected, bulkActuals.get(i));
//            
//            expecteds.add(expected);
//        }
//
//        bulkActuals = _dao.bulkLoadPartial(keys, bulkActuals, "d", "daa");
//        for(int i = 0; i < beans.size(); i++)
//        {
//            SampleBean saved = beans.get(i);
//            SampleBean expected = expecteds.get(i);
//            
//            actuals.set(i, _dao.loadPartial(saved.getRowKey(), actuals.get(i), "d", "daa"));//includes double's physical name
//            expected.setDoubleVal(saved.getDoubleVal());
//            assertEquals(expected, actuals.get(i));
//            assertEquals(expected, bulkActuals.get(i));
//        }
//
//        bulkActuals = _dao.bulkLoadPartial(keys, bulkActuals, "unmapped-10", "unmapped-19");
//
//        for(int i = 0; i < beans.size(); i++)
//        {
//            SampleBean saved = beans.get(i);
//            SampleBean expected = expecteds.get(i);
//            
//            expected.setUnmapped(new HashMap<String, Object>());
//            for(int j = 10; j < 20; j++)
//                expected.getUnmapped().put("unmapped-" + j, saved.getUnmapped().get("unmapped-" + j));
//
//            actuals.set(i, _dao.loadPartial(saved.getRowKey(), actuals.get(i), "unmapped-10", "unmapped-19"));
//            assertEquals(expected, actuals.get(i));
//            assertEquals(expected, bulkActuals.get(i));
//        }
//    }
//    
//
//    @Test
//    public void testPartialCollectionLoad() throws Exception
//    {
//        int numBeans = 5;
//        List<MapBean> mapBeans = new ArrayList<MapBean>();
//        List<ListBean> listBeans = new ArrayList<ListBean>();
//        List<Long> keys = new ArrayList<Long>();
//        
//        for(int i = 0; i < numBeans; i++)
//        {
//            ListBean lbean = new ListBean();
//            lbean.setRowkey(new Long(i));
//            lbean.setStrProp("str-" + i);
//            lbean.setStrProp1("str1-" + i);
//            lbean.setListProp(new ArrayList<Object>());
//            for(int j = 0; j <= 200; j++)
//                lbean.getListProp().add(i*1000 + j);
//
//            MapBean mbean = new MapBean();
//            mbean.setRowkey(new Long(i));
//            mbean.setStrProp("str-" + i);
//            mbean.setStrProp1("str1-" + i);
//            mbean.setMapProp(new HashMap<String, Object>());
//            for(int j = 0; j <= 200; j++)
//                mbean.getMapProp().put("key-" + j, j);
//
//            mapBeans.add(mbean);
//            listBeans.add(lbean);
//            keys.add(lbean.getRowkey());
//        }
//
//        _listDao.save(listBeans);
//        _mapDao.save(mapBeans);
//
//        List<ListBean> bulkListActuals = _listDao.bulkLoadPartial(keys, null, null, Collections.singleton("strProp1"));
//        assertEquals(numBeans, bulkListActuals.size());
//        Collections.sort(bulkListActuals);
//
//        for(int i = 0; i < listBeans.size(); i++)
//        {
//            ListBean saved = listBeans.get(i);
//            ListBean expected = (ListBean) saved.clone();
//            
//            expected.setStrProp1(null); 
//            ListBean actual = _listDao.loadPartial(saved.getRowkey(), null, null, Collections.singleton("strProp1"));
//            
//            assertEquals(expected, actual);
//            assertEquals(expected, bulkListActuals.get(i));
//        }
//        
//        
//        bulkListActuals = _listDao.bulkLoadPartial(keys, null, Collections.singleton(new CollectionProperty("listProp", 100)), null);
//        assertEquals(numBeans, bulkListActuals.size());
//        Collections.sort(bulkListActuals);
//        for(int i = 0; i < listBeans.size(); i++)
//        {
//            ListBean saved = listBeans.get(i);
//            /*
//             * load a single value from within a collection
//             */
//            ListBean actual = _listDao.loadPartial(saved.getRowkey(), null, Collections.singleton(new CollectionProperty("listProp", 100)), null);
//            
//            ListBean expected = new ListBean();
//            expected.setRowkey(saved.getRowkey());
//            expected.setListProp(new ArrayList<Object>());
//            for(int j = 0; j < 100; j++)
//                expected.getListProp().add(null);
//            
//            expected.getListProp().add(saved.getListProp().get(100));
//            assertEquals(expected, actual);
//            assertEquals(expected, bulkListActuals.get(i));
//        }
//        
//        List<MapBean> bulkMapActuals = _mapDao.bulkLoadPartial(keys, null, null, Collections.singleton("strProp1"));
//        assertEquals(numBeans, bulkMapActuals.size());
//        Collections.sort(bulkMapActuals);
//        for(int i = 0; i < mapBeans.size(); i++)
//        {
//            MapBean saved = mapBeans.get(i);
//            MapBean expected = (MapBean) saved.clone();
//            
//            expected.setStrProp1(null); 
//            MapBean actual = _mapDao.loadPartial(saved.getRowkey(), null, null, Collections.singleton("strProp1"));
//            
//            assertEquals(expected, actual);
//            assertEquals(expected, bulkMapActuals.get(i));
//        }
//        
//        
//        Entry<String, Object> entry = (Entry<String, Object>) mapBeans.get(0).getMapProp().entrySet().iterator().next();
//        bulkMapActuals = _mapDao.bulkLoadPartial(keys, null, Collections.singleton(new CollectionProperty("mapProp", entry.getKey())), null);
//        assertEquals(numBeans, bulkMapActuals.size());
//        Collections.sort(bulkMapActuals);
//        for(int i = 0; i < mapBeans.size(); i++)
//        {
//            MapBean saved = mapBeans.get(i);
//            /*
//             * load a single value from within a collection
//             */
//            MapBean actual = _mapDao.loadPartial(saved.getRowkey(), null, Collections.singleton(new CollectionProperty("mapProp", entry.getKey())), null);
//            
//            MapBean expected = new MapBean();
//            expected.setRowkey(saved.getRowkey());
//            expected.setMapProp(Collections.singletonMap(entry.getKey(), entry.getValue()));
//            
//            assertEquals(expected, actual);
//            assertEquals(expected, bulkMapActuals.get(i));
//        }
//    }
//    
//    @Test
//    public void testPartialCollectionRangeLoad() throws Exception
//    {
//        int numBeans = 5;
//        List<MapBean> mapBeans = new ArrayList<MapBean>();
//        List<ListBean> listBeans = new ArrayList<ListBean>();
//        List<Long> keys = new ArrayList<Long>();
//        
//        for(int i = 0; i < numBeans; i++)
//        {
//            ListBean lbean = new ListBean();
//            lbean.setRowkey(new Long(i));
//            lbean.setStrProp("str-" + i);
//            lbean.setStrProp1("str1-" + i);
//            lbean.setListProp(new ArrayList<Object>());
//            for(int j = 0; j <= 200; j++)
//                lbean.getListProp().add(i*1000 + j);
//
//            MapBean mbean = new MapBean();
//            mbean.setRowkey(new Long(i));
//            mbean.setStrProp("str-" + i);
//            mbean.setStrProp1("str1-" + i);
//            mbean.setMapProp(new HashMap<String, Object>());
//            for(int j = 0; j <= 200; j++)
//                mbean.getMapProp().put("key-" + j + "-" + i, i*1000 + j);
//
//            mapBeans.add(mbean);
//            listBeans.add(lbean);
//            keys.add(lbean.getRowkey());
//        }
//
//        _listDao.save(listBeans);
//        _mapDao.save(mapBeans);
//        
//
//        /*
//         * lists
//         */
//        //do the same test using bulk API
//        List<ListBean> bulkListActuals = _listDao.bulkLoadPartial(keys, null, "strProp1", "strProp1");
//        assertEquals(numBeans, bulkListActuals.size());
//        Collections.sort(bulkListActuals);
//        List<ListBean> singleListActuals = new ArrayList<ListBean>();
//        for(int i = 0; i < listBeans.size(); i++)
//        {
//            ListBean saved = listBeans.get(i);
//            ListBean expected = new ListBean();
//            expected.setRowkey(saved.getRowkey());
//            expected.setStrProp1(saved.getStrProp1()); 
//            
//            singleListActuals.add(_listDao.loadPartial(saved.getRowkey(), null, "strProp1", "strProp1"));
//
//            assertEquals(expected, singleListActuals.get(i));
//            assertEquals(expected, bulkListActuals.get(i));
//        }
//
//        _listDao.bulkLoadPartial(keys, bulkListActuals, new CollectionProperty("listProp", 25),  new CollectionProperty("listProp", 175));
//        assertEquals(numBeans, bulkListActuals.size());
//
//        for(int i = 0; i < listBeans.size(); i++)
//        {
//            ListBean saved = listBeans.get(i);
//
//            ListBean expected = new ListBean();
//            expected.setRowkey(saved.getRowkey());
//            expected.setStrProp1(saved.getStrProp1()); 
//            expected.setListProp(new ArrayList<Object>());
//            for(int j = 0; j <= 175; j++)
//                expected.getListProp().add(j < 25 ? null : saved.getListProp().get(j));
//
//            _listDao.loadPartial(saved.getRowkey(), singleListActuals.get(i), new CollectionProperty("listProp", 25),  new CollectionProperty("listProp", 175));
//            
//            assertEquals(expected, singleListActuals.get(i));
//            assertEquals(expected, bulkListActuals.get(i));
//        }
//        
//        /*
//         * maps
//         */
//        
//        //do the same test using bulk API
//        List<MapBean> bulkMapActuals = _mapDao.bulkLoadPartial(keys, null, "strProp1", "strProp1");
//        List<MapBean> mapActuals = new ArrayList<MapBean>();
//        List<MapBean> expectedMaps = new ArrayList<MapBean>();
//        
//        assertEquals(numBeans, bulkMapActuals.size());
//        Collections.sort(bulkMapActuals);
//        
//        for(int i = 0; i < mapBeans.size(); i++)
//        {
//            MapBean saved = mapBeans.get(i);
//            MapBean expected = new MapBean();
//            expected.setRowkey(saved.getRowkey());
//            expected.setStrProp1(saved.getStrProp1()); 
//
//            MapBean actual = _mapDao.loadPartial(saved.getRowkey(), null, "strProp1", "strProp1");
//            
//            assertEquals(expected, actual);
//            assertEquals(expected, bulkMapActuals.get(i));
//
//            mapActuals.add(actual);
//            expectedMaps.add(expected);
//        }
//        
//        
//        /*
//         * load a range from within a collection
//         */
//        String start = "key-100", end = "key-201";
//        bulkMapActuals = _mapDao.bulkLoadPartial(keys, mapActuals, new CollectionProperty("mapProp", start), new CollectionProperty("mapProp", end));
//        assertEquals(numBeans, bulkMapActuals.size());
//        for(int i = 0; i < mapBeans.size(); i++)
//        {
//            MapBean saved = mapBeans.get(i);
//            MapBean expected = expectedMaps.get(i);
//            mapActuals.set(i, _mapDao.loadPartial(saved.getRowkey(), mapActuals.get(i), new CollectionProperty("mapProp", start), new CollectionProperty("mapProp", end)));
//            
//            expected.setRowkey(saved.getRowkey());
//            expected.setMapProp(new HashMap<String, Object>());
//            for(Object o : saved.getMapProp().entrySet())
//            {
//                Entry<String, Object> e = (Entry<String, Object>) o;
//                String k = e.getKey();
//                if(k.compareTo(start) >= 0 && k.compareTo(end) <= 0)
//                    expected.getMapProp().put(k, e.getValue());
//                    
//            }
//            
//            assertEquals(expected, mapActuals.get(i));
//            assertEquals(expected, bulkMapActuals.get(i));
//        }
//    }
//
    @Test
    public void testFindByIndex() throws Exception
    {
        int numBeans = CassandraDaoBase.ROW_RANGE_SIZE+1;//force dao to do multiple ranges
        List<IndexedBean> idxBeans = new ArrayList<IndexedBean>();
        for(int i = 0; i < numBeans; i++)
        {
            IndexedBean idxBean = new IndexedBean();
            idxBean.setRowKey(new Long(i));
            idxBean.setIntVal(i/10);
            idxBean.setLongVal(i/10);
            idxBean.setStrVal("strval");
            
            idxBeans.add(idxBean);
        }
        
        _indexedDao.save(idxBeans);
        
        IndexedBean idxTmpl = new IndexedBean();
        idxTmpl.setIntVal(5);
        List<IndexedBean> idxActuals = new ArrayList<IndexedBean>(_indexedDao.bulkFindByIndex(idxTmpl));

        Collections.sort(idxActuals);
        assertEquals(idxBeans.subList(50, 60), idxActuals);
        for(IndexedBean idxBean : idxActuals)
            assertTrue(((IEnhancedBean) idxBean).getModifiedFields().isEmpty());

        idxTmpl = new IndexedBean();
        idxTmpl.setStrVal("strval");
        idxActuals = new ArrayList<IndexedBean>(_indexedDao.bulkFindByIndex(idxTmpl));
        
        Collections.sort(idxActuals);
        assertEquals(idxBeans, idxActuals);
        for(IndexedBean idxBean : idxActuals)
            assertTrue(((IEnhancedBean) idxBean).getModifiedFields().isEmpty());
    }
}
