package com.feedly.cassandra.dao;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import org.junit.Before;
import org.junit.Test;

import com.feedly.cassandra.PersistenceManager;
import com.feedly.cassandra.entity.enhance.IndexedBean;
import com.feedly.cassandra.test.CassandraServiceTestBase;

public class InlineRepairStrategyTest extends CassandraServiceTestBase
{
    PersistenceManager _pm;
    IndexedBeanDao _dao;

    @Before
    public void before()
    {
        _pm = new PersistenceManager();
        
        _dao = new IndexedBeanDao();
        _dao.setKeyspaceFactory(_pm);
        InlineRepairStrategy strategy = new InlineRepairStrategy();
        strategy.setKeyspaceFactory(_pm);
        _dao.setStaleValueIndexStrategy(strategy);
        _dao.init();
        
        configurePersistenceManager(_pm);
        
        _pm.setPackagePrefixes(new String[] {IndexedBean.class.getPackage().getName()});
        _pm.init();
    }

    @Test
    public void testRepair()
    {
        int numBeans = 20;
        List<IndexedBean> beans = new ArrayList<IndexedBean>();
        for(int i = 0; i < numBeans; i++)
        {
            IndexedBean idxBean = new IndexedBean();
            idxBean.setRowKey(new Long(i));
            idxBean.setCharVal((char) ('a' + i));
            idxBean.setIntVal(i);
            idxBean.setLongVal(i/2L);
            idxBean.setStrVal("strval");
            
            beans.add(idxBean);
        }
        
        _dao.mput(beans);
        
        for(IndexedBean bean : beans.subList(6, 12))
            bean.setLongVal(-1L);

        _dao.mput(beans);

        IndexedBean tmpl = new IndexedBean();
        tmpl.setLongVal(0L);
        IndexedBean endtmpl = new IndexedBean();
        endtmpl.setLongVal(100L);
        
        //access incorrect values to trigger repair
        _dao.mfindBetween(tmpl, endtmpl);
        
        //incorrect values accessed, check repair was performed (size of index is == to row count)
        BytesArraySerializer bas = BytesArraySerializer.get();
        DynamicCompositeSerializer cas = new DynamicCompositeSerializer();

        RangeSlicesQuery<DynamicComposite,DynamicComposite,byte[]> query = HFactory.createRangeSlicesQuery(keyspace, cas, cas, bas);
        query.setKeys(null, null);
        query.setColumnFamily("indexedbean_idx");
        query.setRange(null, null, false, 100);
        
        Row<DynamicComposite, DynamicComposite, byte[]> row = null;
        
        Iterator<Row<DynamicComposite, DynamicComposite, byte[]>> iterator = query.execute().get().iterator();
        while(iterator.hasNext())
        {
            row = iterator.next();
            if(row.getKey().size() == 2 && row.getKey().get(0).equals("longVal"))
                break;
            else
                row = null;
        }
        
        assertEquals(numBeans, row.getColumnSlice().getColumns().size());

        //check all values are accessible
        tmpl.setLongVal(-1L);
        List<IndexedBean> actuals = new ArrayList<IndexedBean>(_dao.mfind(tmpl));
        Collections.sort(actuals);
        assertEquals(beans.subList(6, 12), actuals);
        
        for(int i = 0; i < numBeans; i += 2)
        {
            if(i >= 6 && i <=12)
                continue;
            
            tmpl = new IndexedBean();
            tmpl.setLongVal(i/2L);
            
            actuals = new ArrayList<IndexedBean>(_dao.mfind(tmpl));
            Collections.sort(actuals);
            assertEquals(beans.subList(i, i+2), actuals);
        }
    }
}
