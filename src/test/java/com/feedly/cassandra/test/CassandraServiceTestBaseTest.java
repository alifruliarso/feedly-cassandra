package com.feedly.cassandra.test;

import static org.junit.Assert.*;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import org.junit.BeforeClass;
import org.junit.Test;

public class CassandraServiceTestBaseTest
{
    CassandraServiceTestBase test = new CassandraServiceTestBase();
    
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        CassandraServiceTestBase.beforeClass();
    }
    
    private void assertColFamilyCount(String family, int expected)
    {
        RangeSlicesQuery<String, String, String> q = HFactory.createRangeSlicesQuery(CassandraServiceTestBase.keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());

        q.setColumnFamily(family).setKeys("", "").setReturnKeysOnly(); //assumes test case creates no more than 100 rows

        assertEquals(expected, q.execute().get().getCount());
    }
    
    @Test
    public void testDeleteAll()
    {
        CassandraServiceTestBase.createColumnFamily("ColFamily0");
        CassandraServiceTestBase.createColumnFamily("ColFamily1");
        Mutator<String> mutator = HFactory.createMutator(CassandraServiceTestBase.keyspace, StringSerializer.get());
        for(int i = 0; i < 10; i++)
            mutator.insert("test" + i, "ColFamily" + i/5, HFactory.createStringColumn("col", "val" + i)); 
        

        assertColFamilyCount("ColFamily0", 5);
        assertColFamilyCount("ColFamily1", 5);

        test.deleteAll();

        //should be able to recreate the same tables.
        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(CassandraServiceTestBase.KEYSPACE, "ColFamily0");
        CassandraServiceTestBase.cluster.addColumnFamily(cfDef);    
        cfDef = HFactory.createColumnFamilyDefinition(CassandraServiceTestBase.KEYSPACE, "ColFamily1");
        CassandraServiceTestBase.cluster.addColumnFamily(cfDef);    

    }
}
