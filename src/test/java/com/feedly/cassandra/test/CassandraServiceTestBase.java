package com.feedly.cassandra.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.logging.LogManager;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.feedly.cassandra.PersistenceManager;

/**
 * runs an embedded cassandra instance.
 * 
 * @author kireet
 *
 */
public class CassandraServiceTestBase
{
    public static final String KEYSPACE = "TestKeyspace";

    private static EmbeddedCassandraService cassandra;
    protected static Cluster cluster;
    protected static Keyspace keyspace;
    
    /**
     * Set embedded cassandra up and spawn it in a new thread.
     * 
     * @throws TTransportException
     * @throws IOException
     * @throws InterruptedException
     */
    @BeforeClass
    public static void beforeClass() throws TTransportException, IOException, InterruptedException
    {
        try
        {
            InputStream stream = CassandraServiceTestBase.class.getClassLoader().getResourceAsStream("logging.properties");
            if(stream == null)
                throw new IllegalStateException("logging.properties not found in classpath");
            
            LogManager.getLogManager().readConfiguration(stream);
            cassandra = new EmbeddedCassandraService();
            boolean started = cassandra.cleanStart();
            
            if (started)
            {
                cluster = HFactory.getOrCreateCluster("test-cluster", "localhost:9160");
                KeyspaceDefinition keyspaceDefn = HFactory.createKeyspaceDefinition(KEYSPACE,
                                                                                    ThriftKsDef.DEF_STRATEGY_CLASS,
                                                                                    1,
                                                                                    Collections.<ColumnFamilyDefinition> emptyList());
                
                cluster.addKeyspace(keyspaceDefn, true);
                keyspace = HFactory.createKeyspace(KEYSPACE, cluster);
                
            }
        }
        catch(RuntimeException re)
        {
            re.printStackTrace();
            throw re;
        }
    }

    @AfterClass
    public static void afterClass()
    {
        cluster.getConnectionManager().shutdown();
    }
    
    
    public static void configurePersistenceManager(PersistenceManager pm)
    {
        pm.setClusterName(cluster.getName());
        pm.setKeyspaceName(KEYSPACE);
        pm.setHostConfiguration(new CassandraHostConfigurator("localhost:9160"));
    }

    public static void createColumnFamily(String family)
    {
        createColumnFamily(family, ComparatorType.ASCIITYPE);
    }
    
    public static void createColumnFamily(String family, ComparatorType ctype)
    {
        KeyspaceDefinition kdef = cluster.describeKeyspace(KEYSPACE);

        for(ColumnFamilyDefinition cdef : kdef.getCfDefs())
        {
            if(cdef.getName().equals(family))
            {
                if(cdef.getComparatorType().equals(ctype))
                    return;
                else
                    throw new IllegalStateException(String.format("Column Family %s exists, but existing comparator type %s doesn't match parameter %s", 
                                                                  family, cdef.getComparatorType(), ctype));
            }
        }

        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(KEYSPACE, family, ctype);
        cluster.addColumnFamily(cfDef);    
    }
    
    @After
    public void deleteAll()
    {
        for(ColumnFamilyDefinition defn : cluster.describeKeyspace(KEYSPACE).getCfDefs())
            cluster.dropColumnFamily(KEYSPACE, defn.getName(), true);
    }
    
}