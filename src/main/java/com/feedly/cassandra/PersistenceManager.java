package com.feedly.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.model.QuorumAllConsistencyLevelPolicy;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.entity.EIndexType;
import com.feedly.cassandra.entity.EntityMetadata;
import com.feedly.cassandra.entity.IndexMetadata;
import com.feedly.cassandra.entity.PropertyMetadataBase;
import com.feedly.cassandra.entity.SimplePropertyMetadata;
import com.feedly.cassandra.entity.enhance.EntityTransformerTask;
import com.feedly.cassandra.entity.enhance.IEnhancedEntity;

public class PersistenceManager implements IKeyspaceFactory
{
    private static final Logger _logger = LoggerFactory.getLogger(PersistenceManager.class.getName());
    private static final Map<EConsistencyLevel, ConsistencyLevelPolicy> _consistencyMapping;
    
    private boolean _syncSchema = true;
    private String[] _sourcePackages = new String[0];
    private Set<Class<?>> _colFamilies;
    private String _keyspace;
    private CassandraHostConfigurator _hostConfig;
    private String _clusterName;
    private Cluster _cluster;
    private int _replicationFactor = 1;
    
    static
    {
        Map<EConsistencyLevel, ConsistencyLevelPolicy> consistencyMapping = new HashMap<EConsistencyLevel, ConsistencyLevelPolicy>();
        consistencyMapping.put(EConsistencyLevel.ONE, new AllOneConsistencyLevelPolicy());
        consistencyMapping.put(EConsistencyLevel.QUOROM, new QuorumAllConsistencyLevelPolicy());
        consistencyMapping.put(EConsistencyLevel.ALL, 
                               new ConsistencyLevelPolicy()
                               {
                                    @Override
                                    public HConsistencyLevel get(OperationType op)
                                    {
                                        return HConsistencyLevel.ALL;
                                    }
                        
                                    @Override
                                    public HConsistencyLevel get(OperationType op, String cfName)
                                    {
                                        return HConsistencyLevel.ALL;
                                    }
                
                               });
        
        _consistencyMapping = Collections.unmodifiableMap(consistencyMapping);
    }
    
    public void setReplicationFactor(int r)
    {
        _replicationFactor = r;
    }
    
    public void setClusterName(String cluster)
    {
        _clusterName = cluster;
    }
    public void setKeyspaceName(String keyspace)
    {
        _keyspace = keyspace;
    }
    
    public void setHostConfiguration(CassandraHostConfigurator config)
    {
        _hostConfig = config;
    }
    
    public void setPackagePrefixes(String[] packages)
    {
        _sourcePackages = packages;
    }
    
    public void setSyncSchema(boolean b)
    {
        _syncSchema = b;
    }
    
    public void destroy()
    {
        try
        {
            _logger.info("stopping cassandra cluster");
            HFactory.shutdownCluster(_cluster);
        }
        catch(Exception ex)
        {
            _logger.error("error shutting down cluster", ex);
        }
    }
    public void init()
    {
        if(_sourcePackages.length == 0)
            _logger.warn("No source packages configured! This is probably not right.");

        Set<Class<?>> annotated = new HashSet<Class<?>>();
        for(String pkg : _sourcePackages)
        {
            Reflections reflections = new Reflections(pkg);
            annotated.addAll(reflections.getTypesAnnotatedWith(ColumnFamily.class));
        }
        _logger.info("found {} classes", annotated.size());
        Iterator<Class<?>> iter = annotated.iterator();
        
        while(iter.hasNext())
        {
            Class<?> family = iter.next();
            boolean enh = false;
            for(Class<?> iface : family.getInterfaces())
            {
                if(iface.equals(IEnhancedEntity.class))
                {
                    enh = true;
                    break;
                }
            }
            
            if(!enh)
            {
                _logger.warn(family.getName() + " has not been enhanced after compilation, it will be ignored. See ", 
                             EntityTransformerTask.class.getName());
                
                iter.remove();
            }
        }

        _colFamilies = Collections.unmodifiableSet(annotated);
        
        _cluster = HFactory.getOrCreateCluster(_clusterName, _hostConfig);
        if(_syncSchema)
            syncKeyspace();
    }
    
    public Set<Class<?>> getColumnFamilies()
    {
        return _colFamilies;
    }
    
    private void syncKeyspace()
    {
        KeyspaceDefinition kdef = _cluster.describeKeyspace(_keyspace);
        if(kdef == null)
        {
            kdef = HFactory.createKeyspaceDefinition(_keyspace,
                                                     ThriftKsDef.DEF_STRATEGY_CLASS,
                                                     _replicationFactor,
                                                     new ArrayList<ColumnFamilyDefinition>());
            
            _cluster.addKeyspace(kdef, true);
        }
        
        for(Class<?> family : _colFamilies)
        {
            syncColumnFamily(family, kdef);
        }
    }
    
    public Keyspace createKeyspace(EConsistencyLevel level)
    {
        return HFactory.createKeyspace(_keyspace, _cluster, _consistencyMapping.get(level));
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void syncColumnFamily(Class<?> family, KeyspaceDefinition keyspaceDef)
    {
        ColumnFamily annotation = family.getAnnotation(ColumnFamily.class);
        EntityMetadata<?> meta = new EntityMetadata(family);
        
        String familyName = annotation.name();
        ColumnFamilyDefinition existing = null;
        
        for(ColumnFamilyDefinition cfdef : keyspaceDef.getCfDefs())
        {
            if(cfdef.getName().equals(familyName))
            {
                _logger.debug("Column Family {} already exists", familyName);
                existing = cfdef;
                break;
            }
        }
        
        if(existing == null)
        {
            _logger.info("Column Family {} missing, creating...", familyName);

            ColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition(HFactory.createColumnFamilyDefinition(_keyspace, annotation.name()));
            if(meta.useCompositeColumns())
            {
                _logger.info("{}: comparator type: dynamic composite", familyName);
                cfDef.setComparatorType(ComparatorType.DYNAMICCOMPOSITETYPE);
                cfDef.setComparatorTypeAlias(DynamicComposite.DEFAULT_DYNAMIC_COMPOSITE_ALIASES);
            }
            else
            {
                _logger.info("{}: comparator type: UTF8", familyName);
                cfDef.setComparatorType(ComparatorType.UTF8TYPE);
            }
            
            Set<String> hashIndexed = new HashSet<String>();
            Set<String> rangeIndexed = new HashSet<String>();
            for(IndexMetadata im : meta.getIndexes())
            {
                if(im.getType() == EIndexType.HASH)
                {
                    SimplePropertyMetadata pm = im.getIndexedProperties().get(0);
                    cfDef.addColumnDefinition(createColDef(meta, familyName, pm)); //must be exactly 1 prop
                    hashIndexed.add(pm.getPhysicalName());
                }
                else
                {
                    rangeIndexed.add(im.id());
                }
            }
            
            syncRangeIndexTables(meta, !rangeIndexed.isEmpty(), keyspaceDef);
            
            cfDef.setCompressionOptions(compressionOptions(annotation));
            _logger.info("{}: compression options: {}, hash indexed columns: {}, range indexed columns", 
                         new Object[] {familyName, cfDef.getCompressionOptions(), hashIndexed, rangeIndexed});
            
                
            _cluster.addColumnFamily(cfDef, true);
        }
        else 
        {
            existing = new BasicColumnFamilyDefinition(existing);
            Map<String, String> compressionOptions = existing.getCompressionOptions();
            boolean doUpdate = false;
            boolean hasRangeIndexes = false;
            
            Set<SimplePropertyMetadata> hashIndexedProps = new HashSet<SimplePropertyMetadata>();
            for(IndexMetadata im : meta.getIndexes())
            {
                if(im.getType() == EIndexType.HASH)
                    hashIndexedProps.add(im.getIndexedProperties().get(0)); //must be exactly 1
                else
                    hasRangeIndexes = true;
            }
            
            //check if existing column metadata is in sync
            for(ColumnDefinition colMeta : existing.getColumnMetadata())
            {
                String colName;
                if(meta.useCompositeColumns())
                {
                    DynamicComposite col = DynamicComposite.fromByteBuffer(colMeta.getName());
                    Object prop1 = col.get(0);
                    if(prop1 instanceof String)
                        colName = (String) prop1;
                    else
                        colName = null;
                }
                else
                    colName = StringSerializer.get().fromByteBuffer(colMeta.getName());
                
                PropertyMetadataBase pm = meta.getPropertyByPhysicalName(colName);
                if(pm != null)
                {
                    boolean isHashIndexed = hashIndexedProps.remove(pm);
                    
                    if(colMeta.getIndexType() != null && !isHashIndexed)
                        _logger.warn("{}.{} is indexed in cassandra, but not in the data model. manual intervention needed", 
                                     familyName, pm.getPhysicalName());
                    
                    if(colMeta.getIndexType() == null && isHashIndexed)
                        throw new IllegalStateException(familyName + "." + pm.getPhysicalName() + 
                                " is not indexed in cassandra, manually add the index and then restart");
                }
                else
                {
                    _logger.warn("encountered unmapped column {}.{}", familyName, colMeta.getName());
                }
            }

            syncRangeIndexTables(meta, hasRangeIndexes, keyspaceDef);
            
            for(SimplePropertyMetadata pm : hashIndexedProps)
            {
                existing.addColumnDefinition(createColDef(meta, familyName, pm));
                
                _logger.info("adding index on {}.{}", familyName, pm.getPhysicalName()); 
                doUpdate = true;
            }
            
            if(!annotation.compressed())
            {
                //if don't want to compress but family is compressed, disable compression
                if(compressionOptions != null && !compressionOptions.isEmpty())
                {
                    doUpdate = true;
                    existing.setCompressionOptions(null);
                }
            }
            else //compression requested, check that options are in sync
            {
                Map<String, String> newOpts = compressionOptions(annotation);
                
                if(!newOpts.equals(compressionOptions))
                {
                    doUpdate = true;
                    existing.setCompressionOptions(newOpts);
                }
            }
            
            if(doUpdate)
            {
                _logger.info("Updating compression options for family {}: {} ", familyName, existing.getCompressionOptions());
                _cluster.updateColumnFamily(existing, true);
            }
        }
    }

    private void syncRangeIndexTables(EntityMetadata<?> meta, boolean hasRangeIndexes, KeyspaceDefinition keyspaceDef)
    {
        boolean walExists = false, idxExists = false, revIdxExists = false;
        for(ColumnFamilyDefinition existing : keyspaceDef.getCfDefs())
        {
            if(existing.getName().equals(meta.getWalFamilyName()))
                walExists = true;
            else if(existing.getName().equals(meta.getIndexFamilyName()))
                idxExists = true;
            
            if(walExists && revIdxExists && idxExists)
                break;
        }

        
        if(!hasRangeIndexes)
        {
            if(walExists)
                _logger.warn("{}: does not have range indexes but 'write ahead log' table {} exists. manual drop may be safely done.", 
                             meta.getFamilyName(), meta.getWalFamilyName());
            if(idxExists)
                _logger.warn("{}: does not have range indexes but 'index' table {} exists. manual drop may be safely done.", 
                             meta.getFamilyName(), meta.getIndexFamilyName());
        }
        
        //assume if the table exists, it is created correctly

        if(hasRangeIndexes)
        {
            if(!walExists)
            {
                ColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition(HFactory.createColumnFamilyDefinition(_keyspace, meta.getWalFamilyName()));
                _logger.info("{}: has range indexes - create 'write ahead log' table {}", meta.getFamilyName(), meta.getWalFamilyName());
                cfDef.setComparatorType(ComparatorType.UTF8TYPE);
                addCompressionOptions(cfDef);
                _cluster.addColumnFamily(cfDef, true);
            }
            if(!idxExists)
            {
                ColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition(HFactory.createColumnFamilyDefinition(_keyspace, meta.getIndexFamilyName()));
                _logger.info("{}: has range indexes - create 'index' table {}", meta.getFamilyName(), meta.getIndexFamilyName());
                cfDef.setComparatorType(ComparatorType.DYNAMICCOMPOSITETYPE);
                cfDef.setComparatorTypeAlias(DynamicComposite.DEFAULT_DYNAMIC_COMPOSITE_ALIASES);
                addCompressionOptions(cfDef);
                _cluster.addColumnFamily(cfDef, true);
            }
        }
    }
    
    private void addCompressionOptions(ColumnFamilyDefinition def)
    {
        Map<String, String> opts = new HashMap<String, String>();

        opts.put(CompressionParameters.SSTABLE_COMPRESSION, SnappyCompressor.class.getName());
        opts.put(CompressionParameters.CHUNK_LENGTH_KB, "64");

        def.setCompressionOptions(opts);
    }
    
    private BasicColumnDefinition createColDef(EntityMetadata<?> meta, String familyName, SimplePropertyMetadata pm)
    {
        BasicColumnDefinition colDef = new BasicColumnDefinition();
        colDef.setIndexName(String.format("%s_%s", familyName, pm.getPhysicalName()));
        colDef.setIndexType(ColumnIndexType.KEYS);
        colDef.setName(ByteBuffer.wrap(pm.getPhysicalNameBytes()));
        colDef.setValidationClass(BytesType.class.getName()); //skip validation
        return colDef;
    }

    private Map<String, String> compressionOptions(ColumnFamily annotation)
    {
        if(annotation.compressed())
        {
            if(annotation.compressionAlgo() == null || annotation.compressionChunkLength() <= 0)
                throw new IllegalArgumentException("invalid compression settings for " + annotation.name());
            
            Map<String, String> newOpts = new HashMap<String, String>();
            
            String compressionAlgo = annotation.compressionAlgo();
            if(compressionAlgo.equals("DeflateCompressor"))
                newOpts.put(CompressionParameters.SSTABLE_COMPRESSION, DeflateCompressor.class.getName());
            else if(compressionAlgo.equals("SnappyCompressor"))
                newOpts.put(CompressionParameters.SSTABLE_COMPRESSION, SnappyCompressor.class.getName());
            
            newOpts.put(CompressionParameters.CHUNK_LENGTH_KB, String.valueOf(annotation.compressionChunkLength()));
            return newOpts;
        }

        //in 1.1 do this instead of returning null
        //            return Collections.singletonMap(CompressionParameters.SSTABLE_COMPRESSION, "");
        
        return null;
    }

}
