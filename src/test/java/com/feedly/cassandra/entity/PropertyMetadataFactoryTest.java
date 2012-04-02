package com.feedly.cassandra.entity;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import me.prettyprint.cassandra.serializers.BooleanSerializer;
import me.prettyprint.cassandra.serializers.CharSerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.FloatSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;

import org.junit.Test;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.anno.RowKey;

public class PropertyMetadataFactoryTest
{
    @Test
    public void testMetadata() throws SecurityException, NoSuchFieldException
    {
        for(Class<?> c : SampleEntity.class.getInterfaces())
            System.out.println(c.getName());
        
        int idx = 8;
        EntityMetadata<SampleEntity> meta = new EntityMetadata<SampleEntity>(SampleEntity.class);
        assertEquals(idx--, meta.getProperties().size());
        
        /*
         * strProp
         */
        SimplePropertyMetadata simpleMeta = (SimplePropertyMetadata) meta.getProperties().get(idx--);
        assertSimpleProperty(simpleMeta, "strProp", String.class, StringSerializer.get());

        /*
         * mapOfMaps
         */
        MapPropertyMetadata mapMeta = (MapPropertyMetadata) meta.getProperties().get(idx--);
        assertEquals("mapOfMaps", mapMeta.getName());
        assertEquals(Map.class, mapMeta.getFieldType());
        SimplePropertyMetadata keyMeta = mapMeta.getKeyPropertyMetadata();
        assertSimpleProperty(keyMeta, "", String.class, StringSerializer.get());
        
        mapMeta = (MapPropertyMetadata) mapMeta.getValuePropertyMetadata();
        
        keyMeta = mapMeta.getKeyPropertyMetadata();
        assertSimpleProperty(keyMeta, "", String.class, StringSerializer.get());
        
        SimplePropertyMetadata valueMeta = (SimplePropertyMetadata) mapMeta.getValuePropertyMetadata();
        assertSimpleProperty(valueMeta, "", Double.class, DoubleSerializer.get());

        /*
         * map
         */
        mapMeta = (MapPropertyMetadata) meta.getProperties().get(idx--);
        assertEquals("map", mapMeta.getName());
        assertEquals(Map.class, mapMeta.getFieldType());
        keyMeta = mapMeta.getKeyPropertyMetadata();
        assertSimpleProperty(keyMeta, "", String.class, StringSerializer.get());
        
        valueMeta = (SimplePropertyMetadata) mapMeta.getValuePropertyMetadata();
        assertSimpleProperty(valueMeta, "", Double.class, DoubleSerializer.get());
        
        /*
         * listOfLists
         */
        ListPropertyMetadata listMeta = (ListPropertyMetadata) meta.getProperties().get(idx--);
        assertEquals("listOfLists", listMeta.getName());
        assertEquals(List.class, listMeta.getFieldType());
        
        listMeta = (ListPropertyMetadata) listMeta.getElementPropertyMetadata();
        assertEquals("", listMeta.getName());
        assertEquals(List.class, listMeta.getFieldType());

        valueMeta = (SimplePropertyMetadata) listMeta.getElementPropertyMetadata();
        assertSimpleProperty(valueMeta, "", String.class, StringSerializer.get());
        
        /*
         * list
         */
        listMeta = (ListPropertyMetadata) meta.getProperties().get(idx--);
        assertEquals("list", listMeta.getName());
        assertEquals(List.class, listMeta.getFieldType());
        
        valueMeta = (SimplePropertyMetadata) listMeta.getElementPropertyMetadata();
        assertSimpleProperty(valueMeta, "", String.class, StringSerializer.get());
        
        /*
         * embedded
         */
        ObjectPropertyMetadata objMeta = (ObjectPropertyMetadata) meta.getProperties().get(idx--);
        assertEmbeddedMeta("embedded", objMeta);
        
        /*
         * beanMap
         */
        mapMeta = (MapPropertyMetadata) meta.getProperties().get(idx--);
        assertEquals("beanMap", mapMeta.getName());
        assertEquals(SortedMap.class, mapMeta.getFieldType());
        keyMeta = mapMeta.getKeyPropertyMetadata();
        assertSimpleProperty(keyMeta, "", String.class, StringSerializer.get());
        
        ObjectPropertyMetadata beanValueMeta = (ObjectPropertyMetadata) mapMeta.getValuePropertyMetadata();
        assertEmbeddedMeta("", beanValueMeta);
        
        /*
         * beanList
         */
        listMeta = (ListPropertyMetadata) meta.getProperties().get(idx--);
        assertEquals("beanList", listMeta.getName());
        assertEquals(List.class, listMeta.getFieldType());
        
        beanValueMeta = (ObjectPropertyMetadata) listMeta.getElementPropertyMetadata();
        assertEmbeddedMeta("", beanValueMeta);
    }
    

    private void assertEmbeddedMeta(String name, ObjectPropertyMetadata meta)
    {
        assertEquals(name, meta.getName());
        assertEquals(EmbeddedBean.class, meta.getFieldType());

        /*
         * boolVal
         * charVal
         * dateVal
         * doubleVal
         * floatVal
         * intVal
         * longVal
         * strVal
         */
        List<PropertyMetadataBase> properties = meta.getObjectMetadata().getProperties();
        assertEquals(8, properties.size());
        assertSimpleProperty(properties.get(0), "boolVal",   boolean.class, BooleanSerializer.get());
        assertSimpleProperty(properties.get(1), "charVal",   char.class,    CharSerializer.get());
        assertSimpleProperty(properties.get(2), "dateVal",   Date.class,    DateSerializer.get());
        assertSimpleProperty(properties.get(3), "doubleVal", double.class,  DoubleSerializer.get());
        assertSimpleProperty(properties.get(4), "floatVal",  float.class,   FloatSerializer.get());
        assertSimpleProperty(properties.get(5), "intVal",    int.class,     IntegerSerializer.get());
        assertSimpleProperty(properties.get(6), "longVal",   long.class,    LongSerializer.get());
        assertSimpleProperty(properties.get(7), "strVal",    String.class,  StringSerializer.get());
    }


    private void assertSimpleProperty(PropertyMetadataBase pm,
                                      String name,
                                      Class<?> type,
                                      Serializer<?> serializer)
    {
        SimplePropertyMetadata spm = (SimplePropertyMetadata) pm;
        
        assertEquals(name, spm.getName());
        assertEquals(type, spm.getFieldType());
        assertEquals(serializer, spm.getSerializer());
    }


    @ColumnFamily
    public class SampleEntity
    {
        @RowKey
        private long rowKey;
        
        @Column
        private EmbeddedBean embedded;
        
        @Column
        private String strProp;

        @Column
        private List<String> list;

        @Column
        private List<List<String>> listOfLists;
        
        @Column
        private List<EmbeddedBean> beanList; 

        @Column
        private Map<String, Double> map; 

        @Column
        private Map<String, Map<String, Double>> mapOfMaps; 
        
        @Column
        private SortedMap<String, EmbeddedBean> beanMap;

        public long getRowKey()
        {
            return rowKey;
        }

        public void setRowKey(long rowKey)
        {
            this.rowKey = rowKey;
        }

        public EmbeddedBean getEmbedded()
        {
            return embedded;
        }

        public void setEmbedded(EmbeddedBean embedded)
        {
            this.embedded = embedded;
        }

        public String getStrProp()
        {
            return strProp;
        }

        public void setStrProp(String strProp)
        {
            this.strProp = strProp;
        }

        public List<String> getList()
        {
            return list;
        }

        public void setList(List<String> list)
        {
            this.list = list;
        }

        public List<EmbeddedBean> getBeanList()
        {
            return beanList;
        }

        public void setBeanList(List<EmbeddedBean> beanList)
        {
            this.beanList = beanList;
        }

        public Map<String, Double> getMap()
        {
            return map;
        }

        public void setMap(Map<String, Double> map)
        {
            this.map = map;
        }

        public SortedMap<String, EmbeddedBean> getBeanMap()
        {
            return beanMap;
        }

        public void setBeanMap(SortedMap<String, EmbeddedBean> beanMap)
        {
            this.beanMap = beanMap;
        }

        public List<List<String>> getListOfLists()
        {
            return listOfLists;
        }

        public void setListOfLists(List<List<String>> listOfLists)
        {
            this.listOfLists = listOfLists;
        }

        public Map<String, Map<String, Double>> getMapOfMaps()
        {
            return mapOfMaps;
        }

        public void setMapOfMaps(Map<String, Map<String, Double>> mapOfMaps)
        {
            this.mapOfMaps = mapOfMaps;
        }

    }
}
