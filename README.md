# Feedly-Cassandra

## A compact ORM library for cassandra

Feedly Cassandra is an unimaginatively named object mapping library for cassandra. Some of its features are:

* Familiar, Clean API:
    * Inspired by the hibernate DAO/query by example pattern.
    * Minimal configuration to get started.
    * Annotation based configuration.
* Feature rich
    * Native and custom secondary indexing.
    * Support for nested objects.
    * Support for collections.
    * Counter and TTL support.
    * Automatic schema generation.
    * JMX enabled.
* Open Source under Apache 2 License.

## Getting Started

Getting started is a snap!

### Define Entity classes.

Entity classes map to data that will be stored in cassandra. Here's a simple example:

    @ColumnFamily(name="simple_fam")
    public class SimpleEntity
    {
      @RowKey
      private String myKey;
    
      @Column(name="s")
      private String aString;

      @Column(name="i")
      private int anInt;

      //getters and setters, maybe a nice toString() method if we're in a good mood, etc., etc.
    }

This class maps to a column family in cassandra named "simple_fam". The entryId will be a property and the family will contain a single 
column (physically named "u" in cassandra, entryUrl in java). That's it! For examples of more complicated things like collections, multi
column indexes, ttl support and beyond, check out the unit tests, particularly the @com.feedly.cassandra.entity package@.

### "Enhance" Entity classes.

The deep dark nasty bit of the library is that the ORM bean classes are bytecode manipulated. On the upside, the manipulated classes are 
just your bean classes full of vanilla getters and setters. Plus there is a nice ant task you can put in your build and forget I ever 
mentioned anything.

First compile the code as normal. then run an ant task as follows:

    <taskdef name="enhance" classname="com.feedly.cassandra.entity.enhance.EntityTransformerTask">
      <classpath>
        <path refid="your.classpath" /> <!-- contains library jar and dependencies -->
      </classpath>
    </taskdef>

    <enhance>
      <fileset dir="${entity.class.files.dir}" />
    </enhance>

The above will manipulate entity classes as necessary for the library to work. Best practice is to isolate entity classes into a single
package and then specify that directory in the enhance task's fileset. Check out the pom.xml to see how the unit tests work this step into
the eclipse build and maven life cycle. Now on to more pleasant matters.

### Accessing Data

To access data, define a DAO per entity, as such:

    public class SimpleEntityDao extends CassandraDaoBase<String, SimpleEntity>
    {
      public SimpleEntityDao()
      {
        super(String.class, SimpleEntity.class, EConsistencyLevel.QUOROM);
      }
    }

That's actually the entire class. Generally this is sufficient - the dao base class provides all the necessary methods, but in theory one
could add methods to implement some application specific logic related to the entity. Here's some code to access data:

    SimpleEntityDao dao = ...//construction, initialization, etc.
    SimpleEntity e = dao.get("myKey");

See `CassandraDaoBaseTest` for more involved operations, like secondary index lookups, range finds, partial object retrievals, partial
collection retrievals, and more. As another quick example, if we had a range index defined on anInt, here's how to do a range find for all
entities with an anInt value between 0 and 2 (inclusive):

    SimpleEntity start = new SimpleEntity(), end = new SimpleEntity();
    start.setAnInt(0);
    end.setAnInt(2);
    Collection<SimpleEntity> entities = dao.mfindBetween(start, end);

The returned collection utilizes lazy loading to allow iteration over large collections.

## Initialization/Configuration

At this point, you may get the feeling that a bunch of details have been skipped. Here is where things start to tie together. The library
uses hector under the hood. All the hector configuration goes into the `PersistenceManager` class. DAO classes are initialized with a
reference to the persistence manager. This is how data gets from java to the appropriate cassandra cluster.

Another detail is how to define a compatible schema for the library to work. First you need to understand how the library works then then
write a CQL script making sure to...wait never mind. Just call `PersistenceManager.init()`.

### Indexing

The library supports native indexing as well as custom secondary indexes. Generally native indexes are preferred, custom ones can be defined
to support range finds and multi-column indexes. Custom indexes employ a filter/cleanup on read strategy. This can be done inline or offline
in a separate thread. If we wanted to index the anInt property, we would change it's annotation to `Column(hashIndexed=true)` or
`Column(rangeIndexed=true)`. The `InlineRepairStrategy` or `OfflineRepairStrategy` can then be configured to cleanup the index.

## More stuff

There are many more points that could be mentioned here, but I am not going to discuss them until I am certain someone besides me will ever
read this document.

## Caveats
This library is being used at Feedly, though on a small cluster with non-critical data. The tests cases are quite thorough and so far things
are going well. But buyer beware.

## License

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2012 DevHD, Inc <http://www.feedly.com/about.html>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
