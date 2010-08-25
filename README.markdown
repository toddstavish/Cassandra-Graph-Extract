Extracting Graphs From Cassandra To InfinteGraph Database
=========================================================

This code base demonstrates processing graphs in InfiniteGraph (graph database) extracted from the Cassandra NoSQL data-store. The datamodel is extracting Friend-of-a-friend (FOAF) relationships.

More information on this project can be found here:

http://blog.stavi.sh/polyglot-persistence-integrating-low-latency

Dependencies:
-------------

Cassandra 0.6.3
InfiniteGraph 0.8
Apache Maven 2.2
Java version: 1.5

Installation:
-------------

1. Download and install the [InfinteGraph Datbase](http://www.infinitegraph.com/)
2. Download and extract [Cassandra](http://cassandra.apache.org/)
3. Start InfiniteGraph lockserver
4. Run a single node instance of Cassandra <installdir>/bin/cassandra -f 
5. Configure Cassandra (see below)

Configuration:
--------------

A working version of the storage-conf.xml is included in the github repository (src/main/resources/storage-conf.xml). The included configuration ontains reasonable defaults for single node operation. Cassandra is meant to run on a cluster of nodes, but will run equally wel on a single machine. This is a hand way of getting familiar with the software while aoiding the complexities of a larger system. If you are running a single node instance for development with no other keyspaces you can just replease the default storage-conf.xml, alternatively you can add in the keyspaces into your existing configuraiton if you are running a multi-node setup.

To add the keyspaceas, open storage-conf.xml in an editor and add keyspace for FOAFi (see below). The only Cassandra configuration step necessary for this example is the definition of the keyspaces. Keyspaces, using an RDBMS point of view, can be compared to schema, normally you have one per application. A keyspace contains the ColumnFamilies. Note, there is no relationship between the ColumnFamiliies; they are separate containers. The FOAF graph extract sample users three column families: People, Friends, Friend. 

<Keyspace Name="FOAF">
<ColumnFamily CompareWith="UTF8Type" Name="People"/>
<ColumnFamily ColumnType="Super" CompareWith="UTF8Type" CompareSubcolumnsWith="UTF8Type" Name="Friends"/>
<ColumnFamily CompareWith="UTF8Type" Name="Friend"/>
</Keyspace>

InfiteGraph's configurations are also located in the resources directory (src/main/resources/FriendsGraphProperties.properties). They are loaded dynamically and placed into the running applications directory by Maven. No changes need to be made with this configuration.

