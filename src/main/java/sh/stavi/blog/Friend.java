package sh.stavi.blog;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SlicePredicate;

public class Friend extends Base
{

    public static boolean checkIfFriendExists(Cassandra.Client client, String friendKey)
	{
        try
		{
            ColumnParent columnParent = new ColumnParent("Friend");
            int count = client.get_count(KEYSPACE, friendKey, columnParent, ConsistencyLevel.ONE);

            return count > 0;
        }
		catch (Exception exception)
		{
            exception.printStackTrace();
        }

        return false;
    }
    
    public static void removeFriend(Cassandra.Client client, String friendKey)
	{
        try
		{
            ColumnPath columnPath = new ColumnPath(COLUMN_FAMILY_FRIEND);
            client.remove(KEYSPACE, friendKey, columnPath, System.currentTimeMillis(), ConsistencyLevel.ALL);
        }
		catch (Exception exception)
		{
            exception.printStackTrace();
        }
    }
    
    public static void createFriend(Cassandra.Client client, String friendKey, String friendsKey, String groupName) {
        try
		{
            long timestamp = System.currentTimeMillis();
            Map<String, List<ColumnOrSuperColumn>> job = new HashMap<String, List<ColumnOrSuperColumn>>();

            List<ColumnOrSuperColumn> columns = new ArrayList<ColumnOrSuperColumn>();
            Column column = new Column(friendsKey.getBytes(ENCODING), groupName.getBytes(ENCODING), timestamp);
            ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
            columnOrSuperColumn.setColumn(column);
            columns.add(columnOrSuperColumn);

            job.put(COLUMN_FAMILY_FRIEND, columns);

            client.batch_insert(KEYSPACE, friendKey, job, ConsistencyLevel.ALL);
        }
		catch (Exception exception)
		{
            exception.printStackTrace();
        }
    }
    
    public static void addFriendsToExistingFriend(Cassandra.Client client, String friendKey, String friendsKey, String groupName)
	{
        try
		{
            long timestamp = System.currentTimeMillis();

            Map<String, Map<String, List<Mutation>>> job = new HashMap<String, Map<String, List<Mutation>>>();
            List<Mutation> mutations = new ArrayList<Mutation>();
            
            Column column = new Column(friendsKey.getBytes(ENCODING), groupName.getBytes(ENCODING), timestamp);
            ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
            columnOrSuperColumn.setColumn(column);

            Mutation mutation = new Mutation();
            mutation.setColumn_or_supercolumn(columnOrSuperColumn);
            mutations.add(mutation);

            Map<String, List<Mutation>> mutationsForColumnFamily = new HashMap<String, List<Mutation>>();
            mutationsForColumnFamily.put(COLUMN_FAMILY_FRIEND, mutations);

            job.put(friendKey, mutationsForColumnFamily);

            client.batch_mutate(KEYSPACE, job, ConsistencyLevel.ALL);
        }
		catch (Exception exception)
		{
            exception.printStackTrace();
        }
    }
    
    public static void removeFriendsFromExistingFriend(Cassandra.Client client, String friendKey, String friendsKey)
	{
        try
		{
            long timestamp = System.currentTimeMillis();

            Map<String, Map<String, List<Mutation>>> job = new HashMap<String, Map<String, List<Mutation>>>();
            List<Mutation> mutations = new ArrayList<Mutation>();
            
            SlicePredicate slicePredicate = new SlicePredicate();
            slicePredicate.addToColumn_names(friendsKey.getBytes(ENCODING));
            
            Deletion deletion = new Deletion(timestamp);
            deletion.setPredicate(slicePredicate);
            
            Mutation mutation = new Mutation();
            mutation.setDeletion(deletion);
            mutations.add(mutation);

            Map<String, List<Mutation>> mutationsForColumnFamily = new HashMap<String, List<Mutation>>();
            mutationsForColumnFamily.put(COLUMN_FAMILY_FRIEND, mutations);

            job.put(friendKey, mutationsForColumnFamily);

            client.batch_mutate(KEYSPACE, job, ConsistencyLevel.ALL);
        }
		catch (Exception exception)
		{
            exception.printStackTrace();
        }
    }
    
    public static void selectSingleFriendWithAllColumns(Cassandra.Client client, String friendKey)
	{
        try
		{
            SlicePredicate slicePredicate = new SlicePredicate();
            SliceRange sliceRange = new SliceRange();
            sliceRange.setStart(new byte[] {});
            sliceRange.setFinish(new byte[] {});
            slicePredicate.setSlice_range(sliceRange);

            ColumnParent columnParent = new ColumnParent(COLUMN_FAMILY_FRIEND);
            List<ColumnOrSuperColumn> result = client.get_slice(KEYSPACE, friendKey, columnParent, slicePredicate, ConsistencyLevel.ONE);

            printToConsole(friendKey, result);
        }
		catch (Exception exception)
		{
            exception.printStackTrace();
        }
    }
    
    public static void selectAllFriendWithAllColumns(Cassandra.Client client)
	{
        try {
            KeyRange keyRange = new KeyRange(100);
            keyRange.setStart_key("");
            keyRange.setEnd_key("");

            SliceRange sliceRange = new SliceRange();
            sliceRange.setStart(new byte[] {});
            sliceRange.setFinish(new byte[] {});

            SlicePredicate slicePredicate = new SlicePredicate();
            slicePredicate.setSlice_range(sliceRange);

            ColumnParent columnParent = new ColumnParent(COLUMN_FAMILY_FRIEND);
            List<KeySlice> keySlices = client.get_range_slices(KEYSPACE, columnParent, slicePredicate, keyRange, ConsistencyLevel.ONE);

            for (KeySlice keySlice : keySlices)
			{
                printToConsole(keySlice.getKey(), keySlice.getColumns());
            }

        }
		catch (Exception exception)
		{
            exception.printStackTrace();
        }
    }
}
