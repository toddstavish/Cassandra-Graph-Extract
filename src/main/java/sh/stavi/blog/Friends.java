package sh.stavi.blog;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;


import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SlicePredicate;

public class Friends extends Base
{

    public static void main(String[] args)
	{
        Cassandra.Client client = setupConnection();

        printStatusLine("Remove all the friends we might have created before.");
        removeFriends(client, "friends");
        
        printStatusLine("Create friends of Tom Riddle.");
        createFriends(client, "friends", "Tom Riddle", "friends", "meetings: Borgin and Burkes, Leaky Cauldron", "01/02/2010", "Bellatrix Lestrange", "Nancy Smith");
 
        printStatusLine("Select friends");
        selectSingleFriendsWithAllColumns(client, "friends");
        
        closeConnection();
    }
    
    public static void removeFriends(Cassandra.Client client, String friendsKey) {
        try
		{
			// First retrieve the friends so we know which friends to clean up...
            SlicePredicate slicePredicate = new SlicePredicate();
            SliceRange sliceRange = new SliceRange();
            sliceRange.setStart(new byte[] {});
            sliceRange.setFinish(new byte[] {});
            slicePredicate.setSlice_range(sliceRange);
            
            ColumnParent columnParent = new ColumnParent(COLUMN_FAMILY_FRIENDS);
            columnParent.setSuper_column("friend".getBytes(ENCODING));
            List<ColumnOrSuperColumn> result = client.get_slice(KEYSPACE, friendsKey, columnParent, slicePredicate, ConsistencyLevel.ONE);
            
            for (ColumnOrSuperColumn columnOrSuperColumn : result)
			{
                Column column = columnOrSuperColumn.getColumn();
                Friend.removeFriendsFromExistingFriend(client, new String(column.getValue(), ENCODING), friendsKey);
            }

            ColumnPath columnPath = new ColumnPath(COLUMN_FAMILY_FRIENDS);
            client.remove(KEYSPACE, friendsKey, columnPath, System.currentTimeMillis(), ConsistencyLevel.ALL);
        }
		catch (Exception exception)
		{
            exception.printStackTrace();
        }
    }
    
    public static void createFriends(Cassandra.Client client, String friendsKey, String person, String groupName, String description, String created, String ... friends)
	{
        try
		{
			long timestamp = System.currentTimeMillis();
			
			List<ColumnOrSuperColumn> friendsSuperColumns = new ArrayList<ColumnOrSuperColumn>();
			
			// Build up the friends SuperColumn
			List<Column> columns = new ArrayList<Column>();
			columns.add(new Column("groupName".getBytes(ENCODING), groupName.getBytes(ENCODING), timestamp));
			columns.add(new Column("description".getBytes(ENCODING), description.getBytes(ENCODING), timestamp));
			columns.add(new Column("person".getBytes(ENCODING), person.getBytes(ENCODING), timestamp));
			columns.add(new Column("created".getBytes(ENCODING), created.getBytes(ENCODING), timestamp));
			
			SuperColumn superColumn = new SuperColumn("friends".getBytes(ENCODING), columns);
			ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
			columnOrSuperColumn.setSuper_column(superColumn);
			friendsSuperColumns.add(columnOrSuperColumn);

			// Build up the friend SuperColumn
			columns = new ArrayList<Column>();
			for (int index = 0; index < friends.length; index++)
			{
			    columns.add(new Column(("" + index).getBytes(ENCODING), friends[index].getBytes(ENCODING), timestamp));
			}
			superColumn = new SuperColumn("friend".getBytes(ENCODING), columns);
			columnOrSuperColumn = new ColumnOrSuperColumn();
			columnOrSuperColumn.setSuper_column(superColumn);
			friendsSuperColumns.add(columnOrSuperColumn);
						
			Map<String, List<ColumnOrSuperColumn>> job = new HashMap<String, List<ColumnOrSuperColumn>>();
			job.put(COLUMN_FAMILY_FRIENDS, friendsSuperColumns);
			
			client.batch_insert(KEYSPACE, friendsKey, job, ConsistencyLevel.ALL);
            
        } 
		catch (Exception exception)
		{
            exception.printStackTrace();
        }
    }
    
    public static void selectSingleFriendsWithAllColumns(Cassandra.Client client, String friendsKey)
	{
        try
		{
            SlicePredicate slicePredicate = new SlicePredicate();
            SliceRange sliceRange = new SliceRange();
            sliceRange.setStart(new byte[] {});
            sliceRange.setFinish(new byte[] {});
            slicePredicate.setSlice_range(sliceRange);

            ColumnParent columnParent = new ColumnParent(COLUMN_FAMILY_FRIENDS);
            List<ColumnOrSuperColumn> result = client.get_slice(KEYSPACE, friendsKey, columnParent, slicePredicate, ConsistencyLevel.ONE);

            printToConsole(friendsKey, result);
        }
		catch (Exception exception)
		{
            exception.printStackTrace();
        }
    }

    public static List<String> findFriends(Cassandra.Client client, String friendsKey, String ... values)
	{
		ArrayList<String> friends = new ArrayList<String>();
        try
		{
            SlicePredicate slicePredicate = new SlicePredicate();
            SliceRange sliceRange = new SliceRange();
            sliceRange.setStart(new byte[] {});
            sliceRange.setFinish(new byte[] {});
            slicePredicate.setSlice_range(sliceRange);

            ColumnParent columnParent = new ColumnParent(COLUMN_FAMILY_FRIENDS);
            List<ColumnOrSuperColumn> result = client.get_slice(KEYSPACE, friendsKey, columnParent, slicePredicate, ConsistencyLevel.ONE);

            for (ColumnOrSuperColumn c : result)
			{
                if (c.getColumn() != null)
				{
                    String name = new String(c.getColumn().getName(), ENCODING);
                    String value = new String(c.getColumn().getValue(), ENCODING);
                    long timestamp = c.getColumn().getTimestamp();
                }
				else if (c.getSuper_column() != null)
				{
                    SuperColumn superColumn = c.getSuper_column();
                    for (Column column : superColumn.getColumns())
					{
                        String name = new String(column.getName(), ENCODING);
                        String value = new String(column.getValue(), ENCODING);
                        long timestamp = column.getTimestamp();
						for (int index = 0; index < values.length; index++)
						{
							if (value.equals(values[index]))
							{
								System.out.println("Hit: " + values[index] + "', value: '" + value);
								friends.add(value);
							}
						}
                    }

                }
            }
        }
		catch (Exception exception)
		{
            exception.printStackTrace();
        }
		return friends;
    }
	
}
