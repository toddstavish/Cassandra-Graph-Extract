package sh.stavi.blog;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ConsistencyLevel;

public class People extends Base
{
    public static void main(String[] args)
	{
        Cassandra.Client client = setupConnection();

        System.out.println("Remove all the people we might have created before.\n");
        removePerson(client, "John Smith");
        removePerson(client, "Steve Williams");
        removePerson(client, "Frank Chen");

        System.out.println("Create the people.\n");
        createPerson(client, "John Smith", "john (at) smith.com", "United Kingdom", "01/01/2002");
        createPerson(client, "Steve Williams", "steve (at) williams.com", "United States", "01/01/2010");
        createPerson(client, "Frank Chen", "frank (at) somedomain.com", "Australia", "01/01/2009");

        System.out.println("Select John Smith.\n");
        selectSinglePersonWithAllColumns(client, "John Smith");
        
        closeConnection();
    }

    private static void removePerson(Cassandra.Client client, String personKey)
	{
        try
		{
            ColumnPath columnPath = new ColumnPath(COLUMN_FAMILY_PEOPLE);
            client.remove(KEYSPACE, personKey, columnPath, System.currentTimeMillis(), ConsistencyLevel.ALL);
        }
		catch (Exception exception)
		{
            exception.printStackTrace();
        }
    }

    private static void createPerson(Cassandra.Client client, String personKey, String email, String country, String registeredSince)
	{
        try
		{
            long timestamp = System.currentTimeMillis();
            Map<String, List<ColumnOrSuperColumn>> job = new HashMap<String, List<ColumnOrSuperColumn>>();

            List<ColumnOrSuperColumn> columns = new ArrayList<ColumnOrSuperColumn>();
            Column column = new Column("email".getBytes(ENCODING), email.getBytes(ENCODING), timestamp);
            ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
            columnOrSuperColumn.setColumn(column);
            columns.add(columnOrSuperColumn);

            column = new Column("country".getBytes(ENCODING), country.getBytes(ENCODING), timestamp);
            columnOrSuperColumn = new ColumnOrSuperColumn();
            columnOrSuperColumn.setColumn(column);
            columns.add(columnOrSuperColumn);

            column = new Column("country".getBytes(ENCODING), country.getBytes(ENCODING), timestamp);
            columnOrSuperColumn = new ColumnOrSuperColumn();
            columnOrSuperColumn.setColumn(column);
            columns.add(columnOrSuperColumn);

            column = new Column("registeredSince".getBytes(ENCODING), registeredSince.getBytes(ENCODING), timestamp);
            columnOrSuperColumn = new ColumnOrSuperColumn();
            columnOrSuperColumn.setColumn(column);
            columns.add(columnOrSuperColumn);

            job.put(COLUMN_FAMILY_PEOPLE, columns);

            client.batch_insert(KEYSPACE, personKey, job, ConsistencyLevel.ALL);
        }
		catch (Exception exception)
		{
            exception.printStackTrace();
        }
    }

    private static void selectSinglePersonWithAllColumns(Cassandra.Client client, String personKey)
	{
        try
		{
            SlicePredicate slicePredicate = new SlicePredicate();
            SliceRange sliceRange = new SliceRange();
            sliceRange.setStart(new byte[] {});
            sliceRange.setFinish(new byte[] {});
            slicePredicate.setSlice_range(sliceRange);

            ColumnParent columnParent = new ColumnParent(COLUMN_FAMILY_PEOPLE);
            List<ColumnOrSuperColumn> result = client.get_slice(KEYSPACE, personKey, columnParent, slicePredicate, ConsistencyLevel.ONE);

            printToConsole(personKey, result);
        }
		catch (Exception exception)
		{
            exception.printStackTrace();
        }
    }
}
