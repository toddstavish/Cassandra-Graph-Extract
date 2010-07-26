package sh.stavi.blog;

import java.util.List;
import java.io.UnsupportedEncodingException;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;

public class Base
{

    protected static final String KEYSPACE = "FOAF";
    protected static final String COLUMN_FAMILY_PEOPLE = "People";
    protected static final String COLUMN_FAMILY_FRIENDS = "Friends";
    protected static final String COLUMN_FAMILY_FRIEND = "Friend";
    
    protected static final String ENCODING = "utf-8";
    
    protected static TTransport tr = null;
 
    protected static Cassandra.Client setupConnection()
	{
        try
		{
            tr = new TSocket("localhost", 9160);
            TProtocol proto = new TBinaryProtocol(tr);
            Cassandra.Client client = new Cassandra.Client(proto);
            tr.open();

            return client;
        }
		catch (TTransportException exception)
		{
            exception.printStackTrace();
        }

        return null;
    }

    protected static void closeConnection()
	{
        try
		{
            tr.flush();
            tr.close();
        }
		catch (TTransportException exception)
		{
            exception.printStackTrace();
        }
    }

    protected static void printStatusLine(String line)
	{
    		System.out.println("\n====================================================");
    		System.out.println(line);
    		System.out.println("====================================================");
    }
   
    protected static void printToConsole(String key, List<ColumnOrSuperColumn> result)
	{
        try
		{
            System.out.println("Key: '" + key + "'");
            for (ColumnOrSuperColumn c : result)
			{
                if (c.getColumn() != null)
				{
                    String name = new String(c.getColumn().getName(), ENCODING);
                    String value = new String(c.getColumn().getValue(), ENCODING);
                    long timestamp = c.getColumn().getTimestamp();
                    System.out.println("  name: '" + name + "', value: '" + value + "', timestamp: " + timestamp);
                }
				else if (c.getSuper_column() != null)
				{
                    SuperColumn superColumn = c.getSuper_column();
                    
                    System.out.println("    Supercolumn: " + new String(superColumn.getName(), ENCODING));
                    for (Column column : superColumn.getColumns())
					{
                        String name = new String(column.getName(), ENCODING);
                        String value = new String(column.getValue(), ENCODING);
                        long timestamp = column.getTimestamp();
                        System.out.println("        name: '" + name + "', value: '" + value + "', timestamp: " + timestamp);
                    }
                    
                }
            }
        }
		catch (UnsupportedEncodingException exception)
		{
            exception.printStackTrace();
        }
    }
}
