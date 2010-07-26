package sh.stavi.blog;

import java.util.List;
import java.util.ArrayList;

import com.objy.graph.BaseEdge;
import com.objy.graph.EdgeKind;
import com.objy.graph.AccessMode;
import com.objy.graph.BaseVertex;
import com.objy.graph.Transaction;
import com.objy.graph.GraphFactory;
import com.objy.graph.GraphDatabase;
import com.objy.graph.ConfigurationException;
import com.objy.graph.navigation.Hop;
import com.objy.graph.navigation.Path;
import com.objy.graph.navigation.Guide;
import com.objy.graph.navigation.Navigator;
import com.objy.graph.navigation.Qualifier;
import com.objy.graph.navigation.NavigationResultHandler;

import org.apache.cassandra.thrift.Cassandra;

public class FriendsGraphExtract
{

    public static void main(String[] args)
    { 
    	Transaction tx = null;
    	GraphDatabase myGraphDB = null;
        
        try
        {
			
			// Create friend network in Cassandra
			Cassandra.Client client = Friends.setupConnection();
	        Friends.printStatusLine("Remove all the friends we might have created before.");
	        Friends.removeFriends(client, "friends");
	        Friends.printStatusLine("Create friends of Tom Riddle.");
	        Friends.createFriends(client, "friends", "Tom Riddle", "friends", "meetings: Borgin and Burkes, Leaky Cauldron", "01/02/2010", "Bellatrix Lestrange", "Nancy Smith");
	        Friends.printStatusLine("Select friends");
	        Friends.selectSingleFriendsWithAllColumns(client, "friends");
			List<String> friends = Friends.findFriends(client, "friends", "Tom Riddle", "Bellatrix Lestrange", "Nancy Smith");
	        Friends.closeConnection();
	
        	// Create a Graph Database
        	GraphFactory.create("FriendsGraph", "FriendsGraphProperties.properties");
            myGraphDB = GraphFactory.open("FriendsGraph", "FriendsGraphProperties.properties");

            // Start a transaction
            tx = myGraphDB.beginTransaction(AccessMode.READ_WRITE);
            
            // Create the three people as vertices and "meeting" edges
            Person friend1 = new Person(friends.get(2));
            Person friend2 = new Person(friends.get(0));
            Person unknown1 = new Person(friends.get(1)); 
            
            MetWith meeting1 = new MetWith("Borgin and Burkes");
            MetWith meeting2 = new MetWith("Leaky Cauldron");
       
            // Add three vertices to graph
            myGraphDB.addVertex(friend1);
            myGraphDB.addVertex(unknown1);
            myGraphDB.addVertex(friend2);

            // Connects the 3 vertices:
            // friend1 <-meeting1-> unknown1 <-meeting2-> friend2
            friend1.addEdge(meeting1, unknown1, EdgeKind.BIDIRECTIONAL);
            unknown1.addEdge(meeting2, friend2, EdgeKind.BIDIRECTIONAL);
            
            // Name the Vertices for search capability
            myGraphDB.nameVertex("friend1", friend1);
            myGraphDB.nameVertex("friend2", friend2);

            // Get the vertex by the name given
            Person retrievedFriend1 = (Person)myGraphDB.getNamedVertex("friend1");
            
            // See if we retrieved the right person.
            if (retrievedFriend1.getName().equals(friend1.getName()))
            {
            	System.out.println("Found " + retrievedFriend1.getName()); 
            }
            
            // Commit the graph additions, downgrading to read
            tx.checkpoint(true);

            //Instantiate the qualifier and result handler we will be using during navigation.
            PrintPathResultsHandler resultPrinter = new PrintPathResultsHandler();
            meetingWithBellatrix meetingWithBellatrix1 = new meetingWithBellatrix();

            // Start a breadth first navigation from Tom Riddle looking for this meeting
            // Qualifier.ANY  - instruct the navigate to pick any path to go down when it is traversing from the node
            // meetingWithBellatrix1 - instruct the navigate to only take paths with ending vertex "Bellatrix Lestrange"
            // resultPrinter - After a path went through both qualifiers, it will print out the name of the vertices and the edges
            //                 that connect them.
            Navigator friend1Navigator = friend1.navigate(Guide.SIMPLE_BREADTH_FIRST, Qualifier.ANY, meetingWithBellatrix1, resultPrinter);
            friend1Navigator.start();
            friend1Navigator.stop();

            tx.commit();
            
            //Delete the Database from the System
	    	System.out.println();
	    	System.out.println("Friend Database removed");
            GraphFactory.delete("FriendsGraph", "FriendsGraphProperties.properties");
	    }
        catch (ConfigurationException cE){
        	System.out.println(" Configuration Exception was thrown .. ");
        	System.out.println(cE.getMessage());
        }
	    finally
	    {
	       // If the transaction was not committed, complete
	       // will roll it back
	    	if (tx != null)
	    		tx.complete();
	        if (myGraphDB != null)
	        	myGraphDB.close();
	    }
    }
}

/*
 * For every result passing through all the qualifiers in the navigation,
 * this will print out all the possible paths.
 */
class PrintPathResultsHandler implements NavigationResultHandler
{
	public void handleResultPath(Path result, Navigator navigator) {
		System.out.print("Found matching path : ");
		System.out.print(result.get(0).getVertex().toString());
		
		// For h in p
        for(Hop h : result)
        {
            if(h.hasEdge())
            {
                System.out.print("<->" + h.getVertex().toString());
            } 
        }
	}
};

/*
 * This qualifier only return true if the end of the path is "Bellatrix Lestrange"
 */
class meetingWithBellatrix implements Qualifier
{
	public boolean qualify(Path currentPath)
	{
		//this is a qualifying path if the last vertex of this path is Bellatrix Lestrange
		if ("Bellatrix Lestrange".equals(((Person)(currentPath.getFinalHop().getVertex())).getName()))
			return true;
		else
			return false;
	}
};

/*
* The persistent class definition for the Person Vertex type
*/
class Person extends BaseVertex
{
    private String name;

    public Person(String name)
    {
        setName(name);
    }

    public String getName() 
    {
        fetch();
        return this.name;
    }

    public void setName(String name) 
    { 
        markModified();
        this.name = name; 
    } 

	@Override
    public String toString()
    {
        return this.name;
    }

};

/*
* The persistent class definition for the MetWith Edge type
*/
class MetWith extends BaseEdge
{
	private String location;

    public MetWith(String location)
    {
        setLocation(location);
    }

    public String getLocation() 
    {
        fetch();
        return this.location;
     }

    public void setLocation(String location) 
    { 
        markModified();
        this.location = location; 
    } 

    public String getLabel()
    {
        return "Met at " + location;
    } 
};