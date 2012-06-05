package transferIn;


import java.io.IOException;
import java.net.UnknownHostException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongotoDynamo {

	
	static AmazonDynamoDBClient dynamoDB;
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		init();
		
		readfromMongo();
		
		writetoDynamo();

	}

	private static void writetoDynamo() {
		// TODO Auto-generated method stub
		
	}

	private static void readfromMongo() throws UnknownHostException, MongoException {
		// TODO Auto-generated method stub
		Mongo mongo;
		mongo = new Mongo("10.3.4.84", 27017);
		mongo.getDB("db").authenticate("cssc", new char[] { '1' });
		DB db = mongo.getDB("db");
		String [] collectionnames = (String[]) db.getCollectionNames().toArray();
		for(int i=0;i<collectionnames.length;i++){
			System.out.println("Name of Collection: "+collectionnames[i]);
			DBCollection collection = db.getCollection(collectionnames[i]);
			
		}
	}

	private static void init() throws IOException {
		// TODO Auto-generated method stub
		AWSCredentials credentials = new PropertiesCredentials(
				MongotoDynamo.class.getResourceAsStream("AwsCredentials.properties"));

        dynamoDB = new AmazonDynamoDBClient(credentials);
	}

}
