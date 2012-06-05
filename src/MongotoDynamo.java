


import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.services.dynamodb.model.TableStatus;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongotoDynamo {

	
	static AmazonDynamoDBClient dynamoDB;
	
	static String tableName = "my-favorite-table";
	static BasicDBObject [] objects;
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
		

        // Create a table with a primary key named 'name', which holds a string
        CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
            .withKeySchema(new KeySchema(new KeySchemaElement().withAttributeName("name").withAttributeType("S")))
            .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(5L));
        TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
        System.out.println("Created Table: " + createdTableDescription);

        // Wait for it to become active
        waitForTableToBecomeAvailable(tableName);

        // Describe our new table
        DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
        TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
        System.out.println("Table Description: " + tableDescription);

        // Add items
        for(int i=0;i<objects.length;i++){
        	Map<String, AttributeValue> item = newItem(objects[i]);
            PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
            PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
            System.out.println("Result: " + putItemResult);
        }
        

	}

	private static Map<String, AttributeValue> newItem(BasicDBObject object) {
		// TODO Auto-generated method stub
		 Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
	        Object [] keynames = object.keySet().toArray();
	        for(int i=0;i<keynames.length;i++){
	        	item.put(keynames[i].toString(), new AttributeValue(object.get(keynames[i].toString()).toString()));
	        }

	        return item;
	}

	private static void waitForTableToBecomeAvailable(String tableName) {
		// TODO Auto-generated method stub
		System.out.println("Waiting for " + tableName + " to become ACTIVE...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {Thread.sleep(1000 * 20);} catch (Exception e) {}
            try {
                DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
                TableDescription tableDescription = dynamoDB.describeTable(request).getTable();
                String tableStatus = tableDescription.getTableStatus();
                System.out.println("  - current state: " + tableStatus);
                if (tableStatus.equals(TableStatus.ACTIVE.toString())) return;
            } catch (AmazonServiceException ase) {
                if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException") == false) throw ase;
            }
        }

        throw new RuntimeException("Table " + tableName + " never went active");
	}

	private static void readfromMongo() throws UnknownHostException, MongoException {
		// TODO Auto-generated method stub
		Mongo mongo;
		mongo = new Mongo("10.3.4.84", 27017);
		mongo.getDB("db").authenticate("cssc", new char[] { '1' });
		DB db = mongo.getDB("db");
		Object [] collectionnames = db.getCollectionNames().toArray();
		for(int i=0;i<1;i++){
			System.out.println("Name of Collection: "+collectionnames[i]);
			tableName = collectionnames[i].toString();
			DBCollection collection = db.getCollection(collectionnames[i].toString());
			DBCursor cursor = collection.find();
			
			objects = new BasicDBObject[cursor.size()];
			for(int j=0; cursor.hasNext(); j++){
				BasicDBObject object = (BasicDBObject) cursor.next();
				objects[j] = object;
				Object [] keynames = object.keySet().toArray();
				for(int k=0;k<keynames.length;k++){
					System.out.println("Key: "+keynames[k]+" Attribute: "+object.getString(keynames[k].toString()));
				}
			}
		}
	}

	private static void init() throws IOException {
		// TODO Auto-generated method stub
		AWSCredentials credentials = new PropertiesCredentials(
				MongotoDynamo.class.getResourceAsStream("AwsCredentials.properties"));

        dynamoDB = new AmazonDynamoDBClient(credentials);
	}

}
