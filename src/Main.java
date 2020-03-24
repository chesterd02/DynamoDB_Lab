import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;

import java.util.Iterator;

public class Main {

    private static String tableName = "follows";
    private static String indexName = "followee_handle-follower_handle-index";

    public static void main(String[] args) throws Exception{
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("https://dynamodb.us-west-2.amazonaws.com", "us-west-2"))
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable("follows");


        //************ADD ITEMS TO THE TABLE***********
//        String follower_handle = "@FredFlinstone";
//
//        String followee_handle = "@ClintEastwood";
//         int i = 0;
//         while (i < 1){
//             i ++;
//             StringBuilder followee_handleSB = new StringBuilder(followee_handle);
//             followee_handleSB.append(i);
//            try{
//                PutItemOutcome outcome = table
//                        .putItem(new Item().withPrimaryKey("follower_handle", follower_handle, "followee_handle",followee_handleSB.toString()));
//
//                System.out.println("Put Item Succeeded:\n" + outcome.getPutItemResult());
//            }catch(Exception e){
//                System.err.println("Unable to add the item: " + follower_handle + " " + followee_handle);
//                System.err.println(e.getMessage());
//            }


        //*************READ ITEMS FROM THE TABLE ******************//
//        String follower = "@FollowFred1";
//        String followee = "@FredFlinstone";
//
//        GetItemSpec spec = new GetItemSpec().withPrimaryKey("follower_handle", follower,"followee_handle",followee);
//        try {
//            System.out.println("Attempting to read the item...");
//            Item outcome = table.getItem(spec);
//            System.out.println("GetItem succeeded: " + outcome);
//
//        }
//        catch (Exception e) {
//            System.err.println("Unable to read item: ");
//            System.err.println(e.getMessage());
//        }

        //************QUERY THE MAIN TABLE *********************//

        // You will Query by the hashKeyName, which in this case is the follower_handle
        // I can not change this to query by the followee_handle
        QuerySpec query = new QuerySpec().withHashKey("follower_handle", "@FredFlinstone");

        ItemCollection<QueryOutcome> items = null;
        Iterator<Item> iterator = null;
        Item item = null;
        try {
            items = table.query(query);

            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();

                System.out.println(item.getString("followee_handle"));
            }

        }
        catch (Exception e) {
            System.err.println("Unable to query movies from 1985");
            System.err.println(e.getMessage());
        }
    }


    // *********** FOR A QUERY ON AN INDEX OF THE MAIN TABLE ***********//

//    QueryRequest query = new QueryRequest()
//            .withTableName(tableName)
//            .withIndexName(indexName);
//
//    query = query.withQueryFilter("Follower_handle", "FredFlinstone");



    //**************FOR A QUERY ON THE MAIN TABLE  ************//

//        ItemCollection<QueryOutcome> items = null;
//        Iterator<Item> iterator = null;
//        Item item = null;
//        try {
//            items = table.query(query);
//
//            iterator = items.iterator();
//            while (iterator.hasNext()) {
//                item = iterator.next();
//
//                System.out.println(item.getString("followee_handle"));
//            }
//
//        }
//        catch (Exception e) {
//            System.err.println("Unable to query movies from 1985");
//            System.err.println(e.getMessage());
//        }
}
