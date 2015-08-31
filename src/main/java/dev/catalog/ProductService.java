package dev.catalog;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ProductService {

    public static final String TABLE_NAME = "product";

    private static final Gson gson = new GsonBuilder().create();

    private static enum FIELD {
        name
    };

    private final Table table;

    public ProductService(AmazonDynamoDBClient client) {
        this.table = new DynamoDB(client).getTable(TABLE_NAME);
    }

    public Product create(String name, Long price) {

        Item item = Item.fromJSON(gson.toJson(new Product(name, price)));

        PutItemSpec putItemSpec = new PutItemSpec()
                .withItem(item)
                .withConditionExpression("attribute_not_exists(#name)")
                .withNameMap(new NameMap()
                        .with("#name", FIELD.name.name()));

        table.putItem(putItemSpec);

        return toProduct(item);
    }

    public Product get(String name) {

        GetItemSpec getItemSpec = new GetItemSpec()
                .withPrimaryKey(FIELD.name.name(), name)
                .withConsistentRead(true);

        Item item = table.getItem(getItemSpec);

        return item != null ? toProduct(item) : null;
    }

    public Product adjustPrice(String name, Long amount) {

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey(new KeyAttribute(FIELD.name.name(), name))
                .withUpdateExpression("SET price = price + :amt")
                .withConditionExpression("price <= (:maxprice - :amt)")
                .withValueMap(new ValueMap()
                        .with(":amt", amount)
                        .with(":maxprice", 200L))
                .withReturnValues(ReturnValue.UPDATED_NEW);

        UpdateItemOutcome updateItemOutcome = table.updateItem(updateItemSpec);

        return toProduct(updateItemOutcome.getItem());
    }

    private Product toProduct(Item item) {
        return gson.fromJson(item.toJSON(), Product.class);
    }
}
