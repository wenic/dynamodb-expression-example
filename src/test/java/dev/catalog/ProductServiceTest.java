package dev.catalog;

import dev.catalog.Product;
import dev.catalog.ProductService;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class ProductServiceTest {
    
    static final String ENDPOINT = "http://localhost:8000";
    static final String ACCESS_KEY = "access";
    static final String SECRET_KEY = "secret";
    
    static final AmazonDynamoDBClient client;
    static {
        client = new AmazonDynamoDBClient(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY));
        client.setEndpoint(ENDPOINT);
    }
    
    private ProductService service = new ProductService(client);
    
    @Before
    public void setUp() {
        
        try {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
                    .withTableName(ProductService.TABLE_NAME);
            
            client.deleteTable(deleteTableRequest);
        } catch (ResourceNotFoundException e) {
            
        }
        
        CreateTableRequest request = new CreateTableRequest()
                .withTableName(ProductService.TABLE_NAME)
                .withAttributeDefinitions(new AttributeDefinition("name", "S"))
                .withKeySchema(new KeySchemaElement("name", KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(5L)
                        .withWriteCapacityUnits(5L));
        
        CreateTableResult createTableResult = client.createTable(request);
    }
    
    @Test
    public void test() {
        
        service.create("Apple", 100L);
        
        Product product = service.get("Apple");
        assertNotNull(product);
        
        assertEquals("Apple", product.getName());
        assertEquals(new Long(100L), product.getPrice());
        
        service.adjustPrice("Apple", 50L);
        
        product = service.get("Apple");
        assertNotNull(product);
        assertEquals("Apple", product.getName());
        assertEquals(new Long(150L), product.getPrice());
    }
}
