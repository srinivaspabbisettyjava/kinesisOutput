package com.amazonaws.lambda.demo;

import org.springframework.beans.factory.annotation.Autowired;

import com.amazonaws.lambda.demo.model.DynamoTable;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;


public class LambdaFunctionHandler implements RequestHandler<KinesisEvent, Integer> {
	
	@Autowired
	private DynamoTable dynamoTable;

	@Override
	public Integer handleRequest(KinesisEvent event, Context context) {
		
		String payload = null;
		context.getLogger().log("Input: " + event);

		for (KinesisEventRecord record : event.getRecords()) {
			payload = new String(record.getKinesis().getData().array());
			 context.getLogger().log("My Input Data: " + payload);
		}
		
		AmazonDynamoDB dbClient = AmazonDynamoDBClientBuilder.standard().build();
		DynamoDB dynamoDB = new DynamoDB(dbClient);
		
		Table table = dynamoDB.getTable("Sample");
		for(int i=0;i<1;i++){
		Item item = new Item().withPrimaryKey("ID",i).withString("Payload", payload);
		context.getLogger().log("Item ------" +item);
		PutItemOutcome outcome = table.putItem(item);
		}

		return event.getRecords().size();
	}
}
