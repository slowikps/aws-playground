// Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// Licensed under the Apache License, Version 2.0.
package dynamo;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class StreamsAdapterDemo {

    private static Worker worker;
    private static KinesisClientLibConfiguration workerConfig;
    private static IRecordProcessorFactory recordProcessorFactory;

    private static AmazonDynamoDBStreamsAdapterClient adapterClient;
//    private static AWSCredentialsProvider streamsCredentials;

    private static AmazonDynamoDB dynamoDBClient;
    private static AWSCredentialsProvider dynamoDBCredentials;

    private static AmazonCloudWatch cloudWatchClient;

//    private static String serviceName = "dynamodb";
//    private static String dynamodbEndpoint = "dynamodb.eu-central-1.amazonaws.com";
    private static String tablePrefix = "KCL-Demo";
    private static String streamArn;

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Starting demo...");

        String srcTable = tablePrefix + "-src";
        String destTable = tablePrefix + "-dest";
//        streamsCredentials = new ProfileCredentialsProvider();
        dynamoDBCredentials = new ProfileCredentialsProvider();
        recordProcessorFactory = new StreamsRecordProcessorFactory(destTable);

        adapterClient = new AmazonDynamoDBStreamsAdapterClient(dynamoDBCredentials, new ClientConfiguration());
        adapterClient.setEndpoint("https://streams.dynamodb.eu-central-1.amazonaws.com");
        dynamoDBClient = AmazonDynamoDBClientBuilder.standard().build();

        cloudWatchClient = AmazonCloudWatchClientBuilder.standard().build();

        setUpTables();
//        performOps(srcTable);
        //<TEST
//        lowLevelAPI();

        //TEST>

        workerConfig = new KinesisClientLibConfiguration(
                "streams-adapter-demo",
                streamArn,
                dynamoDBCredentials,
                "streams-demo-worker").withMaxRecords(1000).withIdleTimeBetweenReadsInMillis(500)
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

        System.out.println("Creating worker for stream: " + streamArn);
        worker = new Worker(recordProcessorFactory,
                workerConfig,
                adapterClient,
                dynamoDBClient,
                cloudWatchClient);

        System.out.println("Starting worker...");
        Thread t = new Thread(worker);
        t.start();


        TimeUnit.DAYS.sleep(1);

        Thread.sleep(25000);
        worker.shutdown();
        t.join();

        if (StreamsAdapterDemoHelper.scanTable(dynamoDBClient, srcTable).getItems()
                .equals(StreamsAdapterDemoHelper.scanTable(dynamoDBClient, destTable).getItems())) {
            System.out.println("Scan result is equal.");
        }
        else {
            System.out.println("Tables are different!");
        }
        System.out.println("Done.");
//        cleanupAndExit(0);
//        System.exit(0);
    }

    private static void lowLevelAPI() {
        String myStreamArn = "arn:aws:dynamodb:eu-central-1:705871468747:table/KCL-Demo-src/stream/2017-07-26T12:50:43.538";
        AmazonDynamoDBStreams streamsClient = AmazonDynamoDBStreamsClientBuilder.standard().build();

        DescribeStreamResult describeStreamResult =
                streamsClient.describeStream(new DescribeStreamRequest()
                        .withStreamArn(myStreamArn));
        String streamArn =
                describeStreamResult.getStreamDescription().getStreamArn();
        List<Shard> shards =
                describeStreamResult.getStreamDescription().getShards();
        for (Shard shard : shards) {
            String shardId = shard.getShardId();
            System.out.println(
                    "Processing " + shardId + " from stream "+ streamArn);

            // Get an iterator for the current shard

            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
                    .withStreamArn(myStreamArn)
                    .withShardId(shardId)
                    .withShardIteratorType(ShardIteratorType.TRIM_HORIZON);
            GetShardIteratorResult getShardIteratorResult =
                    streamsClient.getShardIterator(getShardIteratorRequest);
            String nextItr = getShardIteratorResult.getShardIterator();

            while (nextItr != null) {

                // Use the iterator to read the data records from the shard

                GetRecordsResult getRecordsResult =
                        streamsClient.getRecords(new GetRecordsRequest().
                                withShardIterator(nextItr));
                List<Record> records = getRecordsResult.getRecords();
                System.out.println("Getting records...");
                if(records.isEmpty()){
                    break;
                }
                for (Record record : records) {
                    System.out.println(record);
                }
                nextItr = getRecordsResult.getNextShardIterator();
            }
        }
    }

    private static void setUpTables() {
        String srcTable = tablePrefix + "-src";
        String destTable = tablePrefix + "-dest";
        streamArn = StreamsAdapterDemoHelper.createTable(dynamoDBClient, srcTable);
        StreamsAdapterDemoHelper.createTable(dynamoDBClient, destTable);

        awaitTableCreation(srcTable);
    }

    private static void awaitTableCreation(String tableName) {
        Integer retries = 0;
        Boolean created = false;
        while (!created && retries < 100) {
            DescribeTableResult result = StreamsAdapterDemoHelper.describeTable(dynamoDBClient, tableName);
            created = result.getTable().getTableStatus().equals("ACTIVE");
            if (created) {
                System.out.println("Table is active.");
                return;
            }
            else {
                retries++;
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    // do nothing
                }
            }
        }
        System.out.println("Timeout after table creation. Exiting...");
        cleanupAndExit(1);
    }

    private static void performOps(String tableName) {
        StreamsAdapterDemoHelper.putItem(dynamoDBClient, tableName, "101", "test1");
        StreamsAdapterDemoHelper.updateItem(dynamoDBClient, tableName, "101", "test2");
        StreamsAdapterDemoHelper.deleteItem(dynamoDBClient, tableName, "101");
        StreamsAdapterDemoHelper.putItem(dynamoDBClient, tableName, "102", "demo3");
        StreamsAdapterDemoHelper.updateItem(dynamoDBClient, tableName, "102", "demo4");
        StreamsAdapterDemoHelper.deleteItem(dynamoDBClient, tableName, "102");
    }

    private static void cleanupAndExit(Integer returnValue) {
        String srcTable = tablePrefix + "-src";
        String destTable = tablePrefix + "-dest";
        dynamoDBClient.deleteTable(new DeleteTableRequest().withTableName(srcTable));
        dynamoDBClient.deleteTable(new DeleteTableRequest().withTableName(destTable));
        System.exit(returnValue);
    }

}