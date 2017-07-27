package dynamodb

import java.util.concurrent.TimeUnit

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import dynamo.{StreamsAdapterDemoHelper, StreamsRecordProcessor}

object StreamsDemo extends App {

  val (srcTable, destTable) = {
    val name = "KCL-Demo"
    (s"$name-src", s"$name-dest")
  }

  private val dynamoDBClient = AmazonDynamoDBClientBuilder.standard.build;

  def getArn(tableName: String): String = {
    val result = StreamsAdapterDemoHelper.describeTable(dynamoDBClient, tableName)
    result.getTable.getLatestStreamArn
  }

  private val worker = {
    val recordProcessorFactory = new ScalaRecordProcessorFactory(destTable)
    val credentials            = new ProfileCredentialsProvider

    val workerConfig =
      new KinesisClientLibConfiguration("streams-adapter-demo", getArn(srcTable), credentials, "streams-demo-worker")
        .withMaxRecords(1000)
        .withIdleTimeBetweenReadsInMillis(500)
        .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)

    def getAdapterClient(): AmazonDynamoDBStreamsAdapterClient = {
      val adapterClient = new AmazonDynamoDBStreamsAdapterClient(credentials, new ClientConfiguration)
      adapterClient.setEndpoint("https://streams.dynamodb.eu-central-1.amazonaws.com")
      adapterClient
    }

    new Worker(
      recordProcessorFactory,
      workerConfig,
      getAdapterClient,
      dynamoDBClient,
      AmazonCloudWatchClientBuilder.standard.build
    )
  }

  new Thread(worker).start()

  TimeUnit.SECONDS.sleep(60)
  worker.shutdown()
}

class ScalaRecordProcessorFactory(tableName: String) extends IRecordProcessorFactory {

  override def createProcessor() = new StreamsRecordProcessor(AmazonDynamoDBClientBuilder.standard.build, tableName)
}
