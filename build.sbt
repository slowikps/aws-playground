name := "aws-playground"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
//  "com.amazonaws" % "aws-java-sdk-dynamodb"            % "1.11.109",
//  "com.amazonaws" % "dynamodb-streams-kinesis-adapter" % "1.1.1",
//  "com.amazonaws" % "amazon-kinesis-client"            % "1.7.4",
////  "com.amazonaws" % "amazon-kinesis-connectors"        % "1.3.0",
//  "com.amazonaws" % "dynamodb-streams-kinesis-adapter" % "1.2.2",
  "com.amazonaws" % "dynamodb-streams-kinesis-adapter" % "1.1.1",
  "com.amazonaws" % "amazon-kinesis-client"            % "1.7.4",
  "com.amazonaws" % "aws-java-sdk-sts"                 % "1.11.109",
  "com.gu"        %% "scanamo"                         % "0.9.4"
)

//"com.amazonaws" % "dynamodb-streams-kinesis-adapter" % "1.1.1",
//"com.amazonaws" % "amazon-kinesis-client" % "1.7.4",
//"com.amazonaws" % "aws-java-sdk-sts" % "1.11.109",
