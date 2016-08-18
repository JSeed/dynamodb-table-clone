package com.signiant.devops.dynamodb.clone;

import org.apache.log4j.Logger;

import com.signiant.devops.dynamodb.clone.exception.InvalidStreamViewTypeException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

public class TableCloneClient {

  private static final Logger LOGGER = Logger.getLogger(TableCloneClient.class);

  private static final String STREAM_VIEW_TYPE = "NEW_AND_OLD_IMAGES";

  private static final int WAITING_INTERVAL = 5000;
  private static final int WAITING_TIMEOUT = 60000;

  private AmazonDynamoDBClient sourceClient;
  private AmazonDynamoDBClient replicaClient;
  private String tableName;

  public TableCloneClient(String sourceRegionName, String replicaRegionName, String tableName){
    this.sourceClient = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain()).withRegion(TableCloneUtils.regionFromString(sourceRegionName));
    this.replicaClient = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain()).withRegion(TableCloneUtils.regionFromString(replicaRegionName));
    this.tableName = tableName;
  }

  public TableCloneClient(AmazonDynamoDBClient sourceClient, AmazonDynamoDBClient replicaClient, String tableName){
    this.sourceClient = sourceClient;
    this.replicaClient = replicaClient;
    this.tableName = tableName;
  }

  /**
  * Validate the StreamSpecification on the source table, enabling streams if necessary
  * Returns the new TableDescription
  * Throw InvalidStreamViewTypeException if stream is enabled, but has the wrong view type
  */
  public void validateSource() throws InvalidStreamViewTypeException{

    TableDescription sourceTableDesc = sourceClient.describeTable(tableName).getTable();
    StreamSpecification streamSpec = sourceTableDesc.getStreamSpecification();

    if(streamSpec == null){
      LOGGER.info("Enabling stream with view type " + STREAM_VIEW_TYPE + " on table " + tableName);
      sourceTableDesc = sourceClient.updateTable(new UpdateTableRequest().withTableName(tableName).withStreamSpecification(new StreamSpecification().withStreamEnabled(true).withStreamViewType(STREAM_VIEW_TYPE))).getTableDescription();
    }else if(!streamSpec.getStreamViewType().equals(STREAM_VIEW_TYPE)){
      throw new InvalidStreamViewTypeException("Expected " + STREAM_VIEW_TYPE + ", found " + streamSpec.getStreamViewType());
    }

  }

  /**
  * Create a new replica table from the TableDescription of the source table
  */
  public void cloneTable(Long writeCapacity){
    TableDescription sourceDescription = sourceClient.describeTable(tableName).getTable();

    CreateTableRequest replica = TableCloneUtils.constructCreateTableRequest(sourceDescription, writeCapacity);

    LOGGER.info("Creating replica table " + tableName + ".");
    replicaClient.createTable(replica);
    LOGGER.info("Replica table "+ tableName + " created.");

    try{
      TableUtils.waitUntilActive(replicaClient, tableName, WAITING_TIMEOUT, WAITING_INTERVAL);
    }catch(InterruptedException e){
      LOGGER.warn("Thread was interrupted while polling status of table: " + tableName);
    }
  }

  /**
  * Set the throughput on the replica to that of the source
  */
  public void resetThroughput(){
    TableDescription sourceDescription = sourceClient.describeTable(tableName).getTable();

    UpdateTableRequest resetReplicaRequest = new UpdateTableRequest()
      .withTableName(tableName)
      .withProvisionedThroughput(TableCloneUtils.constructProvisionedThroughput(sourceDescription.getProvisionedThroughput()));

    if(sourceDescription.getGlobalSecondaryIndexes() != null){
      resetReplicaRequest.setGlobalSecondaryIndexUpdates(TableCloneUtils.constructGlobalSecondaryIndexUpdate(sourceDescription.getGlobalSecondaryIndexes()));
    }
    try{
      replicaClient.updateTable(resetReplicaRequest);
      TableUtils.waitUntilActive(replicaClient, tableName, WAITING_TIMEOUT, WAITING_INTERVAL);
    }catch(InterruptedException e){
      LOGGER.warn("Thread was interrupted while polling status of table: " + tableName);
    }catch(AmazonServiceException e){
      if(e.getErrorCode().equals("ValidationException"))
        LOGGER.info("Replica throughput already matches source");
      else
        throw e;
    }
  }
}
