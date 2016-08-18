package com.signiant.devops.dynamodb.clone;

import java.util.List;
import java.util.ArrayList;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexUpdate;
import com.amazonaws.services.dynamodbv2.model.UpdateGlobalSecondaryIndexAction;

public class TableCloneUtils{

  public static Region regionFromString(String regionName) throws IllegalArgumentException{
    Region region = RegionUtils.getRegion(regionName);
    if(region == null){
      throw new IllegalArgumentException("Argument \"" + regionName + "\" is not a valid AWS Region");
    }
    return region;
  }


  /**
  * Creates and returns a CreateTableRequest from a TableDescription
  */
  public static CreateTableRequest constructCreateTableRequest(TableDescription source){
    CreateTableRequest createRequest = new CreateTableRequest()
      .withTableName(source.getTableName())
      .withAttributeDefinitions(source.getAttributeDefinitions())
      .withKeySchema(source.getKeySchema())
      .withProvisionedThroughput(constructProvisionedThroughput(source.getProvisionedThroughput()))
      .withStreamSpecification(source.getStreamSpecification());

      List<LocalSecondaryIndexDescription> sourceLocalIndexes = source.getLocalSecondaryIndexes();
      if(sourceLocalIndexes != null){
        createRequest.setLocalSecondaryIndexes(constructLocalSecondaryIndex(sourceLocalIndexes));
      }


      List<GlobalSecondaryIndexDescription> sourceGlobalIndexes = source.getGlobalSecondaryIndexes();
      if(sourceGlobalIndexes != null){
        createRequest.setGlobalSecondaryIndexes(constructGlobalSecondaryIndex(sourceGlobalIndexes));
      }

      return createRequest;
  }

  /**
  * Creates and returns a CreateTableRequest from a TableDescription with the specified write capacity
  */
  public static CreateTableRequest constructCreateTableRequest(TableDescription source, Long writeCapacity){
    CreateTableRequest createRequest = new CreateTableRequest()
      .withTableName(source.getTableName())
      .withAttributeDefinitions(source.getAttributeDefinitions())
      .withKeySchema(source.getKeySchema())
      .withProvisionedThroughput(constructProvisionedThroughput(source.getProvisionedThroughput(), writeCapacity))
      .withStreamSpecification(source.getStreamSpecification());

      List<LocalSecondaryIndexDescription> sourceLocalIndexes = source.getLocalSecondaryIndexes();
      if(sourceLocalIndexes != null){
        createRequest.setLocalSecondaryIndexes(constructLocalSecondaryIndex(sourceLocalIndexes));
      }


      List<GlobalSecondaryIndexDescription> sourceGlobalIndexes = source.getGlobalSecondaryIndexes();
      if(sourceGlobalIndexes != null){
        createRequest.setGlobalSecondaryIndexes(constructGlobalSecondaryIndex(sourceGlobalIndexes, writeCapacity));
      }

      return createRequest;
  }

  /**
  *  Creates and returns a new ProvisionedThroughput fomr a ProvisionedThroughputDescription
  */
  public static ProvisionedThroughput constructProvisionedThroughput(ProvisionedThroughputDescription source){
    return new ProvisionedThroughput()
    .withReadCapacityUnits(source.getReadCapacityUnits())
    .withWriteCapacityUnits(source.getWriteCapacityUnits());
  }

  /**
  *  Creates and returns a new ProvisionedThroughput fomr a ProvisionedThroughputDescription with the specified write capacity
  */
  public static ProvisionedThroughput constructProvisionedThroughput(ProvisionedThroughputDescription source, Long writeCapacity){
    return new ProvisionedThroughput()
    .withReadCapacityUnits(source.getReadCapacityUnits())
    .withWriteCapacityUnits(writeCapacity);
  }


  /**
  *  Creates and returns a new list of type LocalSecondaryIndex from a List of type LocalSecondaryIndexDescription
  */
  public static List<LocalSecondaryIndex> constructLocalSecondaryIndex(List<LocalSecondaryIndexDescription> source){
    ArrayList<LocalSecondaryIndex> replicaIndexes = new ArrayList<LocalSecondaryIndex>();
    for (LocalSecondaryIndexDescription sourceIndex: source) {
      replicaIndexes.add(constructLocalSecondaryIndex(sourceIndex));
    }
    return replicaIndexes;
  }

  /**
  *  Creates and returns a new LocalSecondaryIndex from a LocalSecondaryIndexDescription
  */
  public static LocalSecondaryIndex constructLocalSecondaryIndex(LocalSecondaryIndexDescription source){
    return new LocalSecondaryIndex()
      .withIndexName(source.getIndexName())
      .withKeySchema(source.getKeySchema())
      .withProjection(source.getProjection());
  }


  /**
  *  Creates and returns a new List of type GlobalSecondaryIndex from a List of type GlobalSecondaryIndexDescription
  */
  public static List<GlobalSecondaryIndex> constructGlobalSecondaryIndex(List<GlobalSecondaryIndexDescription> source){
    ArrayList<GlobalSecondaryIndex> replicaIndexes = new ArrayList<GlobalSecondaryIndex>();
    for (GlobalSecondaryIndexDescription sourceIndex: source) {
      replicaIndexes.add(constructGlobalSecondaryIndex(sourceIndex));
    }
    return replicaIndexes;
  }

  /**
  *  Creates and returns a new List of type GlobalSecondaryIndex  with the specified write capacity from a List of type GlobalSecondaryIndexDescription
  */
  public static List<GlobalSecondaryIndex> constructGlobalSecondaryIndex(List<GlobalSecondaryIndexDescription> source, Long writeCapacity){
    ArrayList<GlobalSecondaryIndex> replicaIndexes = new ArrayList<GlobalSecondaryIndex>();
    for (GlobalSecondaryIndexDescription sourceIndex: source) {
      replicaIndexes.add(constructGlobalSecondaryIndex(sourceIndex, writeCapacity));
    }
    return replicaIndexes;
  }


  /**
  *  Creates and returns a new GlobalSecondaryIndex from a GlobalSecondaryIndexDescription
  */
  public static GlobalSecondaryIndex constructGlobalSecondaryIndex(GlobalSecondaryIndexDescription source){
    return new GlobalSecondaryIndex()
      .withIndexName(source.getIndexName())
      .withKeySchema(source.getKeySchema())
      .withProjection(source.getProjection())
      .withProvisionedThroughput(constructProvisionedThroughput(source.getProvisionedThroughput()));
  }

  /**
  *  Creates and returns a new GlobalSecondaryIndex with the specified WriteCapacity from a GlobalSecondaryIndexDescription
  */
  public static GlobalSecondaryIndex constructGlobalSecondaryIndex(GlobalSecondaryIndexDescription source, Long writeCapacity){
    return new GlobalSecondaryIndex()
      .withIndexName(source.getIndexName())
      .withKeySchema(source.getKeySchema())
      .withProjection(source.getProjection())
      .withProvisionedThroughput(constructProvisionedThroughput(source.getProvisionedThroughput(), writeCapacity));
  }

  /**
  * Creates and returns a new List of type GlobalSecondaryIndexUpdate from a List of type GlobalSecondaryIndexDescription
  */
  public static ArrayList<GlobalSecondaryIndexUpdate> constructGlobalSecondaryIndexUpdate(List<GlobalSecondaryIndexDescription> source){
    ArrayList<GlobalSecondaryIndexUpdate> indexUpdates = new ArrayList<GlobalSecondaryIndexUpdate>();
    for (GlobalSecondaryIndexDescription sourceIndex: source) {
      indexUpdates.add(constructGlobalSecondaryIndexUpdate(sourceIndex));
    }
    return indexUpdates;
  }

  /**
  * Creates and returns a new GlobalSecondaryIndexUpdate to match the values from a GlobalSecondaryIndexDescription
  */
  public static GlobalSecondaryIndexUpdate constructGlobalSecondaryIndexUpdate(GlobalSecondaryIndexDescription source){
    return new GlobalSecondaryIndexUpdate().withUpdate(new UpdateGlobalSecondaryIndexAction()
      .withIndexName(source.getIndexName())
      .withProvisionedThroughput(constructProvisionedThroughput(source.getProvisionedThroughput())));
  }
}
