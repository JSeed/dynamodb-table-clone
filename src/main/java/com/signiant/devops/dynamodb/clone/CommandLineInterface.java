package com.signiant.devops.dynamodb.clone;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import org.apache.log4j.Logger;

import com.signiant.devops.dynamodb.clone.exception.InvalidStreamViewTypeException;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.TableUtils.TableNeverTransitionedToStateException;

public class CommandLineInterface{

  private static final Logger LOGGER = Logger.getLogger(CommandLineInterface.class);

  public static void main(String args[]){
    int exitCode = 0;
    CommandLineArgs params = new CommandLineArgs();
    JCommander cmd = new JCommander(params);

    try{
      cmd.parse(args);
    }catch(ParameterException e){
      JCommander.getConsole().println(e.getMessage());
      cmd.usage();
      System.exit(1);
    }

    if(params.getHelp()) {
      cmd.usage();
      return;
    }

    final String sourceRegion = params.getSourceRegion();
    final String destinationRegion = params.getDestinationRegion();
    final String tableName = params.getTableName();
    final Boolean reset = params.getReset();
    final Long writeCapacity = params.getWriteCapacity();

    TableCloneClient cloneClient = new TableCloneClient(sourceRegion, destinationRegion, tableName);

    if(reset == true){
      LOGGER.info("Resetting throughput on replica table");
      cloneClient.resetThroughput();
    }else{
      try{
        cloneClient.validateSource();
        cloneClient.cloneTable(writeCapacity);
      }catch(InvalidStreamViewTypeException e){
        LOGGER.error("Invalid stream view type set on source table", e);
        exitCode = 1;
      }
    }

    System.exit(exitCode);
  }
}
