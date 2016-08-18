package com.signiant.devops.dynamodb.clone;

import com.beust.jcommander.Parameter;

public class CommandLineArgs{

  public static final String TABLE_NAME = "--tableName";
  @Parameter(names = TABLE_NAME, description = "ARGUMENT - Name of the table", required = true)
  private String tableName;

  public String getTableName() {
      return tableName;
  }

  public static final String SOURCE_REGION = "--sourceRegion";
  @Parameter(names = SOURCE_REGION, description = "ARGUMENT - Region of source table", required = true)
  private String sourceRegion;

  public String getSourceRegion() {
    return sourceRegion;
  }

  public static final String DESTINATION_REGION = "--destinationRegion";
  @Parameter(names = DESTINATION_REGION, description = "ARGUMENT - Region of the destination table", required = true)
  private String destinationRegion;

  public String getDestinationRegion() {
    return destinationRegion;
  }

  public static final String WRITE_CAPACITY = "--writeCapacity";
  @Parameter(names = WRITE_CAPACITY, description = "ARGUMENT - Replica write capacity units", required=false )
  private Long writeCapacity = 500L;

  public Long getWriteCapacity(){
    return writeCapacity;
  }
  public static final String RESET = "--reset";
  @Parameter(names = RESET, description = "FLAG - Reset capacity of replica to match the source and exit", required = false)
  private Boolean reset = false;

  public Boolean getReset(){
    return reset;
  }

  public static final String HELP = "--help";
  @Parameter(names = HELP, description = "FLAG - Display usage information", help = true)
  private boolean help;

  public boolean getHelp(){
    return help;
  }

}
