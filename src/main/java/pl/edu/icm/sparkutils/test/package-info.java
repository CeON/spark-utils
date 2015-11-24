/**
 *
 * Support classes for executing spark jobs in unit tests. <br/><br/>
 *
 *
 *
 * A test method could look like for example this: <br/>
 * <pre>
 *    public void test() {
 * 
 *        // given 
 *        
 *        [prepare the test data] 
 *       
 *       
 *       
 *        SparkJob sparkJob = SparkJobBuilder
 *                                           .create()
 *                                              
 *                                           .setAppName("Spark SQL Avro Cloner")
 *           
 *                                           .setMainClass(SparkSqlAvroCloner.class)
 *                                           .addArg("-avroSchemaClass", Person.class.getName())
 *                                           .addArg("-inputAvroPath", inputDirPath)
 *                                           .addArg("-outputAvroPath", outputDirPath)
 *                                           .addArg("-numberOfCopies", ""+3)
 *                                              
 *                                           .build();
 *       
 *       
 *       // execute
 *      
 *       executor.execute(sparkJob);
 *       
 *       
 *       
 *       // assert
 *       
 *       
 *       [assert the result of the above job execution]
 * }
 * </pre> 
 * 
 * 
 * @author Łukasz Dumiszewski <br/><br/>
 *
 */
package pl.edu.icm.sparkutils.test;


