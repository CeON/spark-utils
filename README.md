# spark-utils
A set of utilities that facilitate writing [Spark](http://spark.apache.org/) programs in Java.

At present it simplifies:
* writing automatic spark job tests,
* working with avro files.

The project is written in Java 8.

### How to use it in maven ##
#### Add [CEON](http://ceon.pl/in-english) maven repository to the "repositories" section of your project's pom or maven settings.xml:
```xml
   <repository>
      <id>ceon-repo</id>
	  <url>https://maven.ceon.pl/artifactory/repo/</url>
   </repository>
```
#### Add the spark-utils dependency to your project's pom:
```xml
   <dependency>
       <groupId>pl.edu.icm.spark-utils</groupId>
       <artifactId>spark-utils</artifactId>
       <version>1.0.0</version>
   </dependency>
```

## Support for testing spark jobs
To test a spark job written in java (or scala) you have to run a java main class which contains the job definition. Usually you'll want to pass some arguments to it (spark application name, job parameters) to configure the job properly.

*Spark-utils* takes on the burden of the tedious and repetitive work of starting the spark job tested and of passing parameters to it.

How can you write a test of a spark job by using *spark-utils*? Just see the example below:

```java
    @Test
    public void peopleClonerJob() throws IOException {
        
        
        //------------------------ given -----------------------------
        
        // prepare some data and params
        String inputDirPath = ...
        String outputDirPath = ...
        
        // configure a job
        SparkJob sparkJob = SparkJobBuilder
                                           .create()
                                           
                                           .setAppName("Spark People Cloner")
        
                                           .setMainClass(SparkPeopleCloner.class) // main class with the job definition
                                           
                                           .addArg("-inputPath", inputDirPath)
                                           .addArg("-outputPath", outputDirPath)
                                           .addArg("-numberOfCopies", "3")
                                           
                                           .build();
        
        
        //------------------------ execute -----------------------------
        
        executor.execute(sparkJob); // execute the job
        
        
        //------------------------ assert -----------------------------
        
        // read the result
        List<Person> people = ... 

        // assert the result is ok
        assertEquals(15, people.size());
        assertEquals(3, people.stream().filter(p->p.getName().equals(new Utf8("Stieg Larsson"))).count());
        
    }


```
You may also be interested in seeing real production code that uses *spark-utils* to test spark jobs. Here is an example from [IIS](https://github.com/openaire/iis) project: [AffMatchingJobTest](https://github.com/openaire/iis/blob/cdh5/iis-wf/iis-wf-affmatching/src/test/java/eu/dnetlib/iis/wf/affmatching/AffMatchingJobTest.java)

## Support for working with [Avro](https://avro.apache.org) files
### Reading avro files

To read an avro file to a spark JavaRDD just import [SparkAvroLoader](https://github.com/CeON/spark-utils/blob/master/src/main/java/pl/edu/icm/sparkutils/avro/SparkAvroLoader.java) and use its **loadJavaRDD** method:
```java
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
...
JavaRDD<SomeAvroRecord> items = new SparkAvroLoader().loadJavaRDD(javaSparkContext, avroInputPath, someAvroRecordClass);
```
where:
* *avroInputPath* is a path to an avro file or directory with avro files
* *SomeAvroRecordClass* is a class generated from an avro [avdl](https://avro.apache.org/docs/1.7.5/idl.html) file

#### Why using the standard hadoop API to read Avro files can be tricky
To read an avro file you could also use a standard hadoop API:

```java
JavaPairRDD<AvroKey<T>, NullWritable> inputRecords = (JavaPairRDD<AvroKey<T>, NullWritable>) sc.newAPIHadoopFile(avroDatastorePath, AvroKeyInputFormat.class, avroRecordClass, NullWritable.class, job.getConfiguration());
```

However, **when using the standard hadoop API in Spark, you can come across unpredictable errors**, because the hadoop record reader reuses the same Writable object for all records read. It is not a problem in case of MapReduce jobs where each record is processed separately. In Spark, however, it can sometimes lead to undesired effects. For example, in case of caching an rdd only the last object read will be cached (multiplee times, equal to the number of all records read). Probably it has something in common with creating multiple references to the same object.

To eliminate this phenomenon, one should clone each avro record after it has been read. This is exactly what the SparkAvroLoader does for you.

In addition, you get a simpler API that is easy to use.



### Writing avro files
To write a spark JavaRDD to avro files, import [SparkAvroSaver](https://github.com/CeON/spark-utils/blob/master/src/main/java/pl/edu/icm/sparkutils/avro/SparkAvroSaver.java) and use its **saveJavaRDD** or **saveJavaPairRDDKeys**, for example:

```java
import pl.edu.icm.sparkutils.avro.SparkAvroSaver
...
sparkAvroSaver.saveJavaRDD(javaRDD, avroSchema, outputPath);
```
where:
* *avroSchema* is an avro schema of objects that will be saved
* *outputPath* points to a place where the avro files will be saved


### Using avro in [Kryo](https://github.com/EsotericSoftware/kryo)

*Spark-utils* makes it easier to use the Kryo serialization by providing a specialized implementation of Kryo registrator. The registrator makes it possible for Kryo to serialize [Avro](https://avro.apache.org) generated classes. Without it Kryo throws an exception while deserializing avro collections (avro uses its own implementation of java.util.List that doesn't have no-argument constructor which is needed by Kryo) (see also: https://issues.apache.org/jira/browse/SPARK-3601).

How to use it:
```java
// in spark job:
...
SparkConf conf = new SparkConf();
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
...
```


