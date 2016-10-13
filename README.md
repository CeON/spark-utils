# spark-utils
A set of java utilities that facilitate writing programs in [Spark](http://spark.apache.org/).

At present it simplifies:
* writing automatic spark job tests,
* working with avro files.

The project is written in Java 8.


## Testing spark jobs
To test a spark job written in java (or scala) you have to run a java main class which contains the job definition. Usually you'll want to pass some arguments to it (spark application name, job parameters) to configure the job properly.

*Spark-utils* takes on the burden of the tedious and repetitive work of starting the tested spark job and of passing the parameters to it.

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


## Working with avro files
