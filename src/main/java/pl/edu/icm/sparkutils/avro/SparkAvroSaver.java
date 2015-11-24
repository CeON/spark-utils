package pl.edu.icm.sparkutils.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

import com.google.common.base.Preconditions;



/**
 * 
 * Convenient saver of spark rdds to avro files.
 * 
 * @author Łukasz Dumiszewski
 */

public final class SparkAvroSaver {

    
    

    //------------------------ CONSTRUCTORS --------------------------
    
    private SparkAvroSaver() {
        throw new IllegalStateException("may not be instantiated");
    }

    
    
    //------------------------ LOGIC --------------------------


    /**
     * Saves the given javaRDD as avro data with the given schema in a directory or file defined by path.  
     */
    public static <T> void saveJavaRDD(JavaRDD<T> javaRDD, Schema avroSchema, String path) {
        Preconditions.checkNotNull(javaRDD);
        checkSchemaAndPath(avroSchema, path);
        
        JavaPairRDD<AvroKey<T>, NullWritable> javaPairRDD = javaRDD.mapToPair(r->new Tuple2<AvroKey<T>, NullWritable>(new AvroKey<T>(r), NullWritable.get()));
        
        saveJavaPairKeyRDD(javaPairRDD, avroSchema, path);
    
    }
    
    
    /**
     * Saves the keys from the given javaPairRDD as avro data with the given schema in a directory or file defined by path.  
     */
    public static <K, V> void saveJavaPairKeyRDD(JavaPairRDD<K, V> javaPairRDD, Schema avroSchema, String path) {
        Preconditions.checkNotNull(javaPairRDD);
        checkSchemaAndPath(avroSchema, path);
        
        Job job = getJob(avroSchema);
        
        javaPairRDD.saveAsNewAPIHadoopFile(path, AvroKey.class, NullWritable.class, AvroKeyOutputFormat.class, job.getConfiguration());
    
    }
    
    

    
    //------------------------ PRIVATE --------------------------
    
    private static Job getJob(Schema avroSchema) {
        
        Job job;
        
        try {
            job = Job.getInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        AvroJob.setOutputKeySchema(job, avroSchema);

        return job;
    }
    
    private static void checkSchemaAndPath(Schema avroSchema, String path) {
        Preconditions.checkNotNull(avroSchema);
        Preconditions.checkArgument(StringUtils.isNotBlank(path));
    }


    
}
