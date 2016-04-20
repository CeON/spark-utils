package pl.edu.icm.sparkutils.avro;

import java.io.IOException;
import java.io.Serializable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Preconditions;


/**
 * Loader of spark rdds from avro files
 * 
 * @author madryk
 *
 */
public class SparkAvroLoader implements Serializable {

    private static final long serialVersionUID = 1L;


    //------------------------ LOGIC --------------------------

    /**
     * Returns a java rdd filled with records of the specified type (avroRecordClass). The records are read from an avro datastore directory specified by
     * the avroDateStore path 
     */
    public <T extends GenericRecord> JavaRDD<T> loadJavaRDD(JavaSparkContext sc, String avroDatastorePath, Class<T> avroRecordClass) {
        Preconditions.checkNotNull(sc);
        Preconditions.checkNotNull(avroDatastorePath);
        Preconditions.checkNotNull(avroRecordClass);


        Schema schema = AvroUtils.toSchema(avroRecordClass.getName());
        Job job = getJob(schema);

        @SuppressWarnings("unchecked")
        JavaPairRDD<AvroKey<T>, NullWritable> inputRecords = (JavaPairRDD<AvroKey<T>, NullWritable>)
                sc.newAPIHadoopFile(avroDatastorePath, AvroKeyInputFormat.class, avroRecordClass, NullWritable.class, job.getConfiguration());
        
        

        // Hadoop's RecordReader reuses the same Writable object for all records
        // which may lead to undesired behavior when caching RDD.
        // Cloning records solves this problem.
        JavaRDD<T> input = inputRecords.map(tuple -> AvroUtils.cloneAvroRecord(tuple._1.datum()));

        return input;
    }


    //------------------------ PRIVATE --------------------------

    private Job getJob(Schema avroSchema) {

        Job job;

        try {
            job = Job.getInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        AvroJob.setInputKeySchema(job, avroSchema);

        return job;
    }
}
