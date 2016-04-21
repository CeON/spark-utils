package pl.edu.icm.sparkutils.avro;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import scala.Tuple2;


/**
 * @author Łukasz Dumiszewski
 */
@RunWith(MockitoJUnitRunner.class)
public class SparkAvroSaverTest {

    private SparkAvroSaver sparkAvroSaver = new SparkAvroSaver();
    
    @Captor
    private ArgumentCaptor<PairFunction<String, AvroKey<String>, NullWritable>> mapToPairFCaptor;
    
    @Captor
    private ArgumentCaptor<Configuration> capturedConfiguration;
    
    @Mock
    private JavaRDD<String> javaRDD;
    
    @Mock
    private JavaPairRDD<String, NullWritable> javaPairRDD;
    
    @Mock
    private Schema avroSchema;
    
    private String path = "/location";

    
    
    //------------------------ TESTS --------------------------
    
    

    @Test(expected=NullPointerException.class)
    public void saveDataFrame_NullDataFrame() throws Exception {
        
        // execute
        
        sparkAvroSaver.saveJavaRDD(null, avroSchema, path);
    }

    
    
    @Test(expected=IllegalArgumentException.class)
    public void saveJavaPairKeyRDD_EmptyPath() throws Exception {
        
        // execute
        
        sparkAvroSaver.saveJavaPairKeyRDD(javaPairRDD, avroSchema, null);
    }
    
    
    @Test(expected=NullPointerException.class)
    public void saveJavaPairRDD_NullSchema() throws Exception {
        
        // execute
        
        sparkAvroSaver.saveJavaPairKeyRDD(javaPairRDD, null, path);
    }
    

    @Test(expected=NullPointerException.class)
    public void saveJavaPairKeyRDD_NullJavaRDD() throws Exception {
        
        // execute
        
        sparkAvroSaver.saveJavaPairKeyRDD(null, avroSchema, path);
    }
    
    
    @Test
    public void saveJavaPairKeyRDD() {
        
        // given
        
        JavaPairRDD<?, ?> javaPairRDD = Mockito.mock(JavaPairRDD.class);
        
        
        // execute
        
        sparkAvroSaver.saveJavaPairKeyRDD(javaPairRDD, avroSchema, path);
        
        
        // assert
        
        verify(javaPairRDD).saveAsNewAPIHadoopFile(eq(path), eq(AvroKey.class), eq(NullWritable.class), eq(AvroKeyOutputFormat.class), capturedConfiguration.capture());
        assertEquals(avroSchema.toString(), capturedConfiguration.getValue().get("avro.schema.output.key"));
        
        
    }

    @Test(expected=IllegalArgumentException.class)
    public void saveJavaRDD_EmptyPath() throws Exception {
        
        // execute
        
        sparkAvroSaver.saveJavaRDD(javaRDD, avroSchema, " ");
    }
    
    
    @Test(expected=NullPointerException.class)
    public void saveJavaRDD_NullSchema() throws Exception {
        
        // execute
        
        sparkAvroSaver.saveJavaRDD(javaRDD, null, path);
    }
    

    @Test(expected=NullPointerException.class)
    public void saveJavaRDD_NullJavaRDD() throws Exception {
        
        // execute
        
        sparkAvroSaver.saveJavaRDD(null, avroSchema, path);
    }

    
    
    @Test
    public void saveJavaRDD() throws Exception {
        
        // given
        
        doReturn(javaPairRDD).when(javaRDD).mapToPair(Matchers.any());
        
        
        // execute
        
        sparkAvroSaver.saveJavaRDD(javaRDD, avroSchema, path);
        
        
        // assert
        
        verify(javaRDD).mapToPair(mapToPairFCaptor.capture());
        
        assertMapToPairFunction(mapToPairFCaptor.getValue());
        
        verify(javaPairRDD).saveAsNewAPIHadoopFile(eq(path), eq(AvroKey.class), eq(NullWritable.class), eq(AvroKeyOutputFormat.class), capturedConfiguration.capture());
        assertEquals(avroSchema.toString(), capturedConfiguration.getValue().get("avro.schema.output.key"));
         
        
    }
    
    
    //------------------------ PRIVATE --------------------------

    private void assertMapToPairFunction(PairFunction<String, AvroKey<String>, NullWritable> mapToPairFunction) throws Exception {
        Tuple2<AvroKey<String>, NullWritable> tuple = mapToPairFunction.call("xyz");
        assertEquals(new AvroKey<String>("xyz"), tuple._1());
        assertEquals(NullWritable.get(), tuple._2());
    }
}
