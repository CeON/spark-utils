package pl.edu.icm.sparkutils.avro;

import org.apache.avro.mapred.AvroKey;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.*;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class SparkAvroLoaderTestV2 {
    private static SparkSession spark;

    private Path workingDir;

    @BeforeClass
    public static void beforeClass() {
        SparkConf conf = new SparkConf();
        conf.setAppName(SparkAvroLoaderTestV2.class.getSimpleName());
        conf.setMaster("local");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        spark = SparkSession.builder().config(conf).getOrCreate();
    }

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory(SparkAvroLoaderTestV2.class.getSimpleName());
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
    }

    @AfterClass
    public static void afterClass() {
        spark.stop();
    }

    @Test
    public void loadJavaRDDWithJavaPairRDD() {
        // given
        Path path = workingDir.resolve("avro");

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        List<Tuple2<AvroKey<Country>, NullWritable>> data = Arrays.asList(
                new Tuple2<>(new AvroKey<>(new Country(1, "one", "PL1")), NullWritable.get()),
                new Tuple2<>(new AvroKey<>(new Country(2, "two", "PL2")), NullWritable.get()),
                new Tuple2<>(new AvroKey<>(new Country(3, "three", "PL3")), NullWritable.get())
        );
        JavaPairRDD<AvroKey<Country>, NullWritable> dataPairRDD = sc.parallelizePairs(data);
        SparkAvroSaver sparkAvroSaver = new SparkAvroSaver();
        sparkAvroSaver.saveJavaPairRDDKeys(dataPairRDD, Country.SCHEMA$, path.toString());

        // when
        List<Country> read = spark.read()
                .format("avro")
                .option("avroSchema", Country.SCHEMA$.toString())
                .load(path.toString())
                .map(rowToCountryFn(), Encoders.kryo(Country.class))
                .collectAsList();

        // then
        List<Country> sorted = read.stream().sorted(Comparator.comparing(Country::getId)).collect(Collectors.toList());
        assertEquals(3, sorted.size());
        assertEquals(data.get(0)._1().datum(), sorted.get(0));
        assertEquals(data.get(1)._1().datum(), sorted.get(1));
        assertEquals(data.get(2)._1().datum(), sorted.get(2));
    }

    @Test
    public void loadJavaRDDWithJavaRDD() {
        // given
        Path path = workingDir.resolve("avro");

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        List<Country> data = Arrays.asList(
                new Country(1, "one", "PL1"),
                new Country(2, "two", "PL2"),
                new Country(3, "three", "PL3")
        );
        JavaRDD<Country> dataRDD = sc.parallelize(data);
        SparkAvroSaver sparkAvroSaver = new SparkAvroSaver();
        sparkAvroSaver.saveJavaRDD(dataRDD, Country.SCHEMA$, path.toString());

        // when
        List<Country> read = spark.read()
                .format("avro")
                .option("avroSchema", Country.SCHEMA$.toString())
                .load(path.toString())
                .map(rowToCountryFn(), Encoders.kryo(Country.class))
                .collectAsList();

        // then
        List<Country> sorted = read.stream().sorted(Comparator.comparing(Country::getId)).collect(Collectors.toList());
        assertEquals(3, sorted.size());
        assertEquals(data.get(0), sorted.get(0));
        assertEquals(data.get(1), sorted.get(1));
        assertEquals(data.get(2), sorted.get(2));
    }

    private static MapFunction<Row, Country> rowToCountryFn() {
        return row -> Country.newBuilder()
                .setId(row.<Integer>getAs("id"))
                .setName(row.getAs("name"))
                .setIso(row.getAs("iso"))
                .build();
    }

}
