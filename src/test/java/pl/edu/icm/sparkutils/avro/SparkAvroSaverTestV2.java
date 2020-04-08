package pl.edu.icm.sparkutils.avro;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;
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

public class SparkAvroSaverTestV2 {
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
        workingDir = Files.createTempDirectory(SparkAvroSaverTestV2.class.getSimpleName());
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
    public void saveJavaRDD() {
        // given
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        Path path = workingDir.resolve("avro");
        List<Country> data = Arrays.asList(
                new Country(1, "one", "PL1"),
                new Country(2, "two", "PL2"),
                new Country(3, "three", "PL3")
        );
        JavaRDD<Country> dataRDD = sc.parallelize(data);
        Dataset<Row> df = spark.createDataFrame(dataRDD.map(countryToRowFn()),
                (StructType) SchemaConverters.toSqlType(Country.SCHEMA$).dataType());

        // when
        df
                .write()
                .format("avro")
                .option("avroSchema", Country.SCHEMA$.toString())
                .save(path.toString());

        // then
        SparkAvroLoader sparkAvroLoader = new SparkAvroLoader();
        List<Country> sorted = sparkAvroLoader.loadJavaRDD(sc, path.toString(), Country.class).collect().stream()
                .sorted(Comparator.comparing(Country::getId))
                .collect(Collectors.toList());
        assertEquals(data.get(0), sorted.get(0));
        assertEquals(data.get(1), sorted.get(1));
        assertEquals(data.get(2), sorted.get(2));
    }

    private static Function<Country, Row> countryToRowFn() {
        return country -> RowFactory.create(country.getId(), country.getName(), country.getIso());
    }

    @Test
    public void saveJavaPairRDDKeys() {
        // given
        Path path = workingDir.resolve("avro");

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        List<Tuple2<Country, NullWritable>> data = Arrays.asList(
                new Tuple2<>(new Country(1, "one", "PL1"), NullWritable.get()),
                new Tuple2<>(new Country(2, "two", "PL2"), NullWritable.get()),
                new Tuple2<>(new Country(3, "three", "PL3"), NullWritable.get())
        );
        JavaPairRDD<Country, NullWritable> dataPairRDD = sc.parallelizePairs(data);
        Dataset<Row> df = spark.createDataFrame(dataPairRDD.map(countryAndNullWritableToRowFn()),
                (StructType) SchemaConverters.toSqlType(Country.SCHEMA$).dataType());

        // when
        df
                .write()
                .format("avro")
                .option("avroSchema", Country.SCHEMA$.toString())
                .save(path.toString());

        // then
        SparkAvroLoader sparkAvroLoader = new SparkAvroLoader();
        List<Country> sorted = sparkAvroLoader.loadJavaRDD(sc, path.toString(), Country.class).collect().stream()
                .sorted(Comparator.comparing(Country::getId))
                .collect(Collectors.toList());
        assertEquals(data.get(0)._1(), sorted.get(0));
        assertEquals(data.get(1)._1(), sorted.get(1));
        assertEquals(data.get(2)._1(), sorted.get(2));
    }

    private static Function<Tuple2<Country, NullWritable>, Row> countryAndNullWritableToRowFn() {
        return pair -> RowFactory.create(pair._1().getId(), pair._1().getName(), pair._1().getIso());
    }

}
