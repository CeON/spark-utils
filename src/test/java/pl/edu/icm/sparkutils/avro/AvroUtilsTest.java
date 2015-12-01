package pl.edu.icm.sparkutils.avro;

import static org.junit.Assert.assertEquals;

import org.apache.avro.Schema;
import org.junit.Test;

/**
 * @author madryk
 */
public class AvroUtilsTest {

    @Test
    public void toSchema() {
        
        // execute
        Schema schema = AvroUtils.toSchema("pl.edu.icm.sparkutils.avro.Country");
        
        // assert
        assertEquals(Country.SCHEMA$, schema);
    }
    
    @Test
    public void cloneAvroRecord() {
        
        // given
        Country country = Country.newBuilder()
                .setId(3)
                .setIso("PL")
                .setName("Poland")
                .build();
        
        // execute
        Country retCountry = AvroUtils.cloneAvroRecord(country);
        
        // assert
        assertEquals(country, retCountry);
    }
}
