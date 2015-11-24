package pl.edu.icm.sparkutils.avro;

import java.lang.reflect.Field;

import org.apache.avro.Schema;

/**
 * 
 * @author Mateusz Kobos
 *
 */
public class AvroUtils {
	public final static String primitiveTypePrefix = 
			"org.apache.avro.Schema.Type.";
	
	/**
	 * For a given name of a class generated from Avro schema return 
	 * a JSON schema.
	 * 
	 * Apart from a name of a class you can also give a name of one of enums
	 * defined in {@link org.apache.avro.Schema.Type}; in such case an
	 * appropriate primitive type will be returned.
	 * 
	 * @param typeName fully qualified name of a class generated from Avro schema, 
	 * e.g. {@code eu.dnetlib.iis.core.schemas.standardexamples.Person},
	 * or a fully qualified name of enum defined by 
	 * {@link org.apache.avro.Schema.Type},
	 * e.g. {@link org.apache.avro.Schema.Type.STRING}.
	 * @return JSON string
	 */
	public static Schema toSchema(String typeName) {
		Schema schema = null;
		if(typeName.startsWith(primitiveTypePrefix)){
			String shortName = typeName.substring(
					primitiveTypePrefix.length(), typeName.length());
			schema = getPrimitiveTypeSchema(shortName);
		} else {
			schema = getAvroClassSchema(typeName);
		}
		return schema;
	}
	
	private static Schema getPrimitiveTypeSchema(String shortName){
		Schema.Type type = Schema.Type.valueOf(shortName);
		Schema schema = Schema.create(type);
		return schema;
	}
	
	private static Schema getAvroClassSchema(String className){
		try {
			Class<?> avroClass = Class.forName(className);
			Field f = avroClass.getDeclaredField("SCHEMA$");
			Schema schema = (Schema) f.get(null);
			return schema;
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(
					"Class \""+className+"\" does not exist");
		} catch (SecurityException e) {
			throw new RuntimeException(e);
		} catch (NoSuchFieldException e) {
			throw new RuntimeException(e);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
    
    
    
   
    
}
