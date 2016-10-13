package pl.edu.icm.sparkutils.avro;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.avro.generic.GenericData;
import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;

/**
 * Registrator for kryo serializer. It adds support for avro generated classes. <br/><br/>
 * Based on the solution provided by Nicolas PHUNG in https://issues.apache.org/jira/browse/SPARK-3601
 * 
 * @author madryk
 * 
 * 
 */
public class AvroCompatibleKryoRegistrator implements KryoRegistrator {

	@SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
	public void registerClasses(Kryo kryo) {
		kryo.register(GenericData.Array.class, new SpecificInstanceCollectionSerializer(ArrayList.class));
	}


	/**
	 * Special serializer for Java collections enforcing certain instance types.
	 * Avro is serializing collections with an "GenericData.Array" type. Kryo is not able to handle
	 * this type, so we use ArrayLists.
	 */
	public static class SpecificInstanceCollectionSerializer<T extends java.util.ArrayList<?>> extends CollectionSerializer implements Serializable {
		
	    private static final long serialVersionUID = 1L;
		
	    private Class<T> type;

		public SpecificInstanceCollectionSerializer(Class<T> type) {
			this.type = type;
		}

		@SuppressWarnings("rawtypes")
        @Override
		protected Collection create(Kryo kryo, Input input, Class<Collection> type) {
			return kryo.newInstance(this.type);
		}

		@SuppressWarnings("rawtypes")
        @Override
		protected Collection createCopy(Kryo kryo, Collection original) {
			return kryo.newInstance(this.type);
		}
	}
}
