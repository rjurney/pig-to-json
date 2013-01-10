package com.hortonworks.pig.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.builtin.LOG;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.EvalFunc;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class ToJson extends EvalFunc<String> {

    private static final Log LOG = LogFactory.getLog(ToJson.class);

    private final boolean _dropFieldsWithErrors;

    Properties myProperties = null;

    public ToJson()
    {
      this("false");
    }

    public ToJson(String dropFieldsWithErrors)
    {
        if ("true".equals(dropFieldsWithErrors) || "dropFieldsWithErrors".equals(dropFieldsWithErrors)) {
            _dropFieldsWithErrors = true;
        }
        else {
            _dropFieldsWithErrors = false;
        }
    }

    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;

        if (myProperties == null) {
            // Retrieve our class specific properties from UDFContext
            myProperties = UDFContext.getUDFContext().getUDFProperties(this.getClass());
        }

        String strSchema = myProperties.getProperty("horton.json.udf.schema");

        // You must set the schema or we cannot convert to JSON - duh!
        if (strSchema == null) {
            throw new IOException("Could not find schema in UDF context");
        }

        Schema schema = null;
        try {
            schema = Utils.getSchemaFromString(strSchema);
        }
        catch(Exception e) {
            LOG.error("Unable to parse schema: " + strSchema);
            e.printStackTrace();
        }

        try {
            // Parse the schema from the string stored in the properties object.
            Object field = input.get(0);
            Object jsonObject = fieldToJson(field, schema.getFields().get(0), _dropFieldsWithErrors);
            String json = jsonObject.toString();
            return json;
        }
        catch(Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }
    }

    public Schema outputSchema(Schema inputSchema) {

        // We must strip the outer {} or this schema won't parse on the back-end
        String schemaString = inputSchema.toString().substring(1, inputSchema.toString().length() - 1);

        // Set the input schema for processing
        UDFContext context = UDFContext.getUDFContext();
        Properties udfProp = context.getUDFProperties(this.getClass());

        udfProp.setProperty("horton.json.udf.schema", schemaString);

        // Construct our output schema which is one field, that is a chararray
        return new Schema(new FieldSchema(null, DataType.CHARARRAY));
    }

    /**
     * Convert a Pig Tuple into a JSON object
     */
    @SuppressWarnings("unchecked")
    protected static JSONObject tupleToJson(Tuple t, FieldSchema tupleFieldSchema, boolean dropFieldsWithErrors) throws ExecException {
        JSONObject json = new JSONObject();
        List<FieldSchema> fieldSchemas = tupleFieldSchema.schema.getFields();
        for (int i=0; i<t.size(); i++) {
            Object field = t.get(i);
            FieldSchema fieldSchema = fieldSchemas.get(i);

            // sometimes we want to skip fields with errors, instead of failing everything
            if (dropFieldsWithErrors) {
                try {
                    json.put(fieldSchema.alias, fieldToJson(field, fieldSchema, dropFieldsWithErrors));
                }
                catch (ExecException e) {
                  LOG.error("Skipping bad field. Schema: " + fieldSchema + " Value: " + field, e);
                }
            }
            else {
                json.put(fieldSchema.alias, fieldToJson(field, fieldSchema, dropFieldsWithErrors));
            }
        }
        return json;
    }

    /**
     * Convert a Pig Bag to a JSON array
     */
    @SuppressWarnings("unchecked")
    private static JSONArray bagToJson(DataBag bag, FieldSchema bagFieldSchema, boolean dropFieldsWithErrors) throws ExecException {
        JSONArray array = new JSONArray();
        FieldSchema tupleSchema = null;
        try {
            tupleSchema = bagFieldSchema.schema.getField(0);
        }
        catch (Exception e) {
            LOG.error("Unable to parse schema: " + bagFieldSchema);
            e.printStackTrace();
        }

        Iterator<Tuple> bagIterator = bag.iterator();
        while(bagIterator.hasNext()) {
            Tuple t = bagIterator.next();
            JSONObject recJson = tupleToJson(t, tupleSchema, dropFieldsWithErrors);
            array.add(recJson);
        }
        return array;
    }

    /**
     * Find the type of a field and convert it to JSON as required.
     */
    private static Object fieldToJson(Object value, FieldSchema fieldSchema, boolean dropFieldsWithErrors) throws ExecException {
        switch (DataType.findType(value)) {
            // Native types that don't need converting
            case DataType.NULL:
            case DataType.INTEGER:
            case DataType.BOOLEAN:
            case DataType.LONG:
            case DataType.FLOAT:
            case DataType.DOUBLE:
            case DataType.CHARARRAY:
                return value;

            case DataType.TUPLE:
                return tupleToJson((Tuple)value, fieldSchema, dropFieldsWithErrors);

            case DataType.BAG:
                return bagToJson(DataType.toBag((DataBag)value), fieldSchema, dropFieldsWithErrors);

            case DataType.MAP:
                throw new ExecException("Map type is not currently supported with JsonStorage");

            case DataType.BYTE:
                throw new ExecException("Byte type is not currently supported with JsonStorage");

            case DataType.BYTEARRAY:
                throw new ExecException("ByteArray type is not currently supported with JsonStorage");

            default:
                throw new ExecException("Unknown type is not currently supported with JsonStorage");
        }
    }
}
