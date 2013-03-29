/*
 * Copyright 2012 Russell Jurney
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

/**
 * Modifications: By Russell Melick
 * Modified Constructor to accept parameter to drop fields with errors.
 * Converted static methods to instance methods for ease of using _dropFieldsWithErrors
 * Added map
 */

package com.hortonworks.pig.udf;

import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
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
            Object jsonObject = fieldToJson(field, schema.getFields().get(0));
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
    protected JSONObject tupleToJson(Tuple t, FieldSchema tupleFieldSchema) throws ExecException {
        JSONObject json = new JSONObject();
        List<FieldSchema> fieldSchemas = tupleFieldSchema.schema.getFields();
        for (int i=0; i<t.size(); i++) {
            Object field = t.get(i);
            FieldSchema fieldSchema = fieldSchemas.get(i);

            // sometimes we want to skip fields with errors, instead of failing everything
            try {
                json.put(fieldSchema.alias, fieldToJson(field, fieldSchema));
            }
            catch (ExecException e) {
                if (_dropFieldsWithErrors) {
                    LOG.error("Skipping bad field. Schema: " + fieldSchema + " Value: " + field, e);
                }
                else {
                    throw e;
                }
            }

        }
        return json;
    }

    /**
     * Convert a Pig Bag to a JSON array
     */
    @SuppressWarnings("unchecked")
    private JSONArray bagToJson(DataBag bag, FieldSchema bagFieldSchema) throws ExecException {
        JSONArray array = new JSONArray();
        FieldSchema tupleSchema = null;
        try {
            tupleSchema = bagFieldSchema.schema.getField(0);
        }
        catch (Exception e) {
            LOG.error("Unable to parse schema: " + bagFieldSchema);
            e.printStackTrace();
        }

        for (Tuple t : bag) {
            JSONObject recJson = tupleToJson(t, tupleSchema);
            array.add(recJson);
        }
        return array;
    }

    /**
     * Convert a map to Json (only supports single level map of simple types)
     */
    private JSONObject mapToJson(Map<String, Object> map, FieldSchema mapFieldSchema)
    {
      LOG.warn("Maps are not fully supported yet. It only supports a single level of simple types.");
      JSONObject json = new JSONObject();
      for (Map.Entry<String, Object> entry : map.entrySet())
      {
        json.put(entry.getKey(), entry.getValue().toString());
      }
      return json;
    }

    /**
     * Find the type of a field and convert it to JSON as required.
     */
    private Object fieldToJson(Object value, FieldSchema fieldSchema) throws ExecException {
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
                return tupleToJson((Tuple)value, fieldSchema);

            case DataType.BAG:
                return bagToJson(DataType.toBag((DataBag)value), fieldSchema);

            case DataType.MAP:
                return mapToJson(DataType.toMap(value), fieldSchema);

            case DataType.BYTE:
                throw new ExecException("Byte type is not currently supported with JsonStorage");

            case DataType.BYTEARRAY:
                throw new ExecException("ByteArray type is not currently supported with JsonStorage");

            default:
                throw new ExecException("Unknown type: " + DataType.findType(value) + " is not currently supported with JsonStorage");
        }
    }
}
