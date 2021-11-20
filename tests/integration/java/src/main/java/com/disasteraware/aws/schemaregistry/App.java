package com.disasteraware.aws.schemaregistry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

public class App {
    static AwsCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder()
            .profileName(System.getenv("AWS_PROFILE"))
            .build();

    static Map<String, Object> configs = new HashMap<>();

    public static void main(String[] args) {
        String dataFormat = Objects.requireNonNull(System.getenv("DATA_FORMAT"));
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, Objects.requireNonNull(System.getenv("AWS_REGION")));
        configs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, Objects.requireNonNull(System.getenv("REGISTRY_NAME")));
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, Objects.requireNonNull(System.getenv("SCHEMA_NAME")));
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);

        if (dataFormat.equals("AVRO")) {
            configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
            configs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
            try {
                byte[] bytes;
                GenericRecord record;
                Schema schema;

                bytes = System.in.readAllBytes();

                GlueSchemaRegistryKafkaDeserializer deserializer = new GlueSchemaRegistryKafkaDeserializer(configs);
                record = (GenericRecord) deserializer.deserialize("test", bytes);
                schema = record.getSchema();

                GlueSchemaRegistryKafkaSerializer serializer = new GlueSchemaRegistryKafkaSerializer(configs);
                bytes = serializer.serialize("test", record);

                System.out.write(bytes, 0, bytes.length);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        } else if (dataFormat.equals("JSON")) {
            configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.JSON.name());
                try {
                    byte[] bytes;
                    Object record;
                    Schema schema;

                    bytes = System.in.readAllBytes();

                    GlueSchemaRegistryKafkaDeserializer deserializer = new GlueSchemaRegistryKafkaDeserializer(configs);
                    record = deserializer.deserialize("test", bytes);

                    GlueSchemaRegistryKafkaSerializer serializer = new GlueSchemaRegistryKafkaSerializer(configs);
                    bytes = serializer.serialize("test", record);

                    System.out.write(bytes, 0, bytes.length);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
        } else {
            System.out.println("Only JSON or AVRO are acceptable data formats");
            System.exit(1);
        }
    }
}
