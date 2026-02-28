package com.example.iggy;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Shared Avro and Protobuf schema helpers for the Iggy format examples.
 *
 * <p>The Avro schema mirrors {@code resources/schemas/event.avsc} and the Protobuf
 * descriptor mirrors {@code resources/schemas/event.proto}.  Both are defined here
 * so that the producer/consumer example classes and their tests can import from a
 * single place without requiring a code-generation step.
 */
public final class Schemas {

    private Schemas() {}

    // -----------------------------------------------------------------------
    // Avro
    // -----------------------------------------------------------------------

    /** Avro schema string that mirrors {@code schemas/event.avsc}. */
    public static final String EVENT_AVRO_SCHEMA_JSON =
            "{"
            + "\"type\":\"record\","
            + "\"name\":\"Event\","
            + "\"namespace\":\"iggy.example\","
            + "\"fields\":["
            + "  {\"name\":\"id\",  \"type\":\"int\"},"
            + "  {\"name\":\"text\",\"type\":\"string\"},"
            + "  {\"name\":\"ts\",  \"type\":\"string\"}"
            + "]}";

    public static final Schema AVRO_SCHEMA =
            new Schema.Parser().parse(EVENT_AVRO_SCHEMA_JSON);

    /**
     * Serialize an event to schemaless Avro bytes.
     *
     * @param id   message id
     * @param text human-readable body
     * @param ts   ISO-8601 UTC timestamp
     * @return binary Avro encoding of the event
     */
    public static byte[] avroSerialize(int id, String text, String ts) throws IOException {
        GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
        record.put("id", id);
        record.put("text", text);
        record.put("ts", ts);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(AVRO_SCHEMA).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    /**
     * Deserialize schemaless Avro bytes back to a {@link GenericRecord}.
     *
     * @param data binary Avro bytes produced by {@link #avroSerialize}
     * @return the decoded record
     */
    public static GenericRecord avroDeserialize(byte[] data) throws IOException {
        BinaryDecoder decoder =
                DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(data), null);
        return new GenericDatumReader<GenericRecord>(AVRO_SCHEMA).read(null, decoder);
    }

    // -----------------------------------------------------------------------
    // Protobuf
    // -----------------------------------------------------------------------

    private static final Descriptors.Descriptor EVENT_DESCRIPTOR;

    static {
        try {
            DescriptorProtos.FileDescriptorProto fileProto =
                    DescriptorProtos.FileDescriptorProto.newBuilder()
                            .setName("iggy_example_event.proto")
                            .setPackage("iggy.example")
                            .setSyntax("proto3")
                            .addMessageType(
                                    DescriptorProtos.DescriptorProto.newBuilder()
                                            .setName("Event")
                                            .addField(field("id",   1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32))
                                            .addField(field("text", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                                            .addField(field("ts",   3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                                            .build())
                            .build();
            Descriptors.FileDescriptor fileDescriptor =
                    Descriptors.FileDescriptor.buildFrom(
                            fileProto, new Descriptors.FileDescriptor[0]);
            EVENT_DESCRIPTOR = fileDescriptor.findMessageTypeByName("Event");
        } catch (Descriptors.DescriptorValidationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static DescriptorProtos.FieldDescriptorProto field(
            String name, int number, DescriptorProtos.FieldDescriptorProto.Type type) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(type)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
    }

    /** Returns the descriptor for the {@code iggy.example.Event} Protobuf message. */
    public static Descriptors.Descriptor eventDescriptor() {
        return EVENT_DESCRIPTOR;
    }

    /**
     * Serialize an event to Protobuf wire-format bytes.
     *
     * @param id   message id
     * @param text human-readable body
     * @param ts   ISO-8601 UTC timestamp
     * @return Protobuf binary encoding of the event
     */
    public static byte[] protobufSerialize(int id, String text, String ts) {
        return DynamicMessage.newBuilder(EVENT_DESCRIPTOR)
                .setField(EVENT_DESCRIPTOR.findFieldByName("id"),   id)
                .setField(EVENT_DESCRIPTOR.findFieldByName("text"), text)
                .setField(EVENT_DESCRIPTOR.findFieldByName("ts"),   ts)
                .build()
                .toByteArray();
    }

    /**
     * Deserialize Protobuf bytes back to a {@link DynamicMessage}.
     *
     * @param data Protobuf wire-format bytes produced by {@link #protobufSerialize}
     * @return the decoded message
     */
    public static DynamicMessage protobufDeserialize(byte[] data)
            throws com.google.protobuf.InvalidProtocolBufferException {
        return DynamicMessage.parseFrom(EVENT_DESCRIPTOR, data);
    }
}
