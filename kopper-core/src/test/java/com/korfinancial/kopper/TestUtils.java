/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.kopper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import com.korfinancial.kopper.dyre.DynamicRecord;
import com.korfinancial.kopper.serde.KopperSerdes;

public final class TestUtils {

	private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);

	private TestUtils() {
	}

	static {
		Locale.setDefault(new Locale("en")); // impacts the country code tests
	}

	public static Map<String, Object> serdeTestConfig() {
		return Map.of("schema.registry.url", "mock://default");
	}

	public static <T extends DynamicRecord> KafkaTemplate<String, T> createProducer(Map<String, Object> producerConfig,
			Class<T> cls, SchemaRegistryClient schemaRegistryClient) {
		ProducerFactory<String, T> pf = new DefaultKafkaProducerFactory<>(producerConfig, Serdes.String().serializer(),
				KopperSerdes.dynamicSerde(cls, schemaRegistryClient).serializer());

		return new KafkaTemplate<>(pf);
	}

	public static String createStringFromFile(String path) {
		try (InputStream is = TestUtils.class.getClassLoader().getResourceAsStream(path)) {
			return new String(is.readAllBytes(), StandardCharsets.UTF_8);
		}
		catch (Exception ex) {
			throw new IllegalArgumentException("Failed to load file " + path);
		}
	}

	public static void registerSchemas(SchemaRegistryClient client, String topic, Schema... schemas) throws Exception {
		for (Schema s : schemas) {
			client.register(String.format("%s-%s", topic, s.getFullName()), new AvroSchema(s));
		}
	}

	public static void registerSchemas(SchemaRegistryClient client, String topic, String... schemaFiles)
			throws Exception {
		Set<String> uniqueValues = new HashSet<>();
		for (String s : schemaFiles) {
			if (uniqueValues.add(s)) {
				AvroSchema avroSchema = new AvroSchema(createStringFromFile(s));
				client.register(String.format("%s-%s", topic, avroSchema.name()), avroSchema);
			}
		}
	}

	public static void registerSchema(SchemaRegistryClient client, Class<?> cls, String schemaFile) throws Exception {

		AvroSchema avroSchema = new AvroSchema(createStringFromFile(schemaFile));
		client.register(cls.getName(), avroSchema);
	}

	public static void registerSchema(SchemaRegistryClient client, Class<?> cls, Schema schema) throws Exception {
		try {
			AvroSchema avroSchema = new AvroSchema(schema);
			client.register(cls.getName(), avroSchema);
		}
		catch (Exception ex) {
			logger.error("unable to register schema " + schema.getFullName(), ex);
			throw ex;
		}
	}

	public static GenericRecordBuilder newGenericRecord(SchemaRegistryClient client, String subject)
			throws RestClientException, IOException {
		SchemaMetadata sm = client.getLatestSchemaMetadata(subject);
		ParsedSchema ps = client.getSchemaById(sm.getId());

		return new GenericRecordBuilder((Schema) ps.rawSchema());
	}

	public static <T> T last(Iterator<T> iter) {
		T last = null;
		while (iter.hasNext()) {
			last = iter.next();
		}
		return last;
	}

	public static <T extends DynamicRecord> Consumer<String, T> createConsumer(EmbeddedKafkaBroker embeddedKafkaBroker,
			Serde<T> serde, String topic) {
		ConsumerFactory<String, T> cf = new DefaultKafkaConsumerFactory<>(
				MessagingConfig.consumerConfig(embeddedKafkaBroker), new StringDeserializer(), serde.deserializer());

		Consumer<String, T> clientConsumer = cf.createConsumer();
		embeddedKafkaBroker.consumeFromAnEmbeddedTopic(clientConsumer, topic);
		return clientConsumer;
	}

}
