/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.avro;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public final class Utils {

	private Utils() {
	}

	public static AvroSchema registerSchema(SchemaRegistryClient schemaRegistryClient, String subject)
			throws RestClientException, IOException {
		return registerSchema(schemaRegistryClient, subject, Map.of());
	}

	public static AvroSchema registerSchema(SchemaRegistryClient schemaRegistryClient, String subject,
			Map<String, AvroSchema> referenceMap) throws RestClientException, IOException {
		Map<String, String> resolvedReferences = new HashMap<>();
		List<SchemaReference> references = new ArrayList<>();
		referenceMap.forEach((k, v) -> {
			resolvedReferences.put(k, v.canonicalString());
			references.add(new SchemaReference(k, v.name(), v.version()));
		});

		// -- construct the schema from the schema file, including references
		String content = createStringFromFile("/avro/" + subject + ".avsc");
		AvroSchema schema = new AvroSchema(content, references, resolvedReferences, -1);

		// -- register the schema
		schemaRegistryClient.register(subject + "-value", schema);

		return schema;
	}

	public static AvroSchema registerSchema(SchemaRegistryClient schemaRegistryClient, String subject,
			AvroSchema schema) throws RestClientException, IOException {
		int schemaId = schemaRegistryClient.register(subject, schema);
		return (AvroSchema) schemaRegistryClient.getSchemaById(schemaId);
	}

	public static SchemaReference asReference(String name, AvroSchema schema) {
		return new SchemaReference(name, schema.name(), schema.version());
	}

	public static String createStringFromFile(String path) {
		if (path.startsWith("/")) {
			path = path.substring(1);
		}

		try (InputStream is = Utils.class.getClassLoader().getResourceAsStream(path)) {
			return new String(is.readAllBytes(), StandardCharsets.UTF_8);
		}
		catch (Exception ex) {
			throw new IllegalArgumentException("Failed to load file " + path);
		}
	}

	public static AvroSchema schemaFromFile(String path) {
		return schemaFromFile(path, Map.of());
	}

	public static AvroSchema schemaFromFile(String path, Map<SchemaReference, String> references) {
		return schemaFromFile(path, 1, references);
	}

	public static AvroSchema schemaFromFile(String path, int version, Map<SchemaReference, String> references) {
		Map<String, String> referenceMapping = new HashMap<>();
		references.forEach((schemaReference, schema) -> referenceMapping.put(schemaReference.getSubject(), schema));

		return new AvroSchema(createStringFromFile(path), references.keySet().stream().toList(), referenceMapping,
				version);
	}

}
