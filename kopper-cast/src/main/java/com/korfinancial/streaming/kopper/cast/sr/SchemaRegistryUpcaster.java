/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.sr;

import java.io.IOException;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import com.korfinancial.streaming.kopper.cast.Upcaster;

public abstract class SchemaRegistryUpcaster<O> implements Upcaster<O, Integer> {

	private final String subject;

	private final Integer targetVersion;

	private final ParsedSchema schema;

	private final SchemaRegistryClient schemaRegistryClient;

	public SchemaRegistryUpcaster(SchemaRegistryClient schemaRegistryClient, String subject, Integer version) {
		this.schemaRegistryClient = schemaRegistryClient;
		this.subject = subject;
		this.targetVersion = version;

		Schema s = schemaRegistryClient.getByVersion(subject, version, true);
		if (s == null) {
			throw new IllegalArgumentException(
					"unable to find a schema for subject " + subject + " version " + version);
		}

		try {
			this.schema = schemaRegistryClient.getSchemaById(s.getId());
		}
		catch (IOException | RestClientException ex) {
			throw new RuntimeException("unable to retrieve schema for subject " + subject + " version " + version, ex);
		}
	}

	@Override
	public Integer getTargetVersion() {
		return targetVersion;
	}

	public ParsedSchema getSchema() {
		return schema;
	}

	public String getSubject() {
		return subject;
	}

	public SchemaRegistryClient getSchemaRegistryClient() {
		return schemaRegistryClient;
	}

	public abstract static class Builder<O> {

		protected final SchemaRegistryClient schemaRegistryClient;

		protected final String subject;

		protected final Integer version;

		Builder(SchemaRegistryClient schemaRegistryClient, String subject, Integer version) {
			this.schemaRegistryClient = schemaRegistryClient;
			this.subject = subject;
			this.version = version;
		}

		public abstract SchemaRegistryUpcaster<O> build();

	}

}
