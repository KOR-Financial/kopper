/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.korfinancial.streaming.kopper.cast.registry.kafka;

import com.korfinancial.streaming.kopper.cast.UpcasterChainNode;
import com.korfinancial.streaming.kopper.cast.avro.DeclarativeAvroUpcaster;
import com.korfinancial.streaming.kopper.cast.sr.AvroUpcasterChain;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;

import com.korfinancial.streaming.kopper.cast.UpcasterChain;
import com.korfinancial.streaming.kopper.cast.registry.UpcasterRegistry;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

public class KafkaUpcasterRegistry implements UpcasterRegistry {

	public static final String STORE_NAME = "__upcasters";

	public static final String TOPIC = "_kopper_upcasters";

	private final SchemaRegistryClient schemaRegistryClient;

	private final ReadOnlyKeyValueStore<String, UpcasterChainState> store;

	private final Producer<String, UpcasterChainState> producer;

	public static void register(Topology topology) {
		topology.addGlobalStore(
				Stores.keyValueStoreBuilder(
					Stores.persistentKeyValueStore(STORE_NAME),
					Serdes.String(),
					Serdes.serdeFrom(
						new SpecificAvroSerializer<UpcasterChainState>(),
						new SpecificAvroDeserializer<UpcasterChainState>()
					)
				),
				STORE_NAME + "-source", new StringDeserializer(), new SpecificAvroDeserializer<>(), TOPIC,
				STORE_NAME + "-sync", StateProcessor::new);
	}

	public KafkaUpcasterRegistry(KafkaStreams kafkaStreams, SchemaRegistryClient schemaRegistryClient, Producer<String, UpcasterChainState> producer) {
		this.schemaRegistryClient = schemaRegistryClient;
		this.store = kafkaStreams
				.store(StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.keyValueStore()));
		this.producer = producer;
	}

	@Override
	public UpcasterChain<GenericRecord, Integer> getUpcasters(String subject) {
		UpcasterChainState chainState = this.store.get(subject);

		UpcasterChain.Builder<GenericRecord, Integer> builder = AvroUpcasterChain.builder(chainState.getId());

		for (UpcasterChainNodeState nodeState : chainState.getNodes()) {
			DeclarativeAvroUpcaster.Builder<Integer> nodeBuilder = DeclarativeAvroUpcaster.builder(
				schemaRegistryClient, nodeState.getSubject(), nodeState.getVersion()
			);

			nodeState.getExpressions().forEach(nodeBuilder::withExpression);

			builder.register(nodeBuilder.build());
		}

		return builder.build();
	}

	@Override
	public void registerChain(UpcasterChain<?, Integer> chain) {
		UpcasterChainState chainState = new UpcasterChainState(chain.getId(), new ArrayList<>());

		UpcasterChainNode<?, Integer> current = chain.getRoot();
		while (current != null) {
			if (! (current.getUpcaster() instanceof DeclarativeAvroUpcaster du)) {
				throw new IllegalStateException("only DeclarativeAvroUpcasters are allowed");
			}

			chainState.getNodes().add(new UpcasterChainNodeState(
				chain.getId(), current.getVersion(), du.getExpressions()
			));

			current = current.getNext();
		}

		try {
			this.producer.send(new ProducerRecord<>(TOPIC, chain.getId(), chainState)).get();
		}
		catch (ExecutionException | InterruptedException ex) {
			throw new RuntimeException(ex);
		}
	}

	static class StateProcessor implements Processor<String, UpcasterChainState, Void, Void> {
		private KeyValueStore<String, UpcasterChainState> store;

		@Override
		public void init(ProcessorContext<Void, Void> context) {
			Processor.super.init(context);
			store = context.getStateStore(STORE_NAME);
		}

		@Override
		public void process(Record<String, UpcasterChainState> record) {
			if (record.value() == null) {
				store.delete(record.key());
			} else {
				store.put(record.key(), record.value());
			}
		}
	}

}
