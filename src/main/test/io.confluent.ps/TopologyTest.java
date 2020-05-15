package io.confluent.ps;

import com.google.common.collect.Maps;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.*;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static org.apache.kafka.common.utils.Utils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class TopologyTest {

    private TestInputTopic<String, String> inputTopicChildOne;
    private TestInputTopic<String, String> inputTopicChildTwo;
    private TestInputTopic<String, String> inputTopicFinal;
    private TestOutputTopic<String, String> outputTopic;
    private TestInputTopic<String, String> triggerTopic;
    private TopologyTestDriver testDriver;

    private static final String FINAL_STORE = "FINAL_STORE";
    private static final String SUPPRESSION_STORE = "suppression_store";

    private int SUPPRESSION_CHECK_FREQUENCY = 10;
    public static final Duration suppressionWindowTime = Duration.ofMillis(500);


    public static final Properties config = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "TestTopicsTest-" + Math.random()),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once"),
            mkEntry(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://sr"),
            mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName()),
            mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName()),
            mkEntry(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")) // disable for debugging
    );

    @Before
    public void setUp() {

        //change this to run different toplogies availabe below
        StreamsBuilder builder = createTopologyWithWallClockSuppression();

        testDriver = new TopologyTestDriver(builder.build(), config);

        inputTopicChildOne = testDriver.createInputTopic("input-topic-child1", Serdes.String().serializer(), Serdes.String().serializer());
        inputTopicChildTwo = testDriver.createInputTopic("input-topic-child2", Serdes.String().serializer(), Serdes.String().serializer());

        inputTopicFinal = testDriver.createInputTopic("input-topic-final", Serdes.String().serializer(), Serdes.String().serializer());

        triggerTopic = testDriver.createInputTopic("trigger-topic", Serdes.String().serializer(), Serdes.String().serializer());
        
        outputTopic = testDriver.createOutputTopic("output-topic", Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @Test
    public void test_ktable_correct(){

        inputTopicFinal.pipeInput("a", "final: ");

        inputTopicChildOne.pipeInput("a","b");
        inputTopicChildOne.pipeInput("a","b");
        inputTopicChildTwo.pipeInput("a","b");

        inputTopicChildTwo.pipeInput("a","c");
        inputTopicChildTwo.pipeInput("a","c");
        inputTopicChildTwo.pipeInput("a","c");

        //send trigger event
        //commented out as it is not required for time based suppression
        // triggerTopic.pipeInput("a", "GO");

        Duration waitUntil = suppressionWindowTime.plus(ofMillis(500));

        //wait for a record to be emitted
        await().during(waitUntil)
                .conditionEvaluationListener(condition ->  System.out.println(String.format("Still no message emitted... (elapsed:%dms, remaining:%dms)", condition.getElapsedTimeInMS(), condition.getRemainingTimeInMS())))
                .until(() -> outputTopic.readValuesToList().size() == 0);

        // move time forward
        long twoMinutesMs = ofMinutes(2).toMillis();
        long now = System.currentTimeMillis();
        testDriver.advanceWallClockTime(now + twoMinutesMs);

        List<KeyValue<String, String>> keyValues = outputTopic.readKeyValuesToList();

        KeyValue<String, String> stringStringKeyValue = keyValues.stream().findFirst().get();
        System.out.format("Key: %s, Value: %s", stringStringKeyValue.key, stringStringKeyValue.value);

        // check that a single record was emitted
        assertThat(keyValues).hasSize(1);
        KeyValue<String, String> actual = keyValues.stream().findFirst().get();

        assertThat(actual.value).isEqualTo("final: bbbccc");
    }

    private StreamsBuilder createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> childStream1 = builder.stream("input-topic-child1", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> child1table = groupAndAccumulate(childStream1, "input-topic-child1-store");

        KStream<String, String> childStream2 = builder.stream("input-topic-child2", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> child2table = groupAndAccumulate(childStream2, "input-topic-child2-store");

        //Join of child tables
        KTable<String, String> joinedChildrenTable = child1table.join(child2table,
                (leftValue, rightValue) -> leftValue + rightValue /* ValueJoiner */
        );

        //Final event
        KStream<String, String> parentEventStream = builder.stream("input-topic-final", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> parentEventTable = groupAndAccumulate(parentEventStream, "input-topic-final");

        KTable<String, String> finalJoin = parentEventTable.join(joinedChildrenTable,
                (leftValue, rightValue) -> leftValue + rightValue, Materialized.as(FINAL_STORE).with(Serdes.String(), Serdes.String())
        );

        finalJoin.toStream().to("output-topic", Produced.with(Serdes.String(), Serdes.String()));
        return builder;
    }

    private StreamsBuilder createTopologyWithTrigger() {
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> triggerStream = builder.stream("trigger-topic", Consumed.with(Serdes.String(), Serdes.String()));

        final KStream<String, String> childStream1 = builder.stream("input-topic-child1", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> child1table = groupAndAccumulate(childStream1, "input-topic-child1-store");

        KStream<String, String> childStream2 = builder.stream("input-topic-child2", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> child2table = groupAndAccumulate(childStream2, "input-topic-child2-store");

        //Join of child tables
        KTable<String, String> joinedChildrenTable = child1table.join(child2table,
                (leftValue, rightValue) -> leftValue + rightValue /* ValueJoiner */
        );

        //Final event
        KStream<String, String> finalDocStream = builder.stream("input-topic-final", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> finalDocTable = finalDocStream.toTable();

        //Creates state store that is used by transformer after trigger
        finalDocTable.join(joinedChildrenTable,
                (leftValue, rightValue) -> leftValue + rightValue, Materialized.as(FINAL_STORE)
        );

        //Suppress until trigger event
        KStream<String, String> filteredKTable = triggerStream.transformValues(() -> new ValueTransformerWithKey<String, String, String>() {

            private ProcessorContext context;
            private TimestampedKeyValueStore<String, String> finalJoinStore;

            @Override
            public void init(ProcessorContext contextParam) {
                this.finalJoinStore = (TimestampedKeyValueStore<String, String>) contextParam.getStateStore(FINAL_STORE);
            }

            @Override
            public String transform(String key, String value) {
                if (value.equals("GO")) {
                    ValueAndTimestamp<String> finalState = finalJoinStore.get(key);
                    return finalState.value();
                }
                return null; // don't emit here, as we didn't get trigger yet
            }

            @Override
            public void close() {
                finalJoinStore.close();
            }
        }, FINAL_STORE);

        filteredKTable.to("output-topic", Produced.with(Serdes.String(), Serdes.String()));
        return builder;
    }

    //same as above but using merge operator in order to reduce the number of intermediate stat stores
    private StreamsBuilder createTopologyWithTriggerUsingMerge() {
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> triggerStream = builder.stream("trigger-topic", Consumed.with(Serdes.String(), Serdes.String()));

        final KStream<String, String> childStream1 = builder.stream("input-topic-child1", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> childStream2 = builder.stream("input-topic-child2", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> mergedChildrenStream = childStream1.merge(childStream2, Named.as("merged-children-stream"));

        KTable accumulatedChildrenTable = groupAndAccumulate(mergedChildrenStream, "accumulated-children-store");

        //Final event
        KStream<String, String> finalDocStream = builder.stream("input-topic-final", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> finalDocTable = finalDocStream.toTable();

        //Creates state store that is used by transformer after trigger
        finalDocTable.join(accumulatedChildrenTable,
                (leftValue, rightValue) -> leftValue + rightValue, Materialized.as(FINAL_STORE)
        );

        //Suppress until trigger event
        KStream<String, String> filteredKTable = triggerStream.transformValues(() -> new ValueTransformerWithKey<String, String, String>() {

            private ProcessorContext context;
            private TimestampedKeyValueStore<String, String> finalJoinStore;

            @Override
            public void init(ProcessorContext contextParam) {
                this.finalJoinStore = (TimestampedKeyValueStore<String, String>) contextParam.getStateStore(FINAL_STORE);
            }

            @Override
            public String transform(String key, String value) {
                if (value.equals("GO")) {
                    //accessing KTable created previously
                    ValueAndTimestamp<String> finalState = finalJoinStore.get(key);
                    return finalState.value();
                }
                return null; // we only emit under above trigger condition (nothing else should come on this stream)
            }

            @Override
            public void close() {
                finalJoinStore.close();
            }
        }, FINAL_STORE);

        filteredKTable.to("output-topic", Produced.with(Serdes.String(), Serdes.String()));
        return builder;
    }

    private StreamsBuilder createTopologyWithWallClockSuppression() {
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> triggerStream = builder.stream("trigger-topic", Consumed.with(Serdes.String(), Serdes.String()));

        final KStream<String, String> childStream1 = builder.stream("input-topic-child1", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> childStream2 = builder.stream("input-topic-child2", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> mergedChildrenStream = childStream1.merge(childStream2, Named.as("merged-children-stream"));

        KTable accumulatedChildrenTable = groupAndAccumulate(mergedChildrenStream, "accumulated-children-store");

        //Final event
        KStream<String, String> finalDocStream = builder.stream("input-topic-final", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> finalDocTable = finalDocStream.toTable();

        //Creates state store that is used by transformer after trigger
        KTable<String, String> finalKTable = finalDocTable.join(accumulatedChildrenTable,
                (leftValue, rightValue) -> leftValue + rightValue, Materialized.as(FINAL_STORE)
        );

        KStream<String, String> finalKStream = finalKTable.toStream();

        //add state store to the topology
        StoreBuilder<KeyValueStore<Long, String>> suppressionStoreBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(SUPPRESSION_STORE), Serdes.Long(), Serdes.String()).withLoggingEnabled(Maps.newHashMap());
        builder.addStateStore(suppressionStoreBuilder);

        //Suppress until trigger event
        KStream<String, String> suppressedStream = finalKStream.transform(() -> new Transformer<String, String, KeyValue<String, String>>() {

            private TimestampedKeyValueStore<String, String> finalJoinStore;
            private KeyValueStore<Long, String> suppressionStore;

            private Duration SUPPRESSION_TIME = suppressionWindowTime;

            @Override
            public void init(ProcessorContext contextParam) {
                this.finalJoinStore = (TimestampedKeyValueStore<String, String>) contextParam.getStateStore(FINAL_STORE);
                this.suppressionStore = (KeyValueStore<Long, String>) contextParam.getStateStore(SUPPRESSION_STORE);

                contextParam.schedule(SUPPRESSION_CHECK_FREQUENCY, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                    long expireTime = System.currentTimeMillis() - SUPPRESSION_TIME.toMillis();
                    KeyValueIterator<Long, String> expiredEntries = suppressionStore.range(0l, expireTime); // ready for emission

                    HashSet<String> uniqKeys = new HashSet<>();
                    expiredEntries.forEachRemaining(entry -> {
                        uniqKeys.add(entry.value);
                        suppressionStore.delete(entry.key);
                    });

                    uniqKeys.forEach(key -> {
                        ValueAndTimestamp<String> latestValue = finalJoinStore.get(key);
                        contextParam.forward(key, latestValue.value());
                    });
                });
            }

            @Override
            public KeyValue<String,String> transform(String key, String value) {
                // update the last seen time stamp for suppression
                long now = System.currentTimeMillis();
                suppressionStore.put(now, key);
                return null; // don't emit here, see punctuator
            }

            @Override
            public void close() {
                suppressionStore.close();
            }
        }, FINAL_STORE, SUPPRESSION_STORE);

        suppressedStream.to("output-topic", Produced.with(Serdes.String(), Serdes.String()));
        return builder;
    }

    private KTable groupAndAccumulate(KStream<String, String> stream, String storeName) {
        return stream.groupByKey()
                // For each key, we concatenate values
                .aggregate(
                        String::new,
                        (aggKey, newValue, aggValue) -> aggValue + newValue,
                        Materialized.as(storeName).with(Serdes.String(), Serdes.String())
                );
    }
}
