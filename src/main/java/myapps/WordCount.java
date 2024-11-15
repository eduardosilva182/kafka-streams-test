package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCount {

    public static void main(String[] args) {
        //Seta as configurações para conversação com os brokers do Kafka
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        kafkaProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        kafkaProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //Cria nova stream conectada ao tópico "streams-plaintext-input"
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> source = streamsBuilder.stream("streams-plaintext-input");

        //Quebra o input de texto recebido via tópico em palavras
        KStream<String, String> words = source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")));

        //Posta cada palavra em outro tópico chamado "streams-linesplit-output"
        words.to("streams-linesplit-output");

        //Conta as palavras que se repetem e armazena em uma store chamada "counts-store"
        KTable<String, Long> counts = words.groupBy((key, value) -> value).count(Materialized.as("counts-store"));

        //Posta o resultado da contagem em outro tópico chamado "streams-wordcount-output"
        counts.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        //Cria um desenho da topologia e exibe no console
        final Topology topology = streamsBuilder.build();
        System.out.println(topology.describe());

        //Cria o stream do Kafka
        final KafkaStreams streams = new KafkaStreams(topology, kafkaProperties);

        //Cria um hook para identificar a interrupção do processo
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }

        System.exit(0);
    }

}
