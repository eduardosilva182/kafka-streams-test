package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class LineSplit {

    public static void main(String[] args) {
        //Seta as configurações para conversação com os brokers do Kafka
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit");
        kafkaProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        kafkaProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //Cria nova stream conectada ao tópico "streams-plaintext-input"
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> source = streamsBuilder.stream("streams-plaintext-input");

        //Quebra o input de texto recebido via tópico em palavras
        KStream<String, String> words = source.flatMapValues(value -> Arrays.asList(value.split("\\W+")));

        //Posta cada palavra em outro tópico chamado "streams-linesplit-output"
        words.to("streams-linesplit-output");

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
