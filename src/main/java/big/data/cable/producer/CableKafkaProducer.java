package big.data.cable.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Created by lnovikova on 11.01.2016.
 */
public class CableKafkaProducer {

    private static final Logger logger = Logger.getLogger(CableKafkaProducer.class);

    public static void main(String[] args) {
        if (args.length != 2) {

            System.out.println("Usage: TruckEventsProducer <broker list> <zookeeper>");
            System.exit(-1);
        }

        logger.debug("Using broker list:" + args[0] + ", zk conn:" + args[1]);

        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);
        props.put("zk.connect", args[1]);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

/*
        props.put("metadata.broker.list", args[0]);
        props.put("zk.connect", args[1]);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
*/

        final String TOPIC = "SbtStream";

        try (Producer<String, String> producer = new KafkaProducer(props)) {
            String filePath = ClassLoader.getSystemResource("cont_cut").getPath();
            try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
                lines.forEachOrdered(line -> processLine(line, producer, TOPIC));
            } catch (IOException e) {
                logger.error("Error during file parsing", e);
                e.printStackTrace();
            }
        }

/*
        producer.send(new ProducerRecord<String, String>(TOPIC, "1"));
        producer.send(new ProducerRecord<String, String>(TOPIC, "hey!"));
        producer.send(new ProducerRecord<String, String>(TOPIC, "2"));
*/

    }

    public static void processLine(String line, Producer<String, String> producer, String topic) {
        producer.send(new ProducerRecord(topic, line));
    }

}
