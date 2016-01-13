package big.data.cable.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Created by lnovikova on 11.01.2016.
 */
public class CableKafkaProducer {

    private static final Logger logger = Logger.getLogger(CableKafkaProducer.class);

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: TruckEventsProducer <broker list> <zookeeper> <dataFilePath>");
            System.exit(-1);
        }

        String filePath = args[2];

        logger.debug(MessageFormat.format("Using broker list: {0}, zk conn: {1}, path to data file: {2}", args[0], args[1], filePath));

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

//        File file2 = new File(ClassLoader.getSystemResource("cont_cut").getPath());

        try (Producer<String, String> producer = new KafkaProducer(props)) {
            try (BufferedReader br = new BufferedReader( new FileReader(new File(filePath)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    processLine(line, producer, TOPIC);
                }
            } catch (FileNotFoundException e) {
                logger.error("Can not find file specified: " + filePath, e);
                // TODO remove after logging enabling
                e.printStackTrace();
            } catch (IOException e) {
                logger.error("Error during file reading", e);
                // TODO remove after logging enabling
                e.printStackTrace();
            }
        }

    }

    public static void processLine(String line, Producer<String, String> producer, String topic) {
        producer.send(new ProducerRecord(topic, line));
    }

}
