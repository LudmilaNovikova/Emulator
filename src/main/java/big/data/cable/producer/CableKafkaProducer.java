package big.data.cable.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.*;
import java.text.MessageFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Created by lnovikova on 11.01.2016.
 */
public class CableKafkaProducer {

    private static final Logger logger = Logger.getLogger(CableKafkaProducer.class);

    public static void main(String[] args) throws InterruptedException {
//        args = new String[]{"192.168.1.31:9092", "192.168.1.31:2181", "D:\\projects\\BigData\\Emulator\\src\\main\\resources\\cont_cut_30000"};
        if (args.length != 4) {
            System.out.println("Usage: TruckEventsProducer <broker list> <zookeeper> <dataFilePath> <messagesPerSecond>");
            System.exit(-1);
        }

        String filePath = args[2];

        System.out.println(MessageFormat.format("Using broker list: {0}, zk conn: {1}, path to data file: {2}", args[0], args[1], filePath));

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

        final String TOPIC = "SbtStream";

        int messagesPerSecond = Integer.parseInt(args[3]);
        Date startDate = null;
        Date curDate;
        long duration;
        int numInBatch = 0;
        int totalPublishedMessagesCount = 0;
        Date emulatorStartDate = new Date();

        try (Producer<String, String> producer = new KafkaProducer(props)) {
            try (BufferedReader br = new BufferedReader( new FileReader(new File(filePath)))) {
                String line;
                emulatorStartDate = new Date();
                while ((line = br.readLine()) != null) {
                    if(numInBatch == 0) startDate = new Date();
                    processLine(line, producer, TOPIC);
                    if (numInBatch == messagesPerSecond - 1) {
                        curDate = new Date();
                        duration = curDate.getTime() - startDate.getTime();
                        if(duration < 1000){
                            System.out.println(MessageFormat.format("Start time: {0,number,#}, current time: {1,number,#}. Going to sleep for {2,number,#} milliseconds", startDate.getTime(), curDate.getTime(), 1000 - duration));
                            Thread.sleep(1000 - duration);
                        }
                        numInBatch = -1;
                    }
                    numInBatch ++;
                    totalPublishedMessagesCount++;
                }
            } catch (FileNotFoundException e) {
                System.out.println("Can not find file specified: " + filePath + e);
                // TODO remove after logging enabling
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println("Error during file reading:" + e);
                // TODO remove after logging enabling
                e.printStackTrace();
            }
        }

        System.out.println("totalPublishedMessagesCount: " + totalPublishedMessagesCount);
        System.out.println(MessageFormat.format("Emulator started at {0} (timestamp: {1,number,#})", emulatorStartDate, emulatorStartDate.getTime()));
        Date emulatorEndDate = new Date();
        System.out.println(MessageFormat.format("Emulator finished at {0} (timestamp: {1,number,#}). Duration {2,number,#} milliseconds", emulatorEndDate, emulatorEndDate.getTime(), emulatorEndDate.getTime() - emulatorStartDate.getTime()));

    }

    public static void processLine(String line, Producer<String, String> producer, String topic) {
        producer.send(new ProducerRecord(topic, line));
    }

}
