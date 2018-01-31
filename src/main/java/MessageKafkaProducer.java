import java.io.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MessageKafkaProducer {

    public static void main(String[] args) {

        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(configProperties);

        String file= System.getProperty("user.dir") +"/data/"+ "one_vessel";
        FileReader flr=null;
        try {
            flr = new FileReader(new File(file));
            BufferedReader br = new BufferedReader(flr);
            String line;
            int counter = 0;
            while ((line = br.readLine()) != null) {
                if (counter > 0) {
                    System.out.println(line);

                    ProducerRecord<String, String> record = new ProducerRecord<>("datacron", line);

                    producer.send(record);
                    Thread.sleep(1000);
                } else {
                    counter++;
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                flr.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
