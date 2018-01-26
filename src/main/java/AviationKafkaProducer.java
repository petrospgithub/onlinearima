import java.io.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class AviationKafkaProducer {

    public static void main(String[] args) {

        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(configProperties);

        InputStream file= AviationKafkaProducer.class.getClass().getResourceAsStream("/6852347_flight.csv"); //.getClass().getResourceAsStream("6852347_flight.csv");

        try (BufferedReader br = new BufferedReader(new InputStreamReader(file))) {
            String line;
            int counter=0;
            while ((line = br.readLine()) != null) {
                if (counter>0) {
                    System.out.println(line);

                    ProducerRecord<String, String> record = new ProducerRecord<>("datacron", line);

                    producer.send(record);
                } else {
                    counter++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                file.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
