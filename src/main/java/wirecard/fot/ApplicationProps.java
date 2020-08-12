package wirecard.fot;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;
import java.util.ResourceBundle;

public class ApplicationProps {

    private static ResourceBundle appBundle = ResourceBundle.getBundle("application");

    public static final String brokerUrl = appBundle.getString("kafka.broker.url");
    public static final String sourceTopic = appBundle.getString("fot.source.topic");
    public static final String sourceApplicationId = appBundle.getString("fot.source.application.id");

    public static final String fotAppId = appBundle.getString("fot.application.id");
    public static final String fotClientId = appBundle.getString("fot.client.id");
    public static final String targetTopic = appBundle.getString("fot.target.topic");


    private static Properties getKafkaStreamProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);

        return props;
    }

    public static Properties getCopyFromProperties() {
        Properties props = getKafkaStreamProps();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, fotAppId);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, fotClientId);
        return props;
    }
}
