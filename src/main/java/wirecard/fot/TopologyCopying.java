package wirecard.fot;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wirecard.fot.ApplicationProps.*;

public class TopologyCopying {

    private static Logger log = LoggerFactory.getLogger("CopyFromOffsetTopology");

    public static void startCopying() {

        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(
                sourceTopic,
                Consumed.with(Serdes.String(), Serdes.String())
        ).to(targetTopic);

        KafkaStreams streams = new KafkaStreams(builder.build(), getCopyFromProperties());

        log.info("Starting copying from topic {} to {}", sourceTopic, targetTopic);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


}
