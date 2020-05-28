package link.klauser.smartproducer;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TopicBagTest {
    TopicBag bag = new TopicBag(List.of(
            "incoming-orders-topic",
            "incoming-email-address-topic",
            "incoming-address-topic"
    ));

    @Test
    void orders() {
        ///// GIVEN ////
        var path = Path.of("events", "incoming-orders",
                "incoming.json");

        ///// WHEN /////
        var topic = bag.inferTopic(path);

        ///// THEN /////
        assertThat(topic).as("inferred topic")
                .isEqualTo("incoming-orders-topic");
    }

    @Test
    void emailVsAddress() {
        ///// GIVEN ////
        var path = Path.of("events","incoming-email-address","address.json");

        ///// WHEN /////
        var topic = bag.inferTopic(path);

        ///// THEN /////
        assertThat(topic).as("inferred topic")
                .isEqualTo("incoming-email-address-topic");
    }
}