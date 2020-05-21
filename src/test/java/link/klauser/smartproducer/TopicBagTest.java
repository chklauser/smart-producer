package link.klauser.smartproducer;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TopicBagTest {
    TopicBag bag = new TopicBag(List.of(
            "in-commons2-code-table-series-sync-event",
            "in-commons2-metadata-definition-series-sync-event",
            "in-commons2-collection-sync-event",
            "in-commons2-country-sync-event",
            "in-commons2-document-sync-event",
            "in-core2-address-sync-event",
            "in-core2-business-partner-sync-event",
            "in-core2-person-sync-event",
            "in-core2-phone-number-sync-event",
            "in-core2-email-address-sync-event",
            "in-core2-portfolio-sync-event",
            "in-core2-position-sync-event",
            "in-sap-financial-key-figures-full",
            "in-sap-org-user-record-full",
            "in-organization2-employee-sync-event",
            "in-crm2-questionnaire-sync-event",
            "in-crm2-issue-order-sync-event",
            "in-crm2-contact-management-order-sync-event",
            "in-instruments2-instrument-sync-event",
            "in-instruments2-exchange-rate-series-sync-event",
            "in-commons2-settlement-order-sync-event",
            "in-cashier2-bank-issued-medium-term-note-order-sync-event",
            "in-payments2-collective-order-sync-event",
            "in-trading2-stock-exchange-order-sync-event",
            "in-trading2-fiduciary-deposit-order-sync-event",
            "in-trading2-money-market-order-core-sync-event",
            "in-payments2-payment-order-sync-event",
            "in-payments2-card-transaction-order-sync-event",
            "in-payments2-incoming-payment-order-sync-event",
            "in-payments2-money-transfer-order-sync-event",
            "in-payments2-cashier-operation-order-sync-event",
            "in-avaloq-authorization-sec-user-sync-event"
    ));

    @Test
    void codeTable() {
        ///// GIVEN ////
        var path = Path.of("events", "in-commons2-code-table-series-sync-event",
                "in-commons2-code-table-series-sync-event-contact_mgmt_gift.json");

        ///// WHEN /////
        var topic = bag.inferTopic(path);

        ///// THEN /////
        assertThat(topic).as("inferred topic")
                .isEqualTo("in-commons2-code-table-series-sync-event");
    }

    @Test
    void emailVsAddress() {
        ///// GIVEN ////
        var path = Path.of("events","in-core2-address-sync-event","in-core2-address-sync-event_20191113_153543.json");

        ///// WHEN /////
        var topic = bag.inferTopic(path);

        ///// THEN /////
        assertThat(topic).as("inferred topic")
                .isEqualTo("in-core2-address-sync-event");
    }
}