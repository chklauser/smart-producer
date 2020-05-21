package link.klauser.smartproducer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SenderTest {
    Sender sender = new Sender();

    @Mock
    Producer<String, byte[]> producer;

    @Captor
    ArgumentCaptor<ProducerRecord<String, byte[]>> recordCaptor;

    @Test
    void streamEmpty() throws IOException {
        ///// GIVEN ////
        expectNoRecord("");
    }

    @Test
    void streamEmptyNoObject() throws IOException {
        ///// GIVEN ////
        expectNoRecord("[]:'_4577HHiPPwbj;..</true");
    }

    @Test
    void streamEmptyTopLevelString() throws IOException {
        expectNoRecord("\"{}\"");
    }

    private void expectNoRecord(String input) throws IOException {
        ///// GIVEN ////
        var sendFutures = new ArrayList<Future<RecordMetadata>>();
        var path = Path.of("path", "to", "file");
        try (var bis = new BufferedInputStream(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)))) {

            ///// WHEN /////
            sender.streamMessages(producer, "the-topic", path, sendFutures, bis);

            ///// THEN /////
            assertThat(sendFutures).as("futures")
                    .isEmpty();
            assertThat(bis.read()).as("read call on input stream")
                    .isEqualTo(-1);

            verifyNoInteractions(producer);
        }
    }

    @Test
    void minimal() throws IOException {
        expectRecords("{}", "{}");
    }

    @Test
    void trim() throws IOException {
        ///// GIVEN ////
        expectRecords(" \n { \n\0▲\r }\n ", "{ \n\0▲\r }");
    }

    @Test
    void surroundedByLf() throws IOException {
        ///// GIVEN ////
        expectRecords("\n\n{\n\n\n}\n\n", "{\n\n\n}");
    }

    @Test
    void surroundedByEscape() throws IOException {
        ///// GIVEN ////
        expectRecords("\\\\\\{\\\\\\}\\\\", "{\\\\\\}");
    }

    @Test
    void surroundedByString() throws IOException {
        ///// GIVEN ////
        expectRecords("\"mmjjuu{rrgy{}pplloo}uuyyhoo\"{\"ppol{}wxd\"}\"iioopp\"", "{\"ppol{}wxd\"}");
    }

    @Test
    void stringEscape() throws IOException {
        ///// GIVEN ////
        expectRecords("yy{\"end\\\"}here\":6}xx", "{\"end\\\"}here\":6}");
    }

    @Test
    void stringDoubleEscape() throws IOException {
        expectRecords("yy{\"end\\\\\"}here\":6\"xx", "{\"end\\\\\"}");
    }

    @Test
    void twoMinimal() throws IOException {
        expectRecords("{}{}", "{}", "{}");
    }

    @Test
    void twoMinimalDistinguishable() throws IOException {
        expectRecords("{1}{2}", "{1}", "{2}");
    }

    @Test
    void twoTrim() throws IOException {
        expectRecords("  {1}  {2}  ", "{1}", "{2}");
    }

    @Test
    void nested() throws IOException {
        expectRecords("{{}}", "{{}}");
    }

    @Test
    void nestedTwo() throws IOException {
        expectRecords("{{}}{{}}", "{{}}", "{{}}");
    }

    @Test
    void nestedDeep() throws IOException {
        expectRecords("xx{{}{{}{}}}yy{}", "{{}{{}{}}}", "{}");
    }

    @Test
    void realistic() throws IOException {
        var input = "# this text gets ignored\n" +
                "{ \n" +
                "  \"first_name\" : \"Will\",\n" +
                "  \"last_name\" : \"Yak\",\n" +
                "  \"location\" : \"Poly\",\n" +
                "  \"websites\" : [ \n" +
                "    {\n" +
                "      \"description\" : \"work\",\n" +
                "      \"URL\" : \"https://example.com/\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"desciption\" : \"tutorials\",\n" +
                "      \"URL\" : \"https://example.com/tutorials\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"blurb\" : [\n" +
                "    {\n" +
                "      \"description\" : \"mega\",\n" +
                "      \"free\" : \"Mug\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"description\" : \"mini\",\n" +
                "      \"error\" : {\"code\":47}\n" +
                "    }\n" +
                "  ]\n" +
                "}\n" +
                "\n" +
                "{\"error\":{\"code\":47}}\n" +
                "// this text also gets ignored\n" +
                "{\n" +
                "\t\"first_name\":\"Robinson\"\n" +
                "\n" +
                "             \"last_name\":\"Wyrd\"}\n" +
                "▲.";
        expectRecords(input, "{ \n" +
                        "  \"first_name\" : \"Will\",\n" +
                        "  \"last_name\" : \"Yak\",\n" +
                        "  \"location\" : \"Poly\",\n" +
                        "  \"websites\" : [ \n" +
                        "    {\n" +
                        "      \"description\" : \"work\",\n" +
                        "      \"URL\" : \"https://example.com/\"\n" +
                        "    },\n" +
                        "    {\n" +
                        "      \"desciption\" : \"tutorials\",\n" +
                        "      \"URL\" : \"https://example.com/tutorials\"\n" +
                        "    }\n" +
                        "  ],\n" +
                        "  \"blurb\" : [\n" +
                        "    {\n" +
                        "      \"description\" : \"mega\",\n" +
                        "      \"free\" : \"Mug\"\n" +
                        "    },\n" +
                        "    {\n" +
                        "      \"description\" : \"mini\",\n" +
                        "      \"error\" : {\"code\":47}\n" +
                        "    }\n" +
                        "  ]\n" +
                        "}",
                "{\"error\":{\"code\":47}}",
                "{\n" +
                        "\t\"first_name\":\"Robinson\"\n" +
                        "\n" +
                        "             \"last_name\":\"Wyrd\"}");
    }

    @Test
    void resetEscape() throws IOException {
        ///// GIVEN ////
        var data = "{\"\\1\"}{}";

        ///// WHEN /////
        expectRecords(data, data.substring(0,data.length()-2), "{}");

        ///// THEN /////
    }

    @SuppressWarnings("unchecked")
    private void expectRecords(String raw, String... expectedMessages) throws IOException {
        ///// GIVEN ////
        if (expectedMessages == null || expectedMessages.length == 0) {
            throw new IllegalArgumentException("Need to expect at least one record.");
        }
        var sendFutures = new ArrayList<Future<RecordMetadata>>();
        var path = Path.of("path", "to", "file");
        var futures = IntStream.range(0, expectedMessages.length)
                .mapToObj(x -> (Future<RecordMetadata>) mock(Future.class, "f" + x))
                .collect(toList());
        when(producer.send(any())).thenReturn(futures.get(0), futures.stream().skip(1).toArray(Future[]::new));
        var input = raw.getBytes(StandardCharsets.UTF_8);
        try (var bis = new BufferedInputStream(new ByteArrayInputStream(input))) {

            ///// WHEN /////
            sender.streamMessages(producer, "the-topic", path, sendFutures, bis);

            ///// THEN /////
            assertThat(bis.read()).as("read call on input stream")
                    .isEqualTo(-1);

            verify(producer, times(expectedMessages.length)).send(recordCaptor.capture());
            var records = recordCaptor.getAllValues();
            var i = 0;
            for (ProducerRecord<String, byte[]> record : records) {
                assertThat(record).as("producer record #%d", i)
                        .hasFieldOrPropertyWithValue("key", null)
                        .hasFieldOrPropertyWithValue("partition", null)
                        .hasFieldOrPropertyWithValue("topic", "the-topic")
                        .extracting("value").isNotNull();
                assertThat(new String(record.value(), StandardCharsets.UTF_8))
                        .as("message value of record #%d", i)
                        .isEqualTo(expectedMessages[i]);

                i += 1;
            }

            verifyNoMoreInteractions(producer);

            assertThat(sendFutures).as("futures")
                    .containsExactlyElementsOf(futures);
        }
    }
}