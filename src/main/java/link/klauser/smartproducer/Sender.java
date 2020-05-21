package link.klauser.smartproducer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static java.util.stream.Collectors.toList;

/**
 * Sends messages to Kafka.
 */
@Slf4j
public class Sender {

    /**
     * <p>
     * Uses a single producer to send messages to Kafka. The order in which topics get filled is determined by
     * {@code effectiveTopics}. Respects the order of files for each topic.
     * </p>
     *
     * @param effectiveTopics     Determines the order in which topics get filled.
     * @param messageFilesByTopic The files to read for each topic.
     * @param producer            The producer to send messages with.
     */
    public void sendInSequence(Set<String> effectiveTopics, Map<String, List<Path>> messageFilesByTopic, Producer<String, byte[]> producer) {
        var totalCount = 0L;
        for (String topic : effectiveTopics) {
            var files = messageFilesByTopic.get(topic);
            var topicCount = sendTopic(producer, topic, files, null);
            System.out.println(topic + ": " + topicCount);
            totalCount += topicCount;
        }
        System.out.println("TOTAL: " + totalCount);
    }

    /**
     * <p>
     * Uses a {@link ForkJoinPool} with up to {@code parallelism} threads and producers. Topics get processed in
     * parallel. The files for a single topic get processed serially. Messages get produced asynchronously, with a
     * flush at the end of each file.
     * </p>
     *
     * @param parallelism         Maximum number of threads/producers to use.
     * @param effectiveTopics     The set of topics to produce messages for. Order is irrelevant.
     * @param messageFilesByTopic The files to read for each topic.
     * @param props               Properties for constructing new Kafka producers.
     */
    public void sendInParallel(int parallelism, Set<String> effectiveTopics, Map<String, List<Path>> messageFilesByTopic, Properties props) {
        System.err.println("Using " + parallelism + " producers to send messages to " + effectiveTopics.size()
                + " topic(s) from " + messageFilesByTopic.values().stream().mapToInt(List::size).sum() + " file(s).");
        System.err.flush();
        try (var producer = new ThreadLocalProducer<String, byte[]>(props, null, null)) {
            var pool = new ForkJoinPool(parallelism);
            var topicTasks = new HashMap<String, ForkJoinTask<Long>>();

            var messageQueue = new ConcurrentLinkedQueue<String>();
            for (String topic : effectiveTopics) {
                var files = messageFilesByTopic.get(topic);
                if (files == null || files.isEmpty()) {
                    log.debug("Skipping topic {} because no message files match that topic.", topic);
                    continue;
                }
                topicTasks.put(topic, pool.submit(() -> sendTopic(producer, topic, files, messageQueue)));
            }

            while (!pool.isQuiescent()) {
                flushMessageQueue(messageQueue);
                pool.awaitQuiescence(100, TimeUnit.MILLISECONDS);
            }
            flushMessageQueue(messageQueue);

            long totalCount = 0L;
            for (Map.Entry<String, ForkJoinTask<Long>> topicTask : topicTasks.entrySet().stream().sorted(Map.Entry.comparingByKey()).collect(toList())) {
                System.out.print(topicTask.getKey() + ": ");
                try {
                    var topicCount = topicTask.getValue().get();
                    totalCount += topicCount;
                    System.out.println(topicCount);
                } catch (ExecutionException e) {
                    System.out.println(e.toString());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new SmartProducerException(e);
                }
            }
            System.out.println("TOTAL: " + totalCount);
        }
    }

    private void flushMessageQueue(ConcurrentLinkedQueue<String> messageQueue) {
        while (true) {
            var msg = messageQueue.poll();
            if (msg == null) {
                break;
            }
            System.err.println(msg);
        }
        System.err.flush();
    }

    /**
     * Read {@code files} in order, producing messages into the supplied {@code topic}+{@code producer}.
     * Respects the order of files in the supplied list.
     *
     * @param producer     The producer to send messages with.
     * @param topic        The topic to send messages to.
     * @param files        List of files to read for this topic.
     * @param messageQueue Optional. Sink for progress messages.
     * @return The total number of messages sent into the topic.
     */
    protected Long sendTopic(Producer<String, byte[]> producer, String topic, List<Path> files, ConcurrentLinkedQueue<String> messageQueue) {
        long messageCount = 0L;
        if (messageQueue != null) {
            messageQueue.add(topic + ": BEGIN");
        }
        for (Path file : files) {
            if (messageQueue != null) {
                messageQueue.add(topic + " << " + file + " BEGIN");
            }
            var fileCount = sendFile(producer, topic, file);
            messageCount += fileCount;
            if (messageQueue != null) {
                messageQueue.add(topic + " << " + file + " SENT " + fileCount + " (TOTAL " + messageCount + ")");
            }
        }
        if (messageQueue != null) {
            messageQueue.add(topic + " SENT " + messageCount);
        }
        return messageCount;
    }

    /**
     * Reads a single file, producing messages asynchronously and then waits for all messages for be acknowledged
     * (flushed).
     *
     * @param producer The producer to send messages into.
     * @param topic    The topic to send messages to.
     * @param file     The file to read.
     * @return The number of messages sent to the topic.
     */
    @SuppressWarnings("java:S1301")
    @SneakyThrows
    protected long sendFile(Producer<String, byte[]> producer, String topic, Path file) {
        var sendFutures = new ArrayList<Future<RecordMetadata>>();
        try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(file.toFile()))) {
            streamMessages(producer, topic, file, sendFutures, input);
        }
        producer.flush();

        long numSent = 0L;
        for (Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
            numSent += 1;
        }

        return numSent;
    }

    /**
     * <p>
     * Scans the supplied {@code input} for JSON objects ({@code { ... }} and sends each top-level object as a message
     * into the {@code topic} and {@code producer}. The producer is allowed to buffer messages. A future for each
     * message gets added to {@code sendFutures} in case the caller wishes to wait for messages to be acknowledged.
     * </p>
     * <p>
     * Implements a partial JSON parser to detect where JSON objects start and end. It doesn't parse the full JSON
     * language, but just enough to keep track of (nested) objects and to correctly parse strings
     * (to not get confused by <code>{ }</code>).
     * </p>
     * <p>
     * If the parser encounters invalid JSON (e.g, missing <code>}</code>), it aborts and reports an error. Line and
     * column information is also tracked to make error message actionable.
     * </p>
     *
     * @param producer    The producer to send with.
     * @param topic       The topic to send to.
     * @param file        The file name (used for diagnostics in case of errors)
     * @param sendFutures Collection to add send futures to.
     * @param input       The input stream. Will consume, but not close the stream.
     * @throws IOException if there is a problem reading from the supplied {@code input} stream
     */
    @SuppressWarnings("java:S3776" /* state machine cannot easily be split up */)
    protected void streamMessages(Producer<String, byte[]> producer, String topic, Path file, Collection<Future<RecordMetadata>> sendFutures, BufferedInputStream input) throws IOException {
        final int MAX_MESSAGE_SIZE = 10 * 1024 * 1024; // 10 MB
        var lineNum = 1L;
        // column will  be increased immediately after the first read
        var colNum = 0L;
        long objectStack = 0;
        long stringStartLine = 0L;
        long stringStartColumn = 0L;
        long objectStartLine = 0L;
        long objectStartColumn = 0L;
        boolean inString = false;
        boolean escaped = false;
        int messageLength = 0;
        while (true) {
            if (objectStack == 0) {
                // if we are outside an object, mark position _before_ we read in case the next character is '{'
                input.mark(MAX_MESSAGE_SIZE);
            }

            int current = input.read();
            if (current == -1) {
                // end of file reached
                if (inString) {
                    throw new SmartProducerException("File ended without closing string starting on line "
                            + stringStartLine + " column " + stringStartColumn + ". File: " + file
                            + (objectStack > 0 ? " Enclosing object starts on line "
                            + objectStartLine + " column " + objectStartColumn : ""));
                }
                if (objectStack > 0) {
                    throw new SmartProducerException("File ended without closing object starting on line " + objectStartLine + " column " + objectStartColumn + ". File: " + file);
                }
                break;
            }
            colNum += 1;

            if (inString) {
                switch (current) {
                    case '"':
                        if (!escaped) {
                            inString = false;
                        }
                        escaped = false;
                        break;
                    case '\\':
                        escaped = !escaped;
                        break;
                    case '\n':
                        escaped = false;
                        lineNum += 1L;
                        // will be reset to col 1 on the next read
                        colNum = 0L;
                        break;
                    default:
                        escaped = false;
                        break;
                }
            } else {
                switch (current) {
                    case '{':
                        objectStack += 1;
                        if (objectStack == 1) {
                            messageLength = 0;
                            objectStartLine = lineNum;
                            objectStartColumn = colNum;
                        }
                        break;
                    case '}':
                        objectStack -= 1;
                        if (objectStack < 0) {
                            throw new SmartProducerException("Unexpected '}'. Line: " + lineNum + " Column: " + colNum + " File: " + file);
                        }
                        if (objectStack == 0) {
                            input.reset();
                            var message = input.readNBytes(messageLength + 1);
                            sendFutures.add(producer.send(new ProducerRecord<>(topic, message)));
                        }
                        break;
                    case '"':
                        inString = true;
                        stringStartLine = lineNum;
                        stringStartColumn = colNum;
                        break;
                    case '\n':
                        lineNum += 1;
                        // will be reset to col 1 on the next read
                        colNum = 0L;
                        break;
                    default:
                        break;
                }
            }

            messageLength += 1;
        }
    }
}
