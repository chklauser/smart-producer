package link.klauser.smartproducer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import picocli.CommandLine;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static link.klauser.smartproducer.SmartProducer.NAME;
import static java.util.stream.Collectors.*;

@CommandLine.Command(showAtFileInUsageHelp = true, showEndOfOptionsDelimiterInUsageHelp = true,
        description = "Sends JSON Kafka messages to a Kafka broker.", name = NAME,
        footerHeading = "%n@|bold EXAMPLES|@%n%n", footer = {
        "@|bold Full data load, purging old messages:|@",
        "  ${COMMAND-NAME} @topics.txt @all-messages.txt --purge",
        "",
        "@|bold Load a single message file into local Kafka broker; topic inferred from file name:|@",
        "  ${COMMAND-NAME} @topics.txt --local incoming-orders_MY_TABLE.json"
}, usageHelpWidth = 120)
@Slf4j
public class SmartProducer implements Runnable {
    public static final String NAME = "smart-producer";

    @CommandLine.ArgGroup
    Parallelism parallelism;

    static class Parallelism {
        @CommandLine.Option(names = {"--parallel", "-j"}, arity = "0..1", defaultValue = "-1",
                description = "Number of producers to run in parallel across all topics. (default: ${DEFAULT-VALUE})")
        int parallel = 8;

        @CommandLine.Option(names = "--no-parallel", description = "Only use a single producer.")
        boolean disableParallel;
    }

    @CommandLine.Option(names = "--purge", description = "Delete old messages before inserting new messages.")
    boolean purge;

    @CommandLine.Option(names = "--purge-wait", description = "Time to wait for Kafka to drop old log segments during a --purge operation. (default: ${DEFAULT-VALUE}, format: PnDTnHnMn.nS)", defaultValue = "PT15S")
    Duration purgeWait;

    @CommandLine.Option(names = {"--topic", "-t", "--event"}, description = "Only send to the indicates topics. " +
            "Can be specified multiple times. Sends to all topics configured via --known-topic by default.")
    List<String> topics = new ArrayList<>();

    @CommandLine.Option(names = {"--known-topic"}, required = true, description = "Needs to be specified multiple " +
            "times to define the set of topics considered to exist. " +
            "Collect these options in a file and include it as an @args file.")
    List<String> knownTopics = new ArrayList<>();

    @CommandLine.Option(names = "--known-group", description = "Name of a consumer group to monitor (see --monitor). May be specified multiple times.")
    List<String> knownConsumerGroups = new ArrayList<>();

    @CommandLine.Parameters(description = "Message files. The file name of file path needs to start with or contain " +
            "a string that can be mapped to one of the known topics.")
    List<Path> messageFiles = new ArrayList<>();

    @CommandLine.Option(names = {"--dry-run"}, description = "Explain which message files would be sent to which topic.")
    boolean dryRun;

    @CommandLine.Option(names = {"--check-format"}, description = "Verify that all listed files can be separated into messages. Does not send any messages.")
    boolean checkFormat;

    @CommandLine.Option(names = {"--property", "-p"}, description = "Additional Kafka producer properties (key=value). " +
            "Can be specified multiple times.")
    Map<String, String> producerProperties;

    @CommandLine.Option(names = {"--admin", "-A"}, description = "Additional Kafka admin properties (key=value). " +
            "Can be specified multiple times.")
    Map<String, String> adminProperties = new HashMap<>();

    @CommandLine.Option(names = "--broker", defaultValue = "localhost:9092", description = "Connect to the indicated Kafka broker (host:port, default: ${DEFAULT-VALUE}).")
    String broker;

    @CommandLine.Option(names = "-C", description = "Custom topic config values to set after --purge.")
    Map<String, String> customConfigs = new HashMap<>();

    @CommandLine.Option(names = "--monitor", description = "Monitor the progress of messages being consumed.")
    boolean monitor;

    @SuppressWarnings({"java:S3776", /* control flow is simple; refactoring negatively affects static analysis */})
    @Override
    public void run() {
        var bag = new TopicBag(knownTopics);

        // Resolve topics (need to be unambiguous)
        // use linked hash set to maintain topic order
        LinkedHashSet<String> effectiveTopics;
        boolean implicitTopics;
        if (topics != null && !topics.isEmpty()) {
            System.err.println("Resolving topic names...");
            implicitTopics = false;
            effectiveTopics = topics.stream()
                    .map(x -> bag.inferTopic(List.of(x)))
                    .collect(toCollection(LinkedHashSet::new));
        } else {
            implicitTopics = true;
            effectiveTopics = new LinkedHashSet<>(knownTopics);
        }

        // Resolve topics for each event
        System.err.println("Resolving topics for message files...");
        var messageFilesByTopic = messageFiles.stream()
                .collect(groupingBy(bag::inferTopic)).entrySet().stream()
                .filter(e -> effectiveTopics.contains(e.getKey()))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (implicitTopics && !messageFiles.isEmpty()) {
            // Without an explicit topic selection, the set of topics inferred from supplied files becomes the effective
            // topic selection.
            effectiveTopics.removeIf(t -> !messageFilesByTopic.containsKey(t));
        }

        if (effectiveTopics.isEmpty()) {
            System.err.println("Nothing to do. No topics or message files selected or none of the message " +
                    "files match any of the selected topics.");
            return;
        }

        var props = producerDefaultProperties();
        if (producerProperties != null) {
            props.putAll(producerProperties);
        }

        if (dryRun) {
            printDryRunInformation(effectiveTopics, messageFilesByTopic, props);
        }

        var sender = new Sender();
        if (checkFormat) {
            System.err.println("Checking message format...");
            sender.sendInSequence(effectiveTopics, messageFilesByTopic, new MockProducer<>());
        }

        if (checkFormat || dryRun) {
            return;
        }

        if (purge) {
            purgeTopics(createAdminClient(), effectiveTopics);
        }

        if (!messageFiles.isEmpty()) {
            if (parallelism == null || !parallelism.disableParallel) {
                var px = parallelism == null || parallelism.parallel < 0 ? 8 : parallelism.parallel;
                sender.sendInParallel(px, effectiveTopics, messageFilesByTopic, props);
            } else {
                try (var producer = new KafkaProducer<String, byte[]>(props)) {
                    sender.sendInSequence(effectiveTopics, messageFilesByTopic, producer);
                }
            }
        } else if(!monitor) {
            System.err.println("No message files specified.");
        }

        if(monitor) {
            if(knownConsumerGroups.isEmpty()) {
                System.err.println("`--monitor` requires that consumer groups are specified using `--known-group=` " +
                        "(can be specified multiple times).");
                return;
            }

            new LagMonitor(createAdminClient(), effectiveTopics, new HashSet<>(knownConsumerGroups)).monitor();
        }
    }

    private AdminClient createAdminClient() {
        var adminProps = new Properties();
        configureClientProperties(adminProps);
        adminProps.putAll(adminProperties);
        return AdminClient.create(adminProps);
    }

    /**
     * Configures topics with a 1ms retention time, waits for Kafka to drop the log segments and then restores the
     * previous configuration.
     *
     * @param client          The Kafka admin client.
     * @param effectiveTopics The set of topics to purge.
     */
    @SuppressWarnings("deprecation")
    private void purgeTopics(AdminClient client, LinkedHashSet<String> effectiveTopics) {
        System.err.println("Fetching existing config for " + effectiveTopics.size() + " topic(s)...");
        Map<ConfigResource, Config> describeResult;
        var topicResources = effectiveTopics.stream().map(t -> new ConfigResource(ConfigResource.Type.TOPIC, t)).collect(toList());
        try {
            describeResult = client.describeConfigs(topicResources).all().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SmartProducerException(e);
        } catch (ExecutionException e) {
            throw new SmartProducerException("Failed to read current topic description.", e.getCause());
        }

        System.err.println("Re-configuring " + effectiveTopics.size() + " topic(s) to have messages dropped...");
        var purgeConfig = new Config(List.of(
                new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "1"),
                new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
        ));
        // Need to use the old `alterConfigs` because `incrementalAlterConfigs` is only supported from Kafka 2.3+
        try {
            client.alterConfigs(topicResources.stream().collect(toMap(e -> e, e -> purgeConfig))).all().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SmartProducerException(e);
        } catch (ExecutionException e) {
            // Don't throw an exception here. We want to try to fix the problem.
            log.error("Failed to change topic configuration for some topics.", e.getCause());
        }

        System.err.println("Waiting for " + purgeWait.toString().replace("PT", "")
                + " to give Kafka a chance to drop old log segments...");
        try {
            Thread.sleep(purgeWait.toMillis());
        } catch (InterruptedException e) {
            log.info("Purge wait interrupted.", e);
            Thread.currentThread().interrupt();
        }

        // Restore original config (either by deleting our override or by setting the original value)
        System.err.println("Restoring configuration for " + effectiveTopics.size() + " topic(s)...");
        try {
            client.alterConfigs(describeResult.entrySet().stream()
                    .collect(toMap(Map.Entry::getKey, e -> restoredConfig(e.getValue(), purgeConfig)))).all().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SmartProducerException(e);
        } catch (ExecutionException e) {
            throw new SmartProducerException("Failed to restore topic configuration for all topics.", e.getCause());
        }
    }

    /**
     * Initializes a {@link Properties} with default values to use for the producer. Can be overridden by
     * {@link #producerProperties}.
     */
    private Properties producerDefaultProperties() {
        var props = new Properties();
        configureClientProperties(props);
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        return props;
    }

    /**
     * Initializes a {@link Properties} with settings to connect to Kafka. Used for both the
     * {@link #producerDefaultProperties()} and for connecting to the admin API.
     *
     * @param props The properties to configure
     */
    private void configureClientProperties(Properties props) {
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, NAME + ":" + System.getProperty("user.name") + "@" + hostName());
    }

    /**
     * Creates a {@link Config} that contains the values of {@code original} for config key that was changed by
     * {@code change}.
     *
     * @param original The original configuration (including defaults)
     * @param change   The change submitted to Kafka
     * @return A new config that reverts {@code change}
     */
    Config restoredConfig(Config original, Config change) {
        Map<String, ConfigEntry> revert = customConfigs.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, e -> new ConfigEntry(e.getKey(), e.getValue())));
        change.entries().stream()
                .filter(c -> !revert.containsKey(c.name()))
                .forEach(c -> revert.put(c.name(), original.get(c.name())));
        return new Config(revert.values());
    }

    /**
     * <p>
     * Print effective Kafka configuration along with topics and which files will get loaded into which topics.
     * </p>
     * <p>
     * Prints directly to stdout.
     * </p>
     *
     * @param effectiveTopics     The selected topics.
     * @param messageFilesByTopic The message files groups by topic.
     * @param props               The Kafka producer properties.
     */
    private void printDryRunInformation(LinkedHashSet<String> effectiveTopics, Map<String, List<Path>> messageFilesByTopic, Properties props) {
        System.out.println("Kafka producer properties:");
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }
        System.out.println();
        for (var topic : effectiveTopics) {
            System.out.println(topic + ":");
            for (Path messageFilePath : messageFilesByTopic.get(topic)) {
                System.out.println("  " + messageFilePath);
            }
            System.out.println();
        }
        if (purge) {
            System.out.println("Would purge the topics mentioned above. (Delete all messages)");
        }
    }

    /**
     * @return computer's name or {@code 'unknown'}. Never {@code null}.
     */
    private String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }

    public static void main(String[] args) {
        var commandLine = new CommandLine(new SmartProducer());
        commandLine.setUseSimplifiedAtFiles(true);
        commandLine.setCaseInsensitiveEnumValuesAllowed(true);
        commandLine.setTrimQuotes(false);
        commandLine.execute(args);
    }
}
