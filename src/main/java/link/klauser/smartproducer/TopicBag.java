package link.klauser.smartproducer;

import com.googlecode.concurrenttrees.radix.node.concrete.DefaultByteArrayNodeFactory;
import com.googlecode.concurrenttrees.solver.LCSubstringSolver;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.Value;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * <p>
 * Implements topic name inference based on longest common substrings. Inference is only successful, if there is a
 * single best match with at least length {@link #MIN_SCORE}. If there are two or more matches with the same
 * "high score", an exception gets thrown.
 * </p>
 */
@ToString
public class TopicBag {
    @Getter
    @NonNull
    private final Set<String> topics;

    public TopicBag(@NonNull Collection<String> knownTopics) {
        topics = Set.copyOf(knownTopics);
    }

    private static final int MIN_SCORE = 4;

    /**
     * <p>
     * Tries to infer a topic from a list of "signals". Only the highest score among the signals is considered.
     * If the highest score is ambiguous, an exception gets thrown (even if antoher signal with a lower score would be
     * unambiguous).
     * </p>
     * <p>
     *     Signals shorter than {@link #MIN_SCORE} are ignored.
     * </p>
     * @param signals Set of signals to consider.
     * @return The (unambiguous) topic.
     * @throws SmartProducerException if the signal is ambiguous.
     */
    @NonNull
    public String inferTopic(@NonNull List<String> signals) {
        var bestCandidate = signals.stream()
                .filter(x -> x != null && x.length() >= MIN_SCORE)
                .map(this::evaluateSignal).max(Comparator.comparing(Candidate::getScore))
                .orElseThrow(() -> new SmartProducerException("No signal supplied."));
        if (bestCandidate.getTopics().isEmpty() || bestCandidate.getScore() < MIN_SCORE) {
            throw new SmartProducerException("No topics found matching " + String.join("/", signals));
        } else if (bestCandidate.getTopics().size() == 1) {
            return bestCandidate.getTopics().get(0);
        } else {
            throw new SmartProducerException(String.join("/", signals) + " is ambiguous. " +
                    "It matches all of the following topics equally well: " +
                    String.join(", ", bestCandidate.getTopics()));
        }
    }

    /**
     * <p>
     *     Tries to infer the topic for a file path by treating each path component as a signal.
     * </p>
     * <p>
     *     See {@link #inferTopic(List)} for more details.
     * </p>
     * @param signals The path
     * @return The (unambiguous) topic.
     * @throws SmartProducerException if the signal is ambiguous.
     */
    @NonNull
    public String inferTopic(@NonNull Path signals) {
        var s2 = IntStream.range(0, signals.getNameCount())
                .mapToObj(i -> signals.getName(i).toString())
                .collect(toList());
        return inferTopic(s2);
    }

    /**
     * Computes the score and finds all matching topics. The candidate is unambiguous if and only if there is exactly
     * one matching topic.
     * @param signal The signal to evaluate
     * @return The score and matched topics.
     */
    private Candidate evaluateSignal(String signal) {
        var collect = topics.stream()
                .map(t -> new Evaluation(t, scoreFor(t, signal)))
                .collect(groupingBy(Evaluation::getScore));
        var highScore = collect.keySet().stream().max(Comparator.comparing(i -> i));
        return highScore.map(score -> new Candidate(signal, score, collect.get(score).stream()
                .map(Evaluation::getTopic)
                .collect(toList()))
        ).orElseGet(() -> new Candidate(signal, 0, List.of()));
    }

    int scoreFor(String topic, String signal) {
        // We don't care how much of "signal" gets matched. What's important is how much we match of topic.
        // In particular, if one topic is a prefix of another topic, a partial match within that prefix SHOULD result
        // in the same score, so that we can warn users about the ambiguity.
        var solver = new LCSubstringSolver(new DefaultByteArrayNodeFactory());
        solver.add(topic);
        solver.add(signal);
        return solver.getLongestCommonSubstring().length();
    }

    @Value
    static class Evaluation {
        @NonNull String topic;
        int score;
    }

    @Value
    static class Candidate {
        String signal;
        int score;
        List<String> topics;
    }
}
