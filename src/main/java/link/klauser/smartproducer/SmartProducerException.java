package link.klauser.smartproducer;

public class SmartProducerException extends RuntimeException {
    public SmartProducerException() {
    }

    public SmartProducerException(String message) {
        super(message);
    }

    public SmartProducerException(String message, Throwable cause) {
        super(message, cause);
    }

    public SmartProducerException(Throwable cause) {
        super(cause);
    }
}
