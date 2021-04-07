package rpc;

public class ServiceNotFoundException extends RuntimeException {
    private static final long serialVersionUID = -7881615437057029024L;

    public ServiceNotFoundException() {
        super("The service can not be found.");
    }
}
