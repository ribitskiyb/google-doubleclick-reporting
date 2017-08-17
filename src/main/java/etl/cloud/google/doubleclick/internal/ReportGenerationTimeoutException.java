package etl.cloud.google.doubleclick.internal;

public class ReportGenerationTimeoutException extends Exception {

    public ReportGenerationTimeoutException() {
        super();
    }

    public ReportGenerationTimeoutException(String msg) {
        super(msg);
    }

    public ReportGenerationTimeoutException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public ReportGenerationTimeoutException(Throwable cause){
        super(cause);
    }

}
