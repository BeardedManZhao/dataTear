package zhao.io.ex;

public class OutUDFException extends DataTearException {
    public OutUDFException(String message) {
        super("自定义数据输出流异常!!!");
    }

    public OutUDFException(String errorStr, org.slf4j.Logger logger) {
        super(errorStr);
        super.LoggerToFile(this, logger);
    }
}
