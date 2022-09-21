package zhao.io.ex;

public class OutUDFException extends NullPointerException {
    public OutUDFException(String message) {
        super("自定义数据输出流异常!!!");
    }
}
