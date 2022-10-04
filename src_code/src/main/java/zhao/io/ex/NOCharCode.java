package zhao.io.ex;

/**
 * 字符编码集设置错误异常
 */
public class NOCharCode extends DataTearException {
    public NOCharCode() {
        super();
    }

    public NOCharCode(String s) {
        super(s);
    }

    public NOCharCode(String errorStr, org.slf4j.Logger logger) {
        super(errorStr);
        super.LoggerToFile(this, logger);
    }

}
