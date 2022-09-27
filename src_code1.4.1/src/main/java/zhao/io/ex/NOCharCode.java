package zhao.io.ex;

/**
 * 字符编码集设置错误异常
 */
public class NOCharCode extends NullPointerException {
    public NOCharCode() {
        super();
    }

    public NOCharCode(String s) {
        super(s);
    }
}
