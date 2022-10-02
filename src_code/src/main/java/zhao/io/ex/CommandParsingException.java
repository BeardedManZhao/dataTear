package zhao.io.ex;

/**
 * 命令解析异常
 */
public class CommandParsingException extends ArrayIndexOutOfBoundsException {
    public CommandParsingException() {
        super();
    }

    public CommandParsingException(int index) {
        super(index);
    }

    public CommandParsingException(String s) {
        super(s);
    }
}
