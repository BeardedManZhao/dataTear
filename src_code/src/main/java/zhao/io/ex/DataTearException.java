package zhao.io.ex;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * DataTear异常类，其中包含一个将异常堆栈打印到日志中的构造函数
 */
public abstract class DataTearException extends NullPointerException {

    public DataTearException() {
    }

    public DataTearException(String errorStr) {
        super(errorStr);
    }

    /**
     * 将错误堆栈输出到对应的logger中
     *
     * @param dataTearException 错误异常
     * @param logger            日志输出器
     */
    public void LoggerToFile(Throwable dataTearException, org.slf4j.Logger logger) {
        StringWriter sw = null;
        PrintWriter pw = null;
        try {
            sw = new StringWriter();
            pw = new PrintWriter(sw);
            dataTearException.printStackTrace(pw);
            pw.flush();
            sw.flush();
        } finally {
            if (sw != null) {
                try {
                    sw.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
            if (pw != null) {
                pw.close();
            }
        }
        logger.error(sw.toString());
    }
}
