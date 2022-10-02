package zhao.io.dataTear.dataOp.dataTearStreams;

import zhao.io.dataTear.dataOp.dataTearRW.Writer;

import java.io.IOException;
import java.io.OutputStream;

/**
 * 构建DT目录的数据流组件，框架内部调用
 * 父接口中提供了一个关键的方法，获取路径，这样实现好的数据流中就可以直接提取路径了，灵活性更强大
 */
public class DTWrite extends Writer {

    private final OutputStream outputStream;
    private final String outPath;

    public DTWrite(OutputStream outputStream, String outPath) {
        this.outputStream = outputStream;
        this.outPath = outPath;
    }

    public static DTBulider bulider() {
        return new DTBulider();
    }

    @Override
    public String getPath() {
        return outPath;
    }

    @Override
    public void write(int b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        outputStream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        outputStream.flush();
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }

    @Override
    public DTWrite toTobject() {
        return this;
    }
}
