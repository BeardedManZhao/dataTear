package zhao.io.dataTear.dataOp.dataTearStreams.localStream;

import zhao.io.dataTear.dataOp.dataTearRW.Reader;
import zhao.io.dataTear.dataOp.dataTearStreams.DT_StreamBase;

import java.io.*;

/**
 * 从本地文件系统读取text存储的数据
 */
public class LocalBufferStream implements DT_StreamBase {

    @Override
    public Reader readStream(String inPath) throws IOException {
        return new Reader().setInputReaderStream(new BufferedReader(new FileReader(inPath)));
    }

    @Override
    public OutputStream writeStream(String outPath) throws IOException {
        return new BufferedOutputStream(new FileOutputStream(outPath));
    }
}
