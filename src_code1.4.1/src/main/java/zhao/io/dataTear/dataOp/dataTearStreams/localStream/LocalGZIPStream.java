package zhao.io.dataTear.dataOp.dataTearStreams.localStream;

import zhao.io.dataTear.dataOp.dataTearRW.Reader;
import zhao.io.dataTear.dataOp.dataTearStreams.DT_StreamBase;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * 内置实现的自定义流，GZIP压缩数据
 */
public class LocalGZIPStream implements DT_StreamBase {

    @Override
    public Reader readStream(String inPath) throws IOException {
        return new Reader().setInputStream(new GZIPInputStream(new FileInputStream(inPath)));
    }

    @Override
    public OutputStream writeStream(String outPath) throws IOException {
        return new GZIPOutputStream(new FileOutputStream(outPath));
    }
}
