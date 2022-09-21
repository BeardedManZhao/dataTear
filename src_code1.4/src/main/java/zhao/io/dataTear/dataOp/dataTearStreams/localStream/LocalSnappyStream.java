package zhao.io.dataTear.dataOp.dataTearStreams.localStream;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;
import zhao.io.dataTear.dataOp.dataTearRW.Reader;
import zhao.io.dataTear.dataOp.dataTearStreams.DT_StreamBase;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * 内置实现的自定义流，Snappy压缩数据
 */
public class LocalSnappyStream implements DT_StreamBase {
    @Override
    public Reader readStream(String inPath) throws IOException {
        return new Reader().setInputStream(new SnappyInputStream(new FileInputStream(inPath)));
    }

    @Override
    public OutputStream writeStream(String outPath) throws IOException {
        return new SnappyOutputStream(new FileOutputStream(outPath));
    }
}
