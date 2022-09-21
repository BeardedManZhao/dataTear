package zhao.io.dataTear.dataOp.dataTearStreams.localStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import zhao.io.dataTear.dataOp.dataTearRW.Reader;
import zhao.io.dataTear.dataOp.dataTearStreams.DT_StreamBase;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * 内置实现的自定义流，BZIP2压缩数据
 */
public class LocalBZIP2Stream implements DT_StreamBase {

    @Override
    public Reader readStream(String inPath) throws IOException {
        return new Reader().setInputStream(new BZip2CompressorInputStream(new FileInputStream(inPath)));
    }

    @Override
    public OutputStream writeStream(String outPath) throws IOException {
        return new BZip2CompressorOutputStream(new FileOutputStream(outPath));
    }
}
