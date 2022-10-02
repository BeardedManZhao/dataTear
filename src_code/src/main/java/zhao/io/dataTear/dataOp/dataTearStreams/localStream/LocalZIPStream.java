package zhao.io.dataTear.dataOp.dataTearStreams.localStream;

import zhao.io.dataTear.dataOp.dataTearRW.Reader;
import zhao.io.dataTear.dataOp.dataTearStreams.DT_StreamBase;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * 内置实现的自定义流，ZIP压缩数据
 */
public class LocalZIPStream implements DT_StreamBase {

    @Override
    public Reader readStream(String inPath) throws IOException {
        ZipFile zipFile = new ZipFile(inPath);
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        ZipEntry zipEntry = entries.hasMoreElements() ? entries.nextElement() : null;
        if (zipEntry != null) {
            return new Reader().setInputStream(zipFile.getInputStream(zipEntry));
        } else {
            logger1.error("内置的ZIP数据输入组件运行了，但是发生了空指针异常。");
            return null;
        }
    }

    @Override
    public OutputStream writeStream(String outPath) throws IOException {
        ZipOutputStream zipOutputStream = new ZipOutputStream(new FileOutputStream(outPath));
        zipOutputStream.putNextEntry(new ZipEntry("DT-ZHAO"));
        return zipOutputStream;
    }
}
