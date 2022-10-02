package zhao.io.dataTear.dataOp.dataTearStreams.hdfsStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import zhao.io.dataTear.dataOp.dataTearRW.Reader;
import zhao.io.dataTear.dataOp.dataTearRW.hdfs.HDFSReaderGZIP;
import zhao.io.dataTear.dataOp.dataTearRW.hdfs.HDFSWriterGZIP;
import zhao.io.dataTear.dataOp.dataTearStreams.DT_StreamBase;
import zhao.io.ex.ZHAOLackOfInformation;

import java.io.IOException;
import java.io.OutputStream;

/**
 * 向HDFS上面输出ZIP的DT目录
 *
 * @author zhao
 */
public class HDFSGZIPStream implements DT_StreamBase {
    FileSystem fileSystem;

    /**
     * @param fileSystem 使用FileSystem连接，注意需要使用强转，转换为此类，因为该方法是本类的特有方法，接口中并不包含
     * @return 链
     */
    public HDFSGZIPStream setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
        return this;
    }

    @Override
    public Reader readStream(String inPath) throws IOException {
        try {
            if (fileSystem == null) {
                throw new NullPointerException();
            }
            Path path = new Path(inPath);
            return HDFSReaderGZIP
                    .builder()
                    .setFileSystem(fileSystem)
                    .setPathObject(path)
                    .setPathString(inPath)
                    .create()
                    .setInputStream(fileSystem.open(path));
        } catch (NullPointerException e) {
            String elog = "您好，您使用了 " + this.getClass().getName() + " 但是您传入的参数似乎为空哦！解决方案：转换为HDFSGZIPStream，设置FileSystem然后获取readStream设置输入路径。";
            logger1.error(elog);
            throw new ZHAOLackOfInformation(elog);
        }
    }

    @Override
    public OutputStream writeStream(String outPath) throws IOException {
        try {
            if (fileSystem == null) {
                throw new ZHAOLackOfInformation("");
            }
            return HDFSWriterGZIP
                    .builder().
                    setFileSystem(fileSystem)
                    .setPathObject(new Path(outPath))
                    .create();
        } catch (ZHAOLackOfInformation e) {
            String elog = "您好，您使用了 " + this.getClass().getName() + " 但是您传入的参数似乎为空哦！解决方案：转换为HDFSGZIPStream，设置FileSystem然后获取WriteStream设置输入路径。";
            logger1.error(elog);
            throw new ZHAOLackOfInformation(elog);
        }
    }
}
