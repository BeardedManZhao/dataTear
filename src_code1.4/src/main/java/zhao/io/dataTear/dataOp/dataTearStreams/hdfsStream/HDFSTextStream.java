package zhao.io.dataTear.dataOp.dataTearStreams.hdfsStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import zhao.io.dataTear.dataOp.dataTearRW.Reader;
import zhao.io.dataTear.dataOp.dataTearRW.hdfs.HDFSReader;
import zhao.io.dataTear.dataOp.dataTearRW.hdfs.HDFSWriter;
import zhao.io.dataTear.dataOp.dataTearStreams.DT_StreamBase;

import java.io.IOException;
import java.io.OutputStream;

/**
 * 向HDFS上面输出文本的DT目录
 *
 * @author zhao
 */
public class HDFSTextStream implements DT_StreamBase {
    FileSystem fileSystem;

    /**
     * @param fileSystem 使用FileSystem连接，注意需要使用强转，转换为此类，因为该方法是本类的特有方法，接口中并不包含
     * @return 链
     */
    public HDFSTextStream setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
        return this;
    }

    /**
     * @param inPath 输出路径
     * @return HDFS写数据流
     */
    @Override
    public Reader readStream(String inPath) throws IOException {
        try {
            Path path = new Path(inPath);
            return HDFSReader.builder().setFileSystem(fileSystem).setPathObject(path).create().setInputStream(fileSystem.open(path));
        } catch (NullPointerException e) {
            String elog = "您好，您使用了 " + this.getClass().getName() + " 但是您传入的参数似乎为空哦！解决方案：转换为HDFSTextDStream，设置FileSystem然后获取readStream设置输入路径。";
            logger1.error(elog);
            throw new IOException(elog);
        }
    }

    /**
     * @param outPath 输出路径
     * @return HDFS写数据流
     */
    @Override
    public OutputStream writeStream(String outPath) throws IOException {
        try {
            return HDFSWriter.builder().setFileSystem(fileSystem).setPathString(outPath).create();
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IOException("您好，您使用了 " + this.getClass().getName() + " 但是您传入的参数似乎为空哦！解决方案：转换为HDFSTextDStream，设置FileSystem然后获取writeStream设置输出路径。");
        }
    }
}
