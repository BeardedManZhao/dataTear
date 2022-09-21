package zhao.io.dataTear.dataOp.dataTearRW;

import zhao.io.dataTear.dataOp.dataTearStreams.DT_StreamBase;
import zhao.io.dataTear.dataOp.dataTearStreams.DT_builtIn_UDF;
import zhao.io.dataTear.dataOp.dataTearStreams.dbStream.DataBaseStream;
import zhao.io.dataTear.dataOp.dataTearStreams.hdfsStream.*;
import zhao.io.dataTear.dataOp.dataTearStreams.localStream.*;

import java.io.IOException;

/**
 * 数据读写组件接口 是本系统的Reader 与 DTMaster超接口，可以用来对接第三方各种程序
 *
 * @author 赵凌宇
 * @version 1.0
 */
public interface RW {

    /**
     * 算法库接口
     * DTfs_StreamBase算法库接口的对接者，这里便是提供对应数据算法流的库
     *
     * @param DTfs_streamName 组件类型 使用哪种方式操作DT数据目录
     * @return 流 会被系统插入，这里是内置的数据算法流组件
     * @see DT_StreamBase
     */
    static DT_StreamBase getDT_UDF_Stream(DT_builtIn_UDF DTfs_streamName) {
        DT_StreamBase DTfs_stream;
        switch (DTfs_streamName) {
            case LOCAL_TEXT:
                DTfs_stream = new LocalBufferStream();
                break;
            case HDFS_TEXT:
                DTfs_stream = new HDFSTextStream();
                break;
            case HDFS_ZIP:
                DTfs_stream = new HDFSZIPStream();
                break;
            case HDFS_GZIP:
                DTfs_stream = new HDFSGZIPStream();
                break;
            case HDFS_BZIP2:
                DTfs_stream = new HDFSBZIP2Stream();
                break;
            case HDFS_SNAPPY:
                DTfs_stream = new HDFSSnappyStream();
                break;
            case LOCAL_ZIP:
                DTfs_stream = new LocalZIPStream();
                break;
            case LOCAL_GZIP:
                DTfs_stream = new LocalGZIPStream();
                break;
            case LOCAL_BZIP2:
                DTfs_stream = new LocalBZIP2Stream();
                break;
            case LOCAL_SNAPPY:
                DTfs_stream = new LocalSnappyStream();
                break;
            case SQLDB_TEXT:
                DTfs_stream = new DataBaseStream();
                break;
            default: {
                Reader.logger.error("系统中没有您需要的组件哦！请重新设置 getDT_UDF_Stream() 的参数。");
                return null;
            }
        }
        Reader.logger.info("DT文件存储内置数据算法流库被访问了，分配适用于 " + DTfs_streamName.name() + " 算法的数据流组件: " + DTfs_stream);
        return DTfs_stream;
    }


    /**
     * @return 是否成功打开数据流
     */
    boolean openStream();

    /**
     * @return 是否成功对数据进行操作
     */
    boolean op_Data();

    /**
     * @return 是否成功关闭数据流
     * @throws IOException 关闭流失败
     */
    boolean closeStream() throws IOException;
}
