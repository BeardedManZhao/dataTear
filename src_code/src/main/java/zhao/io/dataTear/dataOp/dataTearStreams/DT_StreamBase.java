package zhao.io.dataTear.dataOp.dataTearStreams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zhao.io.dataTear.dataOp.dataTearRW.Reader;

import java.io.IOException;
import java.io.OutputStream;

/**
 * 系统实现好的自定义组件接口 由系统内部调用，不需要使用者去实现
 * 数据算法流库中所有流的统一集成接口，用于在外界访问算法库
 */
public interface DT_StreamBase {

    Logger logger1 = LoggerFactory.getLogger("DT_StreamsUtils");

    /**
     * @param inPath 数据输入流的源路径
     * @return 组件数据输入流
     * @throws IOException 数据输入流未成功被打开
     */
    Reader readStream(String inPath) throws IOException;

    /**
     * @param outPath 数据输出流目标
     * @return 组件数据输出流
     * @throws IOException 数据输出流未成功被打开
     */
    OutputStream writeStream(String outPath) throws IOException;
}
