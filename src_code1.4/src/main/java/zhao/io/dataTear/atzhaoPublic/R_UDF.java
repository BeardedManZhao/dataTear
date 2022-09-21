package zhao.io.dataTear.atzhaoPublic;

import zhao.io.dataTear.dataOp.dataTearRW.Reader;

import java.io.IOException;

/**
 *
 */
public interface R_UDF {
    /**
     * 数据输出组件的具体调用
     * <p>
     * 您可以通过这里的实现，将不同的数据输出组件对接进来
     * <p>
     * 还可以选择使用算法库，其中的算法数据组件是完善的，调用起来很方便 只需要通过 RW.getDT_UDF_Stream方法就可以获取到算法库的资源
     *
     * @param inPath 数据输入路径
     * @return 数据输入组件
     * @throws IOException 将数据输入组件对接到接口的时候，可能会发生一些异常
     */
    Reader run(String inPath) throws IOException;
}
