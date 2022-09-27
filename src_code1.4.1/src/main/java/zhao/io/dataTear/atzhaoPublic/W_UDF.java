package zhao.io.dataTear.atzhaoPublic;

import java.io.IOException;
import java.io.OutputStream;

/**
 * <h2>introduce</h2>
 * <hr>
 * <h3>中文</h3>
 * 自定义数据输出组件的实现类，会被DataTear调用，使用者可以通过该接口将数据输出类插入到DataTear中
 * 如果DataTear组件的输出模式是UDF，那么这个方法将会被调用
 * <br>
 *
 * <h3>English</h3>
 * The implementation class of the custom data output component will be called by DataTear. Users can insert the data output class into DataTear through this interface.
 * If the output mode of the DataTear component is UDF, this method will be called
 */
public interface W_UDF {
    /**
     * 数据输出组件的具体调用
     * <p>
     * 您可以通过这里的实现，将不同的数据输出组件对接进来
     * <p>
     * 还可以选择使用算法库，其中的算法数据组件是完善的，调用起来很方便 只需要通过 RW.getDT_UDF_Stream方法就可以获取到算法库的资源
     *
     * @param outPath 数据输出路径
     * @return 数据输出流或其子类
     * @throws IOException 数据输出组件的对接可能会发生异常
     */
    OutputStream run(String outPath) throws IOException;
}
