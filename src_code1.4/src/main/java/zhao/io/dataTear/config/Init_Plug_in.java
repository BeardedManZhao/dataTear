package zhao.io.dataTear.config;

/**
 * 初始化插件接口
 */
public interface Init_Plug_in {
    /**
     * @return 该插件的名称
     */
    String getName();

    /**
     * @return 是否允许继续运行
     */
    boolean run();
}
