package zhao.io.dataTear.config;

/**
 * <h2>introduce</h2>
 * <hr>
 * <h3>中文</h3>
 * 初始化插件接口，在自带的客户端 MAINCli 类启动的时候，会调用初始化的所有插件，如果您只需要调用API，那么您不需要关注这个接口，因为它只在MAINCli中被使用
 *
 * <h3>English</h3>
 * Initialize the plug-in interface. When the built-in client MAINCli class starts, all the initialized plug-ins will be called.
 * If you only need to call the API, then you do not need to pay attention to this interface, because it is only used in MAINCli
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
