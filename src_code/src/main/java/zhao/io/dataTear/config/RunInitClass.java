package zhao.io.dataTear.config;

/**
 * 客户端初始化插件加载器   Client initialization plugin loader
 */
public class RunInitClass {

    public static class R extends LoadClass {
//        Logger log = LoggerFactory.getLogger("插件初始化器");

        /**
         * 通过插件目录构建初始化运行器  Build the init runner from the plugin directory
         *
         * @param jarPath 插件路径（目录） plugin directory
         */
        public R(String jarPath) {
            super(jarPath);
        }

        /**
         * 运行所有的插件初始化方法  Run all plugin initialization methods
         *
         * @param classPath 所有插件的路径(包路径)  Path to all plugins (package path)
         * @return 是否初始化成功，初始化失败将会强制终止该方法的执行，同时返回false
         * <p>
         * Whether the initialization is successful or not, the initialization failure will force the execution of the method to terminate and return false
         */
        public boolean runAllClass(String[] classPath) {
            try {
                main(classPath);
                for (Init_Plug_in plug_in : LoadClass.objectArrayList) {
                    System.out.println("* >>> 加载插件：" + plug_in.getName());
                    if (!plug_in.run()) return false;
                }
                return true;
            } catch (Exception e) {
                System.err.println("* >？> 发生错误：" + e);
                return false;
            }
        }
    }
}
