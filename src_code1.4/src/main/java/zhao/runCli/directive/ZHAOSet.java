package zhao.runCli.directive;

import zhao.io.dataTear.config.ConfigBase;
import zhao.io.ex.CommandParsingException;

/**
 * 更改配置文件信息
 */
public class ZHAOSet implements Execute {
    @Override
    public String getHelp() {
        return "set 【配置项】【配置参数】";
    }

    @Override
    public String GETCommand_NOE() {
        return "set";
    }

    @Override
    public boolean open(String[] args) {
        try {
            ConfigBase.conf.put(args[1], args[2]);
        } catch (ArrayIndexOutOfBoundsException | NullPointerException e) {
            throw new CommandParsingException("您的put格式命令不正确，请改正为：" + getHelp());
        }
        return true;
    }

    @Override
    public boolean run() {
        return true;
    }

    @Override
    public boolean close() {
        return true;
    }
}
