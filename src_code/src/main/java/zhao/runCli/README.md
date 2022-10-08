# runCli directory structure

DaraTear has a built-in client, which is optional. DataTear is mainly a framework, and the client is used for reference
only.

### [MAINCli](https://github.com/BeardedManZhao/dataTear/blob/core/src_code/src/main/java/zhao/runCli/MAINCli.java)

DataTear contains an implemented client, and the client's startup class is this class.

### [directive](https://github.com/BeardedManZhao/dataTear/tree/core/src_code/src/main/java/zhao/runCli/directive)

DataTear contains an implemented client. All the command processing modules in the client are in this package. At
present, they are two modules that read and write DataTear data. These two modules come from the same interface.
