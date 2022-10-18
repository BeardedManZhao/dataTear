# directive directory structure

### [Execute](https://github.com/BeardedManZhao/dataTear/blob/core/src_code/src/main/java/zhao/runCli/directive/Execute.java)

Command processing class interface. All command execution modules are the implementation of this interface, which is a
necessary part of the client.

### [ZHAOPut](https://github.com/BeardedManZhao/dataTear/blob/core/src_code/src/main/java/zhao/runCli/directive/ZHAOPut.java)

DataTear data output module, which can achieve perfect processing logic for data output commands. Note: the client is
prefabricated, and you can take it as an example, mainly for API operations.

### [ZHAOSet](https://github.com/BeardedManZhao/dataTear/blob/core/src_code/src/main/java/zhao/runCli/directive/ZHAOSet.java)

DataTear client module is a class that operates on the configuration information base. The configuration information is
exclusive to the client, and you do not need to use this module in the API.
