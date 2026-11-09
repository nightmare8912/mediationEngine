package org.prats.container;

// this class should be storing all the container processes running that the platform would keep track of (ECs)
public class ContainerProcess {
    private String processName; // ec name
    private Process process;    // reference to the ec process/jvm

    public String getProcessName() {
        return processName;
    }

    public void setProcessName(String processName) {
        this.processName = processName;
    }

    public Process getProcess() {
        return process;
    }

    public void setProcess(Process process) {
        this.process = process;
    }
}
