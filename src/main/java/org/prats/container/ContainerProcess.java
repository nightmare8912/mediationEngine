package org.prats.container;

public record ContainerProcess(
        String processName,
        Process process
) {}