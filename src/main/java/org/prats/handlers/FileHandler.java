package org.prats.handlers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileHandler {
    private String path;

    public FileHandler() {}

    public FileHandler(String path) {
        this.path = path;
    }

    public String openAndRead() {
        String text;
        try
        {
            text = Files.readString(
                    Path.of(path)
            );
        } catch (IOException ioe)
        {
            text = ioe.getStackTrace().toString();
        }
        return text;
    }

    public String openAndRead(String path) {
        this.path = path;
        return openAndRead();
    }
}
