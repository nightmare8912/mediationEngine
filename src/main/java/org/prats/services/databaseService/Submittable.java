package org.prats.services.databaseService;

import org.prats.definitions.Message;

import java.io.IOException;

public interface Submittable {
    void submit(Message message);
}
