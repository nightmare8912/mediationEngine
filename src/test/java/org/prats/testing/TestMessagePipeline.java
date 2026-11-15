package org.prats.testing;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.ProcessType;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.interfaces.*;
import org.prats.forwarders.*;
import org.prats.handlers.*;
import org.prats.parsers.*;
import org.prats.processors.*;
import org.prats.registries.*;
import org.prats.replyChannels.FutureReplyChannel;
import org.prats.security.AccountManager;
import org.prats.services.authenticationService.AuthenticationService;
import org.prats.services.databaseService.DatabaseService;
import org.prats.services.guiService.GUIService;
import org.prats.services.messageService.MessageService;
import java.util.*;
import java.util.concurrent.*;

/** Real in-process service pipeline with an isolated H2 database. */
public final class TestMessagePipeline implements AutoCloseable {
    public final TestDatabase database = new TestDatabase();
    public final MessageParser parser;
    public final CorrelationRegistry correlations = new CorrelationRegistry();
    public final RetryRegistry retries = new RetryRegistry(0, 30);
    public final MessageService messages;
    public final AccountManager accounts;
    public record Route(Operation operation, UUID correlation, boolean accountDatabase) {}
    public final List<Route> routes = new CopyOnWriteArrayList<>();
    public final MessageDispatcher dispatcher;
    public final GUIHandler gui;
    public final MessageProcessor processor;
    private final DatabaseService databaseService;
    private final AuthenticationService authentication;
    private final GUIService guiService;

    public TestMessagePipeline() {
        TestDatabase.context();
        parser = new MessageParser(ProcessType.PLATFORM);
        DatabaseRequestHandler handler = new DatabaseRequestHandler(database);
        databaseService = new DatabaseService(0, 0, 0, database, handler);
        processor = new MessageProcessor(new DatabaseForwarder(databaseService));
        ResponseProcessor responses = new ResponseProcessor(parser, correlations, retries);
        CommandProcessor commands = new CommandProcessor(new CommandParser(ProcessType.PLATFORM), responses, Set.of("help", "status"));
        RequestProcessor requests = new RequestProcessor(correlations, parser, commands, processor);
        commands.setRequestProcessor(requests);
        messages = new MessageService(0, 0, 0, 60, 60, 60, requests, responses, correlations, retries);
        databaseService.setMessageDispatcher(messages);
        dispatcher = new MessageDispatcher() {
            @Override public void dispatchRequest(Message request, ReplyChannel channel) {
                routes.add(new Route(request.getOperation(), request.getCorrelationId(), request.isAccountDatabaseRequest()));
                messages.dispatchRequest(request, channel);
            }
            @Override public void dispatchResponse(Message response) { messages.dispatchResponse(response); }
        };
        accounts = new AccountManager(dispatcher, parser);
        authentication = new AuthenticationService(new AuthenticationHandler(accounts, parser), messages);
        processor.setAuthenticationForwarder(new AuthenticationForwarder(authentication));
        gui = new GUIHandler(dispatcher, parser, List::of);
        guiService = new GUIService(gui, messages);
        processor.setGUIForwarder(new GUIForwarder(guiService));
        databaseService.start();
        messages.start();
        authentication.start();
        guiService.start();
    }

    public CompletableFuture<Message> request(Message request) {
        CompletableFuture<Message> result = new CompletableFuture<>();
        dispatcher.dispatchRequest(request, new FutureReplyChannel(result));
        return result.orTimeout(5, TimeUnit.SECONDS);
    }
    @Override public void close() {
        guiService.shutdown();
        authentication.shutdown();
        databaseService.shutdown();
        messages.shutdown();
    }
}
