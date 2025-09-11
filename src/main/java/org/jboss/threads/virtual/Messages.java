package org.jboss.threads.virtual;

import static java.lang.invoke.MethodHandles.lookup;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

@MessageLogger(projectCode = "JBVT", length = 4)
interface Messages extends BasicLogger {
    Messages msg = Logger.getMessageLogger(lookup(), Messages.class, "org.jboss.threads.virtual");

    @Message(id = 1, value = "Event loop has already been started")
    IllegalStateException eventLoopEntered();
}
