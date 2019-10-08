package com.storey247.azureextensions.servicebus;

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.azure.servicebus.IMessageReceiver;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;

public class MessageLockRenewTask implements Runnable {

    private static final Logger log = Logger.getLogger(MessageLockRenewTask.class.getName());

    private final IMessageReceiver receiver;
    private final UUID lockToken;

    public MessageLockRenewTask(IMessageReceiver receiver, UUID lockToken) {
        this.receiver = receiver;
        this.lockToken = lockToken;
    }

    @Override
    public void run() {
        try {
            this.receiver.renewMessageLock(lockToken);
        } catch (InterruptedException e) {
            // probably not worth doing anything here
        } catch (ServiceBusException e) {
            if (log != null) {
                log.log(Level.SEVERE, String.format("Exception while attempting to renew lock : %s", lockToken), e);
            }
        }
    }
} 