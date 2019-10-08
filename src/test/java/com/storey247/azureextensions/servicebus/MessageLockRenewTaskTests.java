package com.storey247.azureextensions.servicebus;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.UUID;

import com.microsoft.azure.servicebus.IMessageReceiver;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;

import org.junit.Test;

public class MessageLockRenewTaskTests {
    @Test
    public void runAttemptsToRenewMessageLock() throws InterruptedException, ServiceBusException {
        UUID messageId = UUID.randomUUID();
        IMessageReceiver mockReceiver = mock(IMessageReceiver.class);

        MessageLockRenewTask sut = new MessageLockRenewTask(mockReceiver, messageId);

        sut.run();

        verify(mockReceiver).renewMessageLock(messageId);
    }

}