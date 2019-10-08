package com.storey247.azureextensions.servicebus;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.IMessageReceiver;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class AutoRenewMessageReceiverTests {

    private static final UUID LOCK_TOKEN = UUID.randomUUID();

    @Mock
    ScheduledFuture<?> task;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void receiveAsync_given_serverWaitTime_returns_completableFutureIMessage()
            throws InterruptedException, ExecutionException {
        IMessage message = createMockMessage(Instant.MAX);
        IMessageReceiver receiver = mock(IMessageReceiver.class);
        ScheduledExecutorService service = createMockScheduledExecutorService();

        when(receiver.receiveAsync(Duration.ofSeconds(10))).thenReturn(CompletableFuture.completedFuture(message));

        AutoRenewMessageReceiver sut = new AutoRenewMessageReceiver(receiver, service);
        IMessage result = sut.receiveAsync(Duration.ofSeconds(10)).get();

        assertEquals(message, result);
        verify(receiver).receiveAsync(Duration.ofSeconds(10));
        verify(service).scheduleAtFixedRate(any(MessageLockRenewTask.class), anyLong(), anyLong(),
                eq(TimeUnit.SECONDS));
    }

    @Test
    public void receive_given_serverWaitTime_returns_completableFutureIMessage()
            throws InterruptedException, ExecutionException, ServiceBusException {
        IMessage message = createMockMessage(Instant.MAX);
        IMessageReceiver receiver = mock(IMessageReceiver.class);
        ScheduledExecutorService service = createMockScheduledExecutorService();

        when(receiver.receiveAsync(Duration.ofSeconds(10))).thenReturn(CompletableFuture.completedFuture(message));

        AutoRenewMessageReceiver sut = new AutoRenewMessageReceiver(receiver, service);
        IMessage result = sut.receive(Duration.ofSeconds(10));

        assertEquals(message, result);

        verify(receiver).receiveAsync(Duration.ofSeconds(10));
        verify(service).scheduleAtFixedRate(any(MessageLockRenewTask.class), anyLong(), anyLong(),
                eq(TimeUnit.SECONDS));
    }

    @Test
    public void renewMessageLockAsync_given_invalidLockTocken_returns_completableFutureInstant()
            throws InterruptedException, ExecutionException {
        IMessageReceiver receiver = mock(IMessageReceiver.class);
        ScheduledExecutorService service = createMockScheduledExecutorService();
        Instant instant = Instant.now();

        when(receiver.renewMessageLockAsync(LOCK_TOKEN)).thenReturn(CompletableFuture.completedFuture(instant));

        AutoRenewMessageReceiver sut = new AutoRenewMessageReceiver(receiver, service);
        Instant result = sut.renewMessageLockAsync(LOCK_TOKEN).get();

        assertEquals(instant, result);

        verify(receiver).renewMessageLockAsync(LOCK_TOKEN);
        verify(service).scheduleAtFixedRate(any(MessageLockRenewTask.class), anyLong(), anyLong(),
                eq(TimeUnit.SECONDS));
    }

    @Test
    public void receiveBatchAsync_given_maxMessageCount_returns_completableFutureMessageCollection()
            throws InterruptedException, ExecutionException {
        IMessageReceiver receiver = mock(IMessageReceiver.class);
        ScheduledExecutorService service = createMockScheduledExecutorService();
        IMessage message = createMockMessage(Instant.MAX);
        Collection<IMessage> messages = Arrays.asList(message, message, message);

        when(receiver.receiveBatchAsync(10)).thenReturn(CompletableFuture.completedFuture(messages));

        AutoRenewMessageReceiver sut = new AutoRenewMessageReceiver(receiver, service);
        Collection<IMessage> result = sut.receiveBatchAsync(10).get();

        assertEquals(messages, result);

        verify(receiver).receiveBatchAsync(10);
        verify(service, times(messages.size())).scheduleAtFixedRate(any(MessageLockRenewTask.class), anyLong(),
                anyLong(), eq(TimeUnit.SECONDS));
    }

    @Test
    public void deadLetterAsync_given_lockToken_returns_completableFuture()
            throws InterruptedException, ExecutionException {
        IMessageReceiver receiver = mock(IMessageReceiver.class);
        ScheduledExecutorService service = createMockScheduledExecutorService();
        IMessage message = createMockMessage(Instant.MAX);

        when(receiver.receiveAsync()).thenReturn(CompletableFuture.completedFuture(message));
        when(receiver.deadLetterAsync(LOCK_TOKEN)).thenReturn(CompletableFuture.completedFuture(null));

        AutoRenewMessageReceiver sut = new AutoRenewMessageReceiver(receiver, service);
        IMessage result = sut.receiveAsync().get();
        sut.deadLetterAsync(result.getLockToken()).get();

        verify(receiver).receiveAsync();
        verify(receiver).deadLetterAsync(LOCK_TOKEN);
        verify(service).scheduleAtFixedRate(any(MessageLockRenewTask.class), anyLong(), anyLong(),
                eq(TimeUnit.SECONDS));
        verify(task).cancel(false); // make sure the timer cancels too
    }

    private IMessage createMockMessage(Instant lockedUntil) {
        IMessage message = mock(IMessage.class);
        when(message.getLockToken()).thenReturn(LOCK_TOKEN);
        when(message.getLockedUntilUtc()).thenReturn(lockedUntil);
        return message;
    }

    private ScheduledExecutorService createMockScheduledExecutorService() {
        ScheduledExecutorService service = mock(ScheduledExecutorService.class);

        Mockito.doReturn(task).when(service).scheduleAtFixedRate(any(MessageLockRenewTask.class), anyLong(), anyLong(),
                eq(TimeUnit.SECONDS));

        return service;
    }
}