package com.storey247.azureextensions.servicebus;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.microsoft.azure.servicebus.ClientFactory;
import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.IMessageReceiver;
import com.microsoft.azure.servicebus.ReceiveMode;
import com.microsoft.azure.servicebus.TransactionContext;
import com.microsoft.azure.servicebus.Utils;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;

public final class AutoRenewMessageReceiver implements IMessageReceiver {

    private static final ScheduledThreadPoolExecutor DEFAULT_SCHEDULER = new ScheduledThreadPoolExecutor(10);

    static {
        DEFAULT_SCHEDULER.setRemoveOnCancelPolicy(true);
    }

    private final IMessageReceiver internalReceiver;
    private final ScheduledExecutorService internalScheduler;
    private final ConcurrentHashMap<UUID, ScheduledFuture<?>> renewLockTaskMessageIdMap = new ConcurrentHashMap<>();

    public AutoRenewMessageReceiver(String namespaceConnectionString, String entityPath)
            throws InterruptedException, ServiceBusException {
        this(new ConnectionStringBuilder(namespaceConnectionString, entityPath));
    }

    public AutoRenewMessageReceiver(String namespaceConnectionString, String entityPath, ReceiveMode receiveMode)
            throws InterruptedException, ServiceBusException {
        this(new ConnectionStringBuilder(namespaceConnectionString, entityPath), DEFAULT_SCHEDULER, receiveMode);
    }

    public AutoRenewMessageReceiver(String namespaceConnectionString, String entityPath,
            ScheduledExecutorService scheduler, ReceiveMode receiveMode)
            throws InterruptedException, ServiceBusException {
        this(new ConnectionStringBuilder(namespaceConnectionString, entityPath), scheduler, receiveMode);
    }

    public AutoRenewMessageReceiver(ConnectionStringBuilder connectionStringBuilder)
            throws InterruptedException, ServiceBusException {
        this(connectionStringBuilder, DEFAULT_SCHEDULER, ReceiveMode.PEEKLOCK);
    }

    public AutoRenewMessageReceiver(ConnectionStringBuilder connectionStringBuilder, ScheduledExecutorService scheduler,
            ReceiveMode receiveMode) throws InterruptedException, ServiceBusException {
        internalReceiver = ClientFactory.createMessageReceiverFromConnectionStringBuilder(connectionStringBuilder,
                receiveMode);
        internalScheduler = scheduler;
    }

    public AutoRenewMessageReceiver(IMessageReceiver receiver) {
        this(receiver, DEFAULT_SCHEDULER);
    }

    public AutoRenewMessageReceiver(IMessageReceiver receiver, ScheduledExecutorService scheduler) {
        internalReceiver = receiver;
        internalScheduler = scheduler;
    }

    @Override
    public String getEntityPath() {
        return internalReceiver.getEntityPath();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        purgeSchedule();
        return internalReceiver.closeAsync();
    }

    @Override
    public void close() throws ServiceBusException {
        purgeSchedule();
        internalReceiver.close();
    }

    @Override
    public IMessage peek() throws InterruptedException, ServiceBusException {
        return Utils.completeFuture(peekAsync());
    }

    @Override
    public IMessage peek(long fromSequenceNumber) throws InterruptedException, ServiceBusException {
        return Utils.completeFuture(peekAsync(fromSequenceNumber));
    }

    @Override
    public Collection<IMessage> peekBatch(int messageCount) throws InterruptedException, ServiceBusException {
        return Utils.completeFuture(peekBatchAsync(messageCount));

    }

    @Override
    public Collection<IMessage> peekBatch(long fromSequenceNumber, int messageCount)
            throws InterruptedException, ServiceBusException {
        return Utils.completeFuture(peekBatchAsync(fromSequenceNumber, messageCount));
    }

    @Override
    public CompletableFuture<IMessage> peekAsync() {
        return internalReceiver.peekAsync().thenApplyAsync(c -> scheduleLockRenewal(c));
    }

    @Override
    public CompletableFuture<IMessage> peekAsync(long fromSequenceNumber) {
        return internalReceiver.peekAsync(fromSequenceNumber).thenApplyAsync(c -> scheduleLockRenewal(c));
    }

    @Override
    public CompletableFuture<Collection<IMessage>> peekBatchAsync(int messageCount) {
        return internalReceiver.peekBatchAsync(messageCount).thenApplyAsync(c -> scheduleLockRenewal(c));
    }

    @Override
    public CompletableFuture<Collection<IMessage>> peekBatchAsync(long fromSequenceNumber, int messageCount) {
        return internalReceiver.peekBatchAsync(fromSequenceNumber, messageCount)
                .thenApplyAsync(c -> scheduleLockRenewal(c));
    }

    @Override
    public ReceiveMode getReceiveMode() {
        return internalReceiver.getReceiveMode();
    }

    @Override
    public void abandon(UUID lockToken) throws InterruptedException, ServiceBusException {
        Utils.completeFuture(abandonAsync(lockToken));
    }

    @Override
    public void abandon(UUID lockToken, TransactionContext transaction)
            throws InterruptedException, ServiceBusException {
        Utils.completeFuture(abandonAsync(lockToken, transaction));
    }

    @Override
    public void abandon(UUID lockToken, Map<String, Object> propertiesToModify)
            throws InterruptedException, ServiceBusException {
        Utils.completeFuture(abandonAsync(lockToken, propertiesToModify));
    }

    @Override
    public void abandon(UUID lockToken, Map<String, Object> propertiesToModify, TransactionContext transaction)
            throws InterruptedException, ServiceBusException {
        Utils.completeFuture(abandonAsync(lockToken, propertiesToModify, transaction));
    }

    @Override
    public CompletableFuture<Void> abandonAsync(UUID lockToken) {
        return internalReceiver.abandonAsync(lockToken).thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public CompletableFuture<Void> abandonAsync(UUID lockToken, TransactionContext transaction) {
        return internalReceiver.abandonAsync(lockToken, transaction).thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public CompletableFuture<Void> abandonAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
        return internalReceiver.abandonAsync(lockToken, propertiesToModify).thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public CompletableFuture<Void> abandonAsync(UUID lockToken, Map<String, Object> propertiesToModify,
            TransactionContext transaction) {
        return internalReceiver.abandonAsync(lockToken, propertiesToModify, transaction)
                .thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public void complete(UUID lockToken) throws InterruptedException, ServiceBusException {
        Utils.completeFuture(completeAsync(lockToken));
    }

    @Override
    public void complete(UUID lockToken, TransactionContext transaction)
            throws InterruptedException, ServiceBusException {
        Utils.completeFuture(completeAsync(lockToken, transaction));
    }

    @Override
    public CompletableFuture<Void> completeAsync(UUID lockToken) {
        return internalReceiver.completeAsync(lockToken).thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public CompletableFuture<Void> completeAsync(UUID lockToken, TransactionContext transaction) {
        return internalReceiver.completeAsync(lockToken, transaction).thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public void defer(UUID lockToken) throws InterruptedException, ServiceBusException {
        Utils.completeFuture(deferAsync(lockToken));
    }

    @Override
    public void defer(UUID lockToken, TransactionContext transaction) throws InterruptedException, ServiceBusException {
        Utils.completeFuture(deferAsync(lockToken, transaction));
    }

    @Override
    public void defer(UUID lockToken, Map<String, Object> propertiesToModify)
            throws InterruptedException, ServiceBusException {
        Utils.completeFuture(deferAsync(lockToken, propertiesToModify));
    }

    @Override
    public void defer(UUID lockToken, Map<String, Object> propertiesToModify, TransactionContext transaction)
            throws InterruptedException, ServiceBusException {
        Utils.completeFuture(deferAsync(lockToken, propertiesToModify, transaction));
    }

    @Override
    public CompletableFuture<Void> deferAsync(UUID lockToken) {
        return internalReceiver.deferAsync(lockToken).thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public CompletableFuture<Void> deferAsync(UUID lockToken, TransactionContext transaction) {
        return internalReceiver.deferAsync(lockToken, transaction).thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public CompletableFuture<Void> deferAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
        return internalReceiver.deferAsync(lockToken, propertiesToModify).thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public CompletableFuture<Void> deferAsync(UUID lockToken, Map<String, Object> propertiesToModify,
            TransactionContext transaction) {
        return internalReceiver.deferAsync(lockToken, propertiesToModify, transaction)
                .thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public void deadLetter(UUID lockToken) throws InterruptedException, ServiceBusException {
        Utils.completeFuture(deadLetterAsync(lockToken));
    }

    @Override
    public void deadLetter(UUID lockToken, TransactionContext transaction)
            throws InterruptedException, ServiceBusException {
        Utils.completeFuture(deadLetterAsync(lockToken, transaction));
    }

    @Override
    public void deadLetter(UUID lockToken, Map<String, Object> propertiesToModify)
            throws InterruptedException, ServiceBusException {
        Utils.completeFuture(deadLetterAsync(lockToken, propertiesToModify));
    }

    @Override
    public void deadLetter(UUID lockToken, Map<String, Object> propertiesToModify, TransactionContext transaction)
            throws InterruptedException, ServiceBusException {
        Utils.completeFuture(deadLetterAsync(lockToken, propertiesToModify));
    }

    @Override
    public void deadLetter(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription)
            throws InterruptedException, ServiceBusException {
        Utils.completeFuture(deadLetterAsync(lockToken, deadLetterReason, deadLetterErrorDescription));
    }

    @Override
    public void deadLetter(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription,
            TransactionContext transaction) throws InterruptedException, ServiceBusException {
        Utils.completeFuture(
                internalReceiver.deadLetterAsync(lockToken, deadLetterReason, deadLetterErrorDescription, transaction));
    }

    @Override
    public void deadLetter(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription,
            Map<String, Object> propertiesToModify) throws InterruptedException, ServiceBusException {
        Utils.completeFuture(
                deadLetterAsync(lockToken, deadLetterReason, deadLetterErrorDescription, propertiesToModify));
    }

    @Override
    public void deadLetter(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription,
            Map<String, Object> propertiesToModify, TransactionContext transaction)
            throws InterruptedException, ServiceBusException {
        Utils.completeFuture(deadLetterAsync(lockToken, deadLetterReason, deadLetterErrorDescription,
                propertiesToModify, transaction));
    }

    @Override
    public CompletableFuture<Void> deadLetterAsync(UUID lockToken) {
        return internalReceiver.deadLetterAsync(lockToken).thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public CompletableFuture<Void> deadLetterAsync(UUID lockToken, TransactionContext transaction) {
        return internalReceiver.deadLetterAsync(lockToken, transaction).thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public CompletableFuture<Void> deadLetterAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
        return internalReceiver.deadLetterAsync(lockToken, propertiesToModify)
                .thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public CompletableFuture<Void> deadLetterAsync(UUID lockToken, Map<String, Object> propertiesToModify,
            TransactionContext transaction) {
        return internalReceiver.deadLetterAsync(lockToken, propertiesToModify, transaction)
                .thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public CompletableFuture<Void> deadLetterAsync(UUID lockToken, String deadLetterReason,
            String deadLetterErrorDescription) {
        return internalReceiver.deadLetterAsync(lockToken, deadLetterReason, deadLetterErrorDescription)
                .thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public CompletableFuture<Void> deadLetterAsync(UUID lockToken, String deadLetterReason,
            String deadLetterErrorDescription, TransactionContext transaction) {
        return internalReceiver.deadLetterAsync(lockToken, deadLetterReason, deadLetterErrorDescription, transaction)
                .thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public CompletableFuture<Void> deadLetterAsync(UUID lockToken, String deadLetterReason,
            String deadLetterErrorDescription, Map<String, Object> propertiesToModify) {
        return internalReceiver
                .deadLetterAsync(lockToken, deadLetterReason, deadLetterErrorDescription, propertiesToModify)
                .thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public CompletableFuture<Void> deadLetterAsync(UUID lockToken, String deadLetterReason,
            String deadLetterErrorDescription, Map<String, Object> propertiesToModify, TransactionContext transaction) {
        return internalReceiver.deadLetterAsync(lockToken, deadLetterReason, deadLetterErrorDescription,
                propertiesToModify, transaction).thenRun(() -> cancelLockRenewal(lockToken));
    }

    @Override
    public IMessage receive() throws InterruptedException, ServiceBusException {
        return Utils.completeFuture(receiveAsync());
    }

    @Override
    public IMessage receive(Duration serverWaitTime) throws InterruptedException, ServiceBusException {
        return Utils.completeFuture(receiveAsync(serverWaitTime));
    }

    @Override
    public IMessage receiveDeferredMessage(long sequenceNumber) throws InterruptedException, ServiceBusException {
        return Utils.completeFuture(receiveDeferredMessageAsync(sequenceNumber));
    }

    @Override
    public Collection<IMessage> receiveBatch(int maxMessageCount) throws InterruptedException, ServiceBusException {
        return Utils.completeFuture(receiveBatchAsync(maxMessageCount));
    }

    @Override
    public Collection<IMessage> receiveBatch(int maxMessageCount, Duration serverWaitTime)
            throws InterruptedException, ServiceBusException {
        return Utils.completeFuture(receiveBatchAsync(maxMessageCount, serverWaitTime));
    }

    @Override
    public Collection<IMessage> receiveDeferredMessageBatch(Collection<Long> sequenceNumbers)
            throws InterruptedException, ServiceBusException {
        return Utils.completeFuture(receiveDeferredMessageBatchAsync(sequenceNumbers));
    }

    @Override
    public CompletableFuture<IMessage> receiveAsync() {
        return internalReceiver.receiveAsync().thenApplyAsync(c -> scheduleLockRenewal(c));
    }

    @Override
    public CompletableFuture<IMessage> receiveAsync(Duration serverWaitTime) {
        return internalReceiver.receiveAsync(serverWaitTime).thenApplyAsync(c -> scheduleLockRenewal(c));
    }

    @Override
    public CompletableFuture<Collection<IMessage>> receiveBatchAsync(int maxMessageCount) {
        return internalReceiver.receiveBatchAsync(maxMessageCount).thenApplyAsync(c -> scheduleLockRenewal(c));
    }

    @Override
    public CompletableFuture<Collection<IMessage>> receiveBatchAsync(int maxMessageCount, Duration serverWaitTime) {
        return internalReceiver.receiveBatchAsync(maxMessageCount, serverWaitTime)
                .thenApplyAsync(c -> scheduleLockRenewal(c));
    }

    @Override
    public CompletableFuture<IMessage> receiveDeferredMessageAsync(long sequenceNumber) {
        return internalReceiver.receiveDeferredMessageAsync(sequenceNumber).thenApplyAsync(c -> scheduleLockRenewal(c));
    }

    @Override
    public CompletableFuture<Collection<IMessage>> receiveDeferredMessageBatchAsync(Collection<Long> sequenceNumbers) {
        return internalReceiver.receiveDeferredMessageBatchAsync(sequenceNumbers)
                .thenApplyAsync(c -> scheduleLockRenewal(c));
    }

    @Override
    public CompletableFuture<Instant> renewMessageLockAsync(IMessage message) {
        return internalReceiver.renewMessageLockAsync(message).thenApplyAsync(c -> restartLockRenewal(message, c));
    }

    @Override
    public CompletableFuture<Instant> renewMessageLockAsync(UUID lockToken) {
        return internalReceiver.renewMessageLockAsync(lockToken).thenApplyAsync(c -> restartLockRenewal(lockToken, c));
    }

    @Override
    public Instant renewMessageLock(IMessage message) throws InterruptedException, ServiceBusException {
        return Utils.completeFuture(renewMessageLockAsync(message));
    }

    @Override
    public Instant renewMessageLock(UUID lockToken) throws InterruptedException, ServiceBusException {
        return Utils.completeFuture(renewMessageLockAsync(lockToken));
    }

    @Override
    public int getPrefetchCount() {
        return internalReceiver.getPrefetchCount();
    }

    @Override
    public void setPrefetchCount(int prefetchCount) throws ServiceBusException {
        internalReceiver.setPrefetchCount(prefetchCount);
    }

    private IMessage scheduleLockRenewal(IMessage message) {
        if (message != null && message.getLockToken() != null) {
            scheduleLockRenewal(message.getLockToken(), message.getLockedUntilUtc());
        }

        return message;
    }

    private void scheduleLockRenewal(UUID lockToken, Instant lockExpiry) {
        if (lockToken != null) {
            MessageLockRenewTask task = new MessageLockRenewTask(this, lockToken);
            Long schedule = ChronoUnit.SECONDS.between(Instant.now(), lockExpiry) - 20; // give us a 20 sec headroom
            ScheduledFuture<?> renewFuture = internalScheduler.scheduleAtFixedRate(task, schedule, schedule,
                    TimeUnit.SECONDS);
            renewLockTaskMessageIdMap.put(lockToken, renewFuture);
        }
    }

    private Collection<IMessage> scheduleLockRenewal(Collection<IMessage> messages) {
        if (messages != null && !messages.isEmpty()) {
            for (IMessage message : messages) {
                scheduleLockRenewal(message);
            }
        }

        return messages;
    }

    private void cancelLockRenewal(UUID lockToken) {
        ScheduledFuture<?> task = renewLockTaskMessageIdMap.get(lockToken);

        if (task != null) {
            task.cancel(false);
            renewLockTaskMessageIdMap.remove(lockToken);
        }
    }

    private Instant restartLockRenewal(IMessage message, Instant lockExpiry) {
        return restartLockRenewal(message.getLockToken(), lockExpiry);
    }

    private Instant restartLockRenewal(UUID lockToken, Instant lockExpiry) {
        cancelLockRenewal(lockToken);
        scheduleLockRenewal(lockToken, lockExpiry);
        return lockExpiry;
    }

    private void purgeSchedule() {
        for(ScheduledFuture<?> task : renewLockTaskMessageIdMap.values()) {
            task.cancel(false);
        }
        renewLockTaskMessageIdMap.clear();
    }
}