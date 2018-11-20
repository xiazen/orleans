using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;

namespace Orleans.Transactions.State
{
    internal class ReadWriteLock<TState>
       where TState : class, new()
    {
        private readonly TransactionalStateOptions options;
        private readonly TransactionQueue<TState> queue;
        private BatchWorker lockWorker;
        private BatchWorker storageWorker;
        private readonly ILogger logger;

        // the linked list of lock groups
        // the head is the group that is currently holding the lock
        private LockGroup currentGroup = null;
        private const int maxGroupSize = 20;

        // cache the last known minimum so we don't have to recompute it as much
        private DateTime cachedMin = DateTime.MaxValue;

        // group of non-conflicting transactions collectively acquiring/releasing the lock
        private class LockGroup : Dictionary<Guid, TransactionRecord<TState>>
        {
            public int FillCount;
            public List<Task> Tasks; // the tasks for executing the waiting operations
            public DateTime? Deadline;
        }

        public ReadWriteLock(
            IOptions<TransactionalStateOptions> options,
            TransactionQueue<TState> queue,
            BatchWorker storageWorker,
            ILogger logger)
        {
            this.options = options.Value;
            this.queue = queue;
            this.storageWorker = storageWorker;
            this.logger = logger;
            this.lockWorker = new BatchWorkerFromDelegate(LockWork);
        }

        public async Task<TResult> EnterLock<TResult>(Guid transactionId, DateTime priority,
                                   AccessCounter counter, bool isRead, Task<TResult> task)
        {
            bool rollbacksOccurred = false;
            List<Task> cleanup = new List<Task>();

            await this.queue.Ready();

            // search active transactions
            if (Find(transactionId, isRead, out var group, out var record))
            {
                this.logger.LogWarning("ReadWriteLock.EnterLock, shouldn't have active transaction here, since one tx per grain");
                // check if we lost some reads or writes already
                if (counter.Reads > record.NumberReads || counter.Writes > record.NumberWrites)
                {
                    throw new OrleansBrokenTransactionLockException(transactionId.ToString(), "when re-entering lock");
                }

                // check if the operation conflicts with other transactions in the group
                if (HasConflict(isRead, priority, transactionId, group, out var resolvable))
                {
                    if (!resolvable)
                    {
                        throw new OrleansTransactionLockUpgradeException(transactionId.ToString());
                    }
                    else
                    {
                        // rollback all conflicts
                        var conflicts = Conflicts(transactionId, group).ToList();

                        if (conflicts.Count > 0)
                        {
                            foreach (var r in conflicts)
                            {
                                cleanup.Add(Rollback(r, true));
                                rollbacksOccurred = true;
                            }
                        }
                    }
                }
            }
            else
            {
                // check if we were supposed to already hold this lock
                if (counter.Reads + counter.Writes > 0)
                {
                    throw new OrleansBrokenTransactionLockException(transactionId.ToString(), "when trying to re-enter lock");
                }

                // update the lock deadline
                if (group == currentGroup)
                {
                    group.Deadline = DateTime.UtcNow + this.options.LockTimeout;

                    if (logger.IsEnabled(LogLevel.Trace))
                        logger.Trace("set lock expiration at {Deadline}", group.Deadline.Value.ToString("o"));
                }

                // create a new record for this transaction
                record = new TransactionRecord<TState>()
                {
                    TransactionId = transactionId,
                    Priority = priority,
                    Deadline = DateTime.UtcNow + this.options.LockAcquireTimeout
                };

                group.Add(transactionId, record);
                group.FillCount++;

                if (logger.IsEnabled(LogLevel.Trace))
                {
                    if (group == currentGroup)
                        logger.Trace($"enter-lock {transactionId} fc={group.FillCount}");
                    else
                        logger.Trace($"enter-lock-queue {transactionId} fc={group.FillCount}");
                }
            }

            if (group != currentGroup)
            {
                this.logger.LogWarning("ReadWriteLock.EnterLock, shouldn't be hitting here, group should always be equal to currentGroup");
                // task will be executed once its group acquires the lock

                if (group.Tasks == null)
                    group.Tasks = new List<Task>();

                group.Tasks.Add(task);
            }
            else
            {
                // execute task right now
                task.RunSynchronously();

                // look at exception to avoid UnobservedException
                var ignore = task.Exception;
            }

            if (isRead)
            {
                record.AddRead();
            }
            else
            {
                record.AddWrite();
            }

            if (rollbacksOccurred)
            {
                lockWorker.Notify();
            }
            else if (group.Deadline.HasValue)
            {
                lockWorker.Notify(group.Deadline.Value);
            }

            await Task.WhenAll(cleanup);
            return await task;
        }

        public async Task<Tuple<TransactionalStatus, TransactionRecord<TState>>> ValidateLock(Guid transactionId, AccessCounter accessCount)
        {
            if (currentGroup == null || !currentGroup.TryGetValue(transactionId, out TransactionRecord<TState> record))
            {
                return Tuple.Create(TransactionalStatus.BrokenLock, new TransactionRecord<TState>());
            }
            else if (record.NumberReads != accessCount.Reads
                   || record.NumberWrites != accessCount.Writes)
            {
                await Rollback(transactionId, true);
                return Tuple.Create(TransactionalStatus.LockValidationFailed, record);
            }
            else
            {
                return Tuple.Create(TransactionalStatus.Ok, record);
            }
        }

        public void Notify()
        {
            this.lockWorker.Notify();
        }

        public bool TryGetRecord(Guid transactionId, out TransactionRecord<TState> record)
        {
            return this.currentGroup.TryGetValue(transactionId, out record);
        }

        public Task AbortExecutingTransactions()
        {
            if (currentGroup != null)
            {
                Task[] pending = currentGroup.Select(g => BreakLock(g.Key, g.Value)).ToArray();
                currentGroup.Clear();
                return Task.WhenAll(pending);
            }
            return Task.CompletedTask;
        }

        private Task BreakLock(Guid transactionId, TransactionRecord<TState> entry)
        {
            if (logger.IsEnabled(LogLevel.Trace))
                logger.Trace("Break-lock for transaction {TransactionId}", transactionId);

            return this.queue.NotifyOfAbort(entry, TransactionalStatus.BrokenLock);
        }

        public void AbortQueuedTransactions()
        {
           this.logger.LogWarning("ReadWriteLock.AbortQueuedTransactions, Shouldn't have queued transactions, something is wrong...");
        }

        public async Task Rollback(Guid guid, bool notify)
        {
            // no-op if the transaction never happened or already rolled back
            if (currentGroup == null || !currentGroup.TryGetValue(guid, out var record))
            {
                return;
            }

            // remove record for this transaction
            currentGroup.Remove(guid);

            // notify remote listeners
            if (notify)
            {
                await this.queue.NotifyOfAbort(record, TransactionalStatus.BrokenLock);
            }
        }

        private async Task LockWork()
        {
            if (currentGroup != null)
            {
                // check if there are any group members that are ready to exit the lock
                if (currentGroup.Count > 0)
                {
                    if (LockExits(out var single, out var multiple))
                    {
                        if (single != null)
                        {
                            await this.queue.EnqueueCommit(single);
                        }
                        else if (multiple != null)
                        {
                            this.logger.LogWarning("ReadWriteLock.LockWork, shouldn't be hitting here, since only one tx should be in group.");
                            foreach (var r in multiple)
                            {
                                await this.queue.EnqueueCommit(r);
                            }
                        }

                        lockWorker.Notify();
                        storageWorker.Notify();
                    }

                    else if (currentGroup.Deadline < DateTime.UtcNow)
                    {
                        // the lock group has timed out.
                        var txlist = string.Join(",", currentGroup.Keys.Select(g => g.ToString()));
                        logger.Warn(555, $"break-lock timeout for {currentGroup.Count} transactions {txlist}");
                        await AbortExecutingTransactions();
                        lockWorker.Notify();
                    }

                    else if (currentGroup.Deadline.HasValue)
                    {
                        if (logger.IsEnabled(LogLevel.Trace))
                            logger.Trace("recheck lock expiration at {Deadline}", currentGroup.Deadline.Value.ToString("o"));

                        // check again when the group expires
                        lockWorker.Notify(currentGroup.Deadline.Value);
                    }
                }

                else
                {
                    // the lock is empty, set it back to null
                    currentGroup = null;
                }
            }
        }
       
        private bool Find(Guid guid, bool isRead, out LockGroup group, out TransactionRecord<TState> record)
        {
            if (currentGroup == null)
            {
                group = currentGroup = new LockGroup();
                record = null;
                return false;
            }
            else
            {
                this.logger.LogWarning("ReadWriteLock.Find, shouldn't hit here. Since there's only one tx going on per grain");
                group = null;
                var pos = currentGroup;

                if (pos.TryGetValue(guid, out record))
                {
                    group = pos;
                    return true;
                }
                return false;

            }
        }

        private bool HasConflict(bool isRead, DateTime priority, Guid transactionId, LockGroup group, out bool resolvable)
        {
            bool foundResolvableConflicts = false;

            foreach (var kvp in group)
            {
                if (kvp.Key != transactionId)
                {
                    if (isRead && kvp.Value.NumberWrites == 0)
                    {
                        continue;
                    }
                    else
                    {
                        if (priority > kvp.Value.Priority)
                        {
                            resolvable = false;
                            return true;
                        }
                        else
                        {
                            foundResolvableConflicts = true;
                        }
                    }
                }
            }

            resolvable = foundResolvableConflicts;
            return foundResolvableConflicts;
        }

        private IEnumerable<Guid> Conflicts(Guid transactionId, LockGroup group)
        {
            foreach (var kvp in group)
            {
                if (kvp.Key != transactionId)
                {
                    yield return kvp.Key;
                }
            }
        }

        private bool LockExits(out TransactionRecord<TState> single, out List<TransactionRecord<TState>> multiple)
        {
            single = null;
            multiple = null;

            // fast-path the one-element case
            if (currentGroup.Count == 1)
            {
                var kvp = currentGroup.First();
                if (kvp.Value.Role == CommitRole.NotYetDetermined) // has not received commit from TA
                {
                    return false;
                }
                else
                {
                    single = kvp.Value;

                    currentGroup.Remove(single.TransactionId);

                    if (logger.IsEnabled(LogLevel.Debug))
                        logger.Debug($"exit-lock {single.TransactionId} {single.Timestamp:o}");

                    return true;
                }
            }
            else
            {
                this.logger.LogWarning("ReadWriteLock.LockExits, One group should only have one tx, shouldn't be hitting here");
                return false;
            }
        }

        private static int Comparer(TransactionRecord<TState> a, TransactionRecord<TState> b)
        {
            return a.Timestamp.CompareTo(b.Timestamp);
        }
    }
}
