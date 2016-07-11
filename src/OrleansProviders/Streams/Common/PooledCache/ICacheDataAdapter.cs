﻿
using System;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Providers.Streams.Common
{
    /// <summary>
    /// Pooled queue cache stores data in tightly packed structures that need to be transformed to various
    ///   other formats quickly.  Since the data formats may change by queue type and data format,
    ///   this interface allows adapter developers to build custom data transforms appropriate for 
    ///   the various types of queue data.
    /// </summary>
    /// <typeparam name="TQueueMessage"></typeparam>
    /// <typeparam name="TCachedMessage"></typeparam>
    public interface ICacheDataAdapter<in TQueueMessage, TCachedMessage>
        where TQueueMessage : class
        where TCachedMessage : struct
    {
        /// <summary>
        /// Converts a TQueueMessage message from the queue to a TCachedMessage cachable structures.
        /// </summary>
        /// <param name="cachedMessage"></param>
        /// <param name="queueMessage"></param>
        /// <param name="dequeueTimeUtc"></param>
        /// <returns></returns>
        void QueueMessageToCachedMessage(ref TCachedMessage cachedMessage, TQueueMessage queueMessage);

        /// <summary>
        /// Converts a cached message to a batch container for delivery
        /// </summary>
        /// <param name="cachedMessage"></param>
        /// <returns></returns>
        IBatchContainer GetBatchContainer(ref TCachedMessage cachedMessage);

        /// <summary>
        /// Gets the stream sequence token from a cached message.
        /// </summary>
        /// <param name="cachedMessage"></param>
        /// <returns></returns>
        StreamSequenceToken GetSequenceToken(ref TCachedMessage cachedMessage);

        /// <summary>
        /// Given a purge request, indicates if a cached message should be purged from the cache
        /// </summary>
        /// <param name="cachedMessage"></param>
        /// <param name="purgeRequest"></param>
        /// <returns></returns>
        bool ShouldPurge(ref TCachedMessage cachedMessage, IDisposable purgeRequest);

        /// <summary>
        /// Assignable purge action.  This is called when a purge request is triggered.
        /// </summary>
        Action<IDisposable> PurgeAction { set; }
    }

    /// <summary>
    /// Compares cached messages with various stream details
    /// </summary>
    /// <typeparam name="TCachedMessage"></typeparam>
    public interface ICacheDataComparer<in TCachedMessage>
    {
        /// <summary>
        /// Compare a cached message with a sequence token to determine if it message is before or after the token
        /// </summary>
        /// <param name="cachedMessage"></param>
        /// <param name="streamToken"></param>
        /// <returns></returns>
        int Compare(TCachedMessage cachedMessage, StreamSequenceToken streamToken);
        int Compare(TCachedMessage cachedMessage, IStreamIdentity streamIdentity);
    }

    /// <summary>
    /// Cache data comparer exstension functions that switch argument order
    /// </summary>
    public static class CacheDataComparerExtensions
    {
        /// <summary>
        /// Compare a cached message with a sequence token to determine if it message is before or after the token
        /// </summary>
        /// <param name="cachedMessage"></param>
        /// <param name="comparer"></param>
        /// <param name="streamToken"></param>
        /// <returns></returns>
        public static int Compare<TCachedMessage>(this ICacheDataComparer<TCachedMessage> comparer, StreamSequenceToken streamToken, TCachedMessage cachedMessage)
        {
            return 0 - comparer.Compare(cachedMessage, streamToken);
        }

        public static int Compare<TCachedMessage>(this ICacheDataComparer<TCachedMessage> comparer, IStreamIdentity streamIdentity, TCachedMessage cachedMessage)
        {
            return 0 - comparer.Compare(cachedMessage, streamIdentity);
        }
    }
}