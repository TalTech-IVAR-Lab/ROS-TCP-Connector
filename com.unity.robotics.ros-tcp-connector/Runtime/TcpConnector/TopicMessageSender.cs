using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector
{
    public class TopicMessageSender : OutgoingMessageSender, IDisposable
    {
        public string RosMessageName { get; private set; }

        public string TopicName { get; private set; }

        public int QueueSize { get; private set; }

        // Messages waiting to be sent are indexed by their owning epoch outbox. A
        // standalone sender uses this same channel index without a connection outbox.
        readonly object m_OutgoingMessages = new object();
        internal sealed class PendingQueue
        {
            internal readonly TopicMessageSender Sender;
            internal readonly ROSConnection.OutgoingMessageQueue Owner;
            internal readonly ROSConnection.AttemptContext Attempt;
            internal readonly LinkedList<ROSConnection.OutgoingMessageQueue.Entry> Messages =
                new LinkedList<ROSConnection.OutgoingMessageQueue.Entry>();
            internal int Preparations;
            internal bool QueueFullWarning;
            internal PendingQueue(TopicMessageSender sender)
            {
                Sender = sender;
            }
            internal PendingQueue(TopicMessageSender sender, ROSConnection.OutgoingMessageQueue owner,
                ROSConnection.AttemptContext attempt)
            {
                Sender = sender;
                Owner = owner;
                Attempt = attempt;
            }
        }
        readonly PendingQueue m_StandaloneQueue;
        PendingQueue CurrentQueue => ReferenceEquals(m_Connection, null)
            ? m_StandaloneQueue : m_Connection.GetSenderQueue(this);

        // Latching retains the last successful immutable publication and its source
        // reference solely for the legacy Peek API.
        OwnedPublication m_LastMessageSent;
        long m_NextPublication;
        ROSConnection.AttemptContext m_LastReplayAttempt;
        long m_CacheRevision;
        bool m_Disposed;
        readonly ROSConnection m_Connection;
        internal RosTopicState OwnerTopic { get; private set; }

        // Each queued/claimed/cache owner holds one handle under the channel gate.
        // The final handle detaches the source lease for outside-lock release.
        internal sealed class OwnedPublication
        {
            internal readonly MessageUse Use;
            internal int Handles = 1;
            internal long Sequence;
            internal long CacheRevision;
            readonly byte[] m_Frame;

            // Kept for the reference-authority tests and for a source-only cache
            // candidate. Connector publications use the frozen-frame constructor.
            internal OwnedPublication(MessageUse use) { Use = use; }
            internal OwnedPublication(MessageUse use, byte[] frame)
            {
                Use = use;
                m_Frame = frame;
            }
            internal MessageUse ReleaseLocked() { return --Handles == 0 ? Use : null; }

            internal void WriteTo(Stream stream)
            {
                if (m_Frame == null)
                    throw new InvalidOperationException("The publication has no frozen wire snapshot.");
                stream.Write(m_Frame, 0, m_Frame.Length);
            }
        }

        // Optional, used if you want to pool messages and reuse them when they are no longer in use.
        IMessagePool m_MessagePool;
        int m_Preparations;
        int m_ExactMessagesInFlight;
        public bool MessagePoolEnabled { get { lock (m_OutgoingMessages) return m_MessagePool != null; } }

        public TopicMessageSender(string topicName, string rosMessageName, int queueSize)
            : this(topicName, rosMessageName, queueSize, null)
        {
        }

        internal TopicMessageSender(string topicName, string rosMessageName, int queueSize, ROSConnection connection)
            : this(topicName, rosMessageName, queueSize, connection, null)
        {
        }

        internal TopicMessageSender(string topicName, string rosMessageName, int queueSize, ROSConnection connection,
            RosTopicState ownerTopic)
        {
            if (queueSize < 1)
            {
                throw new Exception("Queue size must be greater than or equal to 1.");
            }

            TopicName = topicName;
            RosMessageName = rosMessageName;
            QueueSize = queueSize;
            m_Connection = connection;
            OwnerTopic = ownerTopic;
            m_StandaloneQueue = new PendingQueue(this);
        }

        internal MessageUse AcquireMessageUse(Message message)
        {
            IMessagePool pool;
            lock (m_OutgoingMessages)
            {
                if (m_Disposed)
                    throw new ObjectDisposedException(nameof(TopicMessageSender));
                pool = m_MessagePool;
            }
            return MessageUseRegistry.Acquire(message, pool);
        }

        internal bool TryBeginPreparation(PendingQueue queue)
        {
            lock (m_OutgoingMessages)
            {
                if (m_Disposed || queue == null || m_Preparations >= QueueSize)
                    return false;
                m_Preparations++;
                return true;
            }
        }

        internal void EndPreparation(PendingQueue queue)
        {
            lock (m_OutgoingMessages)
            {
                if (queue == null || m_Preparations <= 0)
                    throw new InvalidOperationException("Publication preparation accounting underflow.");
                m_Preparations--;
            }
        }

        internal bool TryStampPublication(OwnedPublication publication)
        {
            lock (m_OutgoingMessages)
            {
                if (m_Disposed || publication == null)
                    return false;
                publication.Sequence = ++m_NextPublication;
                publication.CacheRevision = m_CacheRevision;
                return true;
            }
        }

        internal OwnedPublication PreparePublication(MessageUse use)
        {
            if (use == null)
                throw new ArgumentNullException(nameof(use));
            try
            {
                var serializer = new MessageSerializer();
                serializer.Write(TopicName);
                long topicBytes = ExactMessageSender.CountBytes(serializer.GetBytesSequence()) - sizeof(int);
                if (topicBytes > ROSConnection.k_MaxTopicNameBytes)
                    throw new IOException("The outgoing topic exceeds the wire topic limit.");
                serializer.SerializeMessageWithLength(use.Source);
                List<byte[]> segments = serializer.GetBytesSequence();
                long total = ExactMessageSender.CountBytes(segments);
                long payloadBytes = total - topicBytes - 2 * sizeof(int);
                if (payloadBytes < 0 || payloadBytes > ROSConnection.k_MaxMessageBytes)
                    throw new IOException("The outgoing payload exceeds the wire message limit.");
                var frame = new byte[checked((int)total)];
                int offset = 0;
                foreach (byte[] segment in segments)
                {
                    Buffer.BlockCopy(segment, 0, frame, offset, segment.Length);
                    offset += segment.Length;
                }
                return new OwnedPublication(use, frame);
            }
            catch
            {
                use.Release(false);
                throw;
            }
        }

        internal MessageUse Queue(MessageUse use)
        {
            PendingQueue queue = CurrentQueue;
            if (!TryBeginPreparation(queue))
            {
                use.Release(false);
                throw new IOException("Publication preparation capacity is exhausted.");
            }
            OwnedPublication publication = null;
            bool preparationActive = true;
            try
            {
                publication = PreparePublication(use);
                use = null;
                if (queue.Owner != null)
                {
                    MessageUse displaced;
                    bool admitted = m_Connection.CommitPublication(this, queue.Attempt, queue, publication,
                        out displaced, signalAttempt: false);
                    preparationActive = false;
                    if (!admitted)
                        throw new IOException("The publishing connection epoch is no longer active.");
                    publication = null;
                    return displaced;
                }
                MessageUse standaloneDisplaced = QueuePreparedForStandalone(publication, queue);
                publication = null;
                return standaloneDisplaced;
            }
            finally
            {
                if (preparationActive)
                    EndPreparation(queue);
                AbortPublication(publication, false);
                use?.Release(false);
            }
        }

        // Retained for the internal standalone/test seam. Connection-owned queues
        // commit through ROSConnection.OutgoingMessageQueue so the global and channel
        // indexes are updated as one operation.
        internal MessageUse QueueForOwner(MessageUse use, PendingQueue queue)
        {
            if (!TryBeginPreparation(queue))
            {
                use.Release(false);
                throw new IOException("Publication preparation capacity is exhausted.");
            }
            OwnedPublication publication = null;
            bool preparationActive = true;
            try
            {
                publication = PreparePublication(use);
                use = null;
                if (queue.Owner != null)
                {
                    MessageUse displaced;
                    bool admitted = m_Connection.CommitPublication(this, queue.Attempt, queue, publication,
                        out displaced, signalAttempt: false);
                    preparationActive = false;
                    if (!admitted)
                        throw new IOException("The publishing connection epoch is no longer active.");
                    publication = null;
                    return displaced;
                }
                MessageUse standaloneDisplaced = QueuePreparedForStandalone(publication, queue);
                publication = null;
                return standaloneDisplaced;
            }
            finally
            {
                if (preparationActive)
                    EndPreparation(queue);
                AbortPublication(publication, false);
                use?.Release(false);
            }
        }

        internal MessageUse QueuePreparedForStandalone(OwnedPublication publication, PendingQueue queue)
        {
            MessageUse release = null;
            lock (m_OutgoingMessages)
            {
                if (m_Disposed)
                    throw new ObjectDisposedException(nameof(TopicMessageSender));
                if (queue == null || queue.Owner != null)
                    throw new IOException("No accepting standalone publication owner.");
                publication.Sequence = ++m_NextPublication;
                publication.CacheRevision = m_CacheRevision;
                if (queue.Messages.Count >= QueueSize)
                {
                    ROSConnection.OutgoingMessageQueue.Entry displaced = queue.Messages.First.Value;
                    queue.Messages.RemoveFirst();
                    displaced.ChannelNode = null;
                    queue.QueueFullWarning = true;
                    release = displaced.Publication.ReleaseLocked();
                }
                var entry = new ROSConnection.OutgoingMessageQueue.Entry(null, this, queue, publication);
                entry.ChannelNode = queue.Messages.AddLast(entry);
            }
            return release;
        }

        internal ExactMessageSender CreateExactSender(Message message, List<byte[]> prefix)
        {
            return CreateExactSenderForUse(AcquireMessageUse(message), prefix);
        }

        // Consumes the preparation lease on every path. The capsule owns it only
        // after construction; rejection/serialization failure abandons the borrow.
        internal ExactMessageSender CreateExactSenderForUse(MessageUse use, List<byte[]> prefix)
        {
            bool reserved;
            lock (m_OutgoingMessages)
            {
                reserved = !m_Disposed && m_ExactMessagesInFlight < QueueSize;
                if (reserved)
                    m_ExactMessagesInFlight++;
            }
            if (!reserved)
            {
                use.Release(false);
                return null;
            }
            try
            {
                var serializer = new MessageSerializer();
                serializer.Write(TopicName);
                long topicBytes = ExactMessageSender.CountBytes(serializer.GetBytesSequence()) - sizeof(int);
                if (topicBytes > ROSConnection.k_MaxTopicNameBytes)
                    throw new IOException("The outgoing service topic exceeds the wire topic limit.");
                serializer.SerializeMessageWithLength(use.Source);
                List<byte[]> payload = serializer.GetBytesSequence();
                // Do not trust MessageSerializer.Length: a custom serializer may have
                // overflowed its int accounting while retaining repeated array segments.
                long payloadBytes = ExactMessageSender.CountBytes(payload) - topicBytes - 2 * sizeof(int);
                if (payloadBytes < 0 || payloadBytes > ROSConnection.k_MaxMessageBytes)
                    throw new IOException("The outgoing service payload exceeds the wire message limit.");
                if (ExactMessageSender.CountBytes(prefix) > 2L * sizeof(int)
                    + ROSConnection.k_MaxTopicNameBytes + ROSConnection.k_MaxMessageBytes)
                    throw new IOException("The outgoing service control frame exceeds the wire frame limit.");
                var statements = new List<byte[]>(prefix.Count + payload.Count);
                statements.AddRange(prefix);
                statements.AddRange(payload);
                return new ExactMessageSender(statements, use, this);
            }
            catch
            {
                ReleaseExactCapacity();
                use?.Release(false);
                throw;
            }
        }

        internal void ReleaseExactCapacity()
        {
            lock (m_OutgoingMessages)
                m_ExactMessagesInFlight--;
        }


        internal SendToState GetStandaloneMessageToSend(out OwnedPublication messageToSend)
        {
            messageToSend = null;
            ROSConnection.OutgoingMessageQueue.Entry entry = null;
            bool warning;
            lock (m_OutgoingMessages)
            {
                if (m_StandaloneQueue.Messages.Count == 0)
                    return SendToState.NoMessageToSendError;
                entry = m_StandaloneQueue.Messages.First.Value;
                m_StandaloneQueue.Messages.RemoveFirst();
                entry.ChannelNode = null;
                warning = m_StandaloneQueue.QueueFullWarning;
                m_StandaloneQueue.QueueFullWarning = false;
                messageToSend = entry.Publication;
            }
            return warning ? SendToState.QueueFullWarning : SendToState.Normal;
        }

        internal MessageUse ReleasePublicationHandle(OwnedPublication publication)
        {
            lock (m_OutgoingMessages)
                return publication.ReleaseLocked();
        }

        internal void AbortPublication(OwnedPublication publication, bool returnEligible)
        {
            if (publication == null) return;
            MessageUse release = ReleasePublicationHandle(publication);
            release?.Release(returnEligible);
        }

        public bool PeekNextMessageToSend(out Message messageToSend)
        {
            PendingQueue queue = CurrentQueue;
            if (queue != null && queue.Owner != null)
                return queue.Owner.PeekChannel(queue, out messageToSend);

            bool result = false;
            messageToSend = null;
            lock (m_OutgoingMessages)
            {
                if (queue != null && queue.Messages.Count > 0 && queue.Messages.First.Value.Publication != null)
                {
                    messageToSend = queue.Messages.First.Value.Publication.Use.Source;
                    result = true;
                }
            }
            return result;
        }

        public void PrepareLatchMessage()
        {
            TryPrepareLatchMessage(null); // Standalone explicit replay remains repeatable.
        }

        internal bool TryPrepareLatchMessage(ROSConnection.AttemptContext attempt)
        {
            return PrepareLatchForOwner(attempt, CurrentQueue);
        }

        internal bool PrepareLatchForOwner(ROSConnection.AttemptContext attempt, PendingQueue queue)
        {
            lock (m_OutgoingMessages)
            {
                if (attempt != null)
                {
                    if (ReferenceEquals(m_LastReplayAttempt, attempt))
                        return false;
                    m_LastReplayAttempt = attempt;
                }
                if (queue != null && !m_Disposed && m_LastMessageSent != null && queue.Messages.Count == 0)
                {
                    // This topic is latching, so to mimic that functionality,
                    // the last sent immutable publication is sent again.
                    m_LastMessageSent.Handles++;
                    var entry = new ROSConnection.OutgoingMessageQueue.Entry(attempt, this, queue, m_LastMessageSent);
                    entry.ChannelNode = queue.Messages.AddFirst(entry);
                    return true;
                }
                return false;
            }
        }

        public override SendToState SendInternal(MessageSerializer messageSerializer, Stream stream)
        {
            if (ReferenceEquals(m_Connection, null))
            {
                SendToState state = GetStandaloneMessageToSend(out OwnedPublication publication);
                if (state == SendToState.NoMessageToSendError)
                    return state;
                SendToState writeState = SendPublication(publication, stream, null);
                return writeState == SendToState.Normal ? state : writeState;
            }

            if (!m_Connection.TryClaimPublication(this, out ROSConnection.OutgoingMessageQueue.Entry entry,
                out bool warning))
                return SendToState.NoMessageToSendError;
            SendToState connectedState = SendPublication(entry.Publication, stream, entry.Attempt);
            return connectedState == SendToState.Normal && warning
                ? SendToState.QueueFullWarning : connectedState;
        }

        internal SendToState SendPublication(OwnedPublication publication, Stream stream, ROSConnection.AttemptContext attempt)
        {
            MessageUse displaced = null;
            MessageUse completed = null;
            try
            {
                publication.WriteTo(stream);
                displaced = ReferenceEquals(m_Connection, null)
                    ? CommitSuccessfulPublication(publication)
                    : m_Connection.TryCommitSenderCache(this, publication, attempt);
            }
            finally
            {
                completed = ReleasePublicationHandle(publication);
                displaced?.Release(true);
                completed?.Release(true);
            }
            return SendToState.Normal;
        }

        internal MessageUse CommitSuccessfulPublication(OwnedPublication publication)
        {
            // Metadata only; connected writers enter through the exact-attempt commit
            // gate, and all detached source releases run after BOTH gates have unwound.
            lock (m_OutgoingMessages)
            {
                if (m_Disposed || publication.CacheRevision != m_CacheRevision
                    || (m_LastMessageSent != null && publication.Sequence < m_LastMessageSent.Sequence))
                    return null;
                publication.Handles++;
                MessageUse displaced = m_LastMessageSent?.ReleaseLocked();
                m_LastMessageSent = publication;
                return displaced;
            }
        }

        internal MessageUse InvalidateCache()
        {
            lock (m_OutgoingMessages)
            {
                m_CacheRevision++;
                MessageUse release = m_LastMessageSent?.ReleaseLocked();
                m_LastMessageSent = null;
                return release;
            }
        }

        public override void ClearAllQueuedData()
        {
            ClearOwnerQueue(CurrentQueue);
        }

        internal void ClearOwnerQueue(PendingQueue queue)
        {
            if (queue == null) return;
            var releases = TakeQueuedPublicationReleases(queue);
            foreach (MessageUse release in releases)
                release.Release(true);
        }

        List<MessageUse> TakeQueuedPublicationReleases(PendingQueue queue)
        {
            if (queue == null)
                return new List<MessageUse>();

            List<ROSConnection.OutgoingMessageQueue.Entry> entries;
            if (queue.Owner != null)
            {
                // The connection outbox owns both indexes. Detach under its gate so a
                // writer can either claim the complete entry or leave it for this
                // cancellation path, never both.
                entries = queue.Owner.DetachChannel(queue);
            }
            else
            {
                lock (m_OutgoingMessages)
                {
                    entries = new List<ROSConnection.OutgoingMessageQueue.Entry>(queue.Messages);
                    foreach (ROSConnection.OutgoingMessageQueue.Entry entry in entries)
                        entry.ChannelNode = null;
                    queue.Messages.Clear();
                    queue.QueueFullWarning = false;
                }
            }

            var releases = new List<MessageUse>();
            lock (m_OutgoingMessages)
            {
                foreach (ROSConnection.OutgoingMessageQueue.Entry entry in entries)
                {
                    if (entry.Publication == null)
                        continue;
                    MessageUse release = entry.Publication.ReleaseLocked();
                    if (release != null)
                        releases.Add(release);
                }
            }
            return releases;
        }

        public void SetMessagePool(IMessagePool messagePool)
        {
            lock (m_OutgoingMessages)
                m_MessagePool = messagePool;
        }

        // Queue clearing deliberately preserves the last success. Terminal disposal
        // is different: it closes admission and releases cache plus queued handles,
        // but never the claimed writer's handle or exact capsule capacity.
        public void Dispose()
        {
            PendingQueue queue = CurrentQueue;
            var releases = new List<MessageUse>();
            lock (m_OutgoingMessages)
            {
                if (m_Disposed)
                    return;
                m_Disposed = true;
                MessageUse cached = m_LastMessageSent?.ReleaseLocked();
                if (cached != null) releases.Add(cached);
                m_LastMessageSent = null;
            }
            releases.AddRange(TakeQueuedPublicationReleases(queue));
            foreach (MessageUse release in releases)
                release.Release(true);
        }
    }

    sealed class ExactMessageSender : OutgoingMessageSender
    {
        internal static long CountBytes(List<byte[]> statements)
        {
            long length = 0;
            foreach (byte[] statement in statements)
            {
                if (statement == null)
                    throw new IOException("An outgoing service frame contains an unfinished segment.");
                length = checked(length + statement.LongLength);
            }
            return length;
        }

        // The single movable owner contains the bytes AND all terminal obligations.
        // Available -> taken by writer OR cancelled by clearer. Once taken, no other
        // caller can release any part of it; only the writer's finally terminalizes it.
        sealed class OwnershipCapsule
        {
            readonly byte[] m_Bytes;
            readonly MessageUse m_Use;
            readonly TopicMessageSender m_CapacityOwner;

            public OwnershipCapsule(List<byte[]> statements, MessageUse use, TopicMessageSender capacityOwner)
            {
                // GetBytesSequence only copies the list; generated byte[] fields still
                // alias its segments. Own both frames before publishing the capsule.
                m_Bytes = new byte[checked((int)CountBytes(statements))];
                int offset = 0;
                foreach (byte[] statement in statements)
                {
                    Buffer.BlockCopy(statement, 0, m_Bytes, offset, statement.Length);
                    offset += statement.Length;
                }
                m_Use = use;
                m_CapacityOwner = capacityOwner;
            }

            public void WriteTo(Stream stream)
            {
                stream.Write(m_Bytes, 0, m_Bytes.Length);
            }

            public void Release(bool returnSource)
            {
                // No ownership lock spans capacity release or best-effort recycling.
                try { m_CapacityOwner.ReleaseExactCapacity(); }
                finally
                {
                    m_Use.Release(returnSource);
                }
            }
        }

        OwnershipCapsule m_Capsule;

        public ExactMessageSender(List<byte[]> statements, MessageUse use, TopicMessageSender capacityOwner)
        {
            m_Capsule = new OwnershipCapsule(statements, use, capacityOwner);
        }

        public override SendToState SendInternal(MessageSerializer messageSerializer, Stream stream)
        {
            OwnershipCapsule capsule = Interlocked.Exchange(ref m_Capsule, null);
            if (capsule == null)
                return SendToState.NoMessageToSendError;
            try
            {
                capsule.WriteTo(stream);
                return SendToState.Normal;
            }
            finally
            {
                capsule.Release(true);
            }
        }

        public override void ClearAllQueuedData()
        {
            OwnershipCapsule capsule = Interlocked.Exchange(ref m_Capsule, null);
            capsule?.Release(true);
        }

        internal void AbortUnqueued()
        {
            // Before successful admission the caller still owns the source. Release the
            // preparation/wire claim, not a pool offer. The same whole-capsule take also
            // makes rollback idempotent and unable to steal an already claimed write.
            OwnershipCapsule capsule = Interlocked.Exchange(ref m_Capsule, null);
            capsule?.Release(false);
        }
    }

    // Domain-wide root identity, never Message.Equals/GetHashCode. R is isolated:
    // source access and deferred pool execution occur only after releasing it.
    sealed class MessageUse
    {
        MessageUseRegistry.Record m_Record;
        internal Message Source { get; private set; }
        internal MessageUse(MessageUseRegistry.Record record)
        {
            m_Record = record;
            Source = record.Source;
        }
        internal void Release(bool returnEligible)
        {
            MessageUseRegistry.Record record = Interlocked.Exchange(ref m_Record, null);
            if (record != null)
                MessageUseRegistry.Release(record, returnEligible);
        }
    }

    static class MessageUseRegistry
    {
        internal enum UseState { Active, ReturnQueued, Returning, Finished, Abandoned }
        internal sealed class Record
        {
            internal Message Source;
            internal IMessagePool Pool;
            internal int Uses;
            internal bool ReturnEligible;
            internal long Generation;
            internal UseState State;
        }
        sealed class Offer
        {
            internal readonly Record Record;
            internal readonly long Generation;
            internal Offer(Record record) { Record = record; Generation = record.Generation; }
        }

        const int k_MaxPending = 32;
        static readonly object s_Gate = new object();
        static readonly List<Record> s_Records = new List<Record>();
        static readonly Queue<Offer> s_Pending = new Queue<Offer>();

        internal static MessageUse Acquire(Message source, IMessagePool pool)
        {
            if (ReferenceEquals(source, null))
                throw new ArgumentNullException(nameof(source));
            lock (s_Gate)
            {
                Record record = FindLocked(source);
                if (record == null)
                {
                    record = new Record { Source = source, Pool = pool, ReturnEligible = pool != null };
                    s_Records.Add(record);
                }
                else
                {
                    if (record.State == UseState.Returning)
                        throw new IOException("The message is being handed back to its pool; reacquire it after handoff.");
                    if (!ReferenceEquals(record.Pool, pool))
                        record.ReturnEligible = false;
                    if (record.State == UseState.ReturnQueued)
                    {
                        // Keep pool/eligibility history. Only this not-yet-claimed offer
                        // is revoked; no arbitrary pool callback can be cancelled safely.
                        record.Generation++;
                        record.State = UseState.Active;
                    }
                }
                record.Uses++;
                return new MessageUse(record);
            }
        }

        static Record FindLocked(Message source)
        {
            foreach (Record record in s_Records)
                if (ReferenceEquals(record.Source, source))
                    return record;
            return null;
        }

        internal static void Release(Record record, bool returnEligible)
        {
            bool signal = false;
            lock (s_Gate)
            {
                record.ReturnEligible &= returnEligible;
                if (--record.Uses != 0)
                    return;
                if (record.ReturnEligible && s_Pending.Count < k_MaxPending
                    && MessageRecycler.AcceptingOffers)
                {
                    record.State = UseState.ReturnQueued;
                    record.Generation++;
                    s_Pending.Enqueue(new Offer(record));
                    signal = true;
                }
                else
                {
                    record.State = UseState.Abandoned;
                    RemoveRecordLocked(record);
                }
            }
            if (signal) MessageRecycler.Signal();
        }

        internal static Record ClaimReturn()
        {
            lock (s_Gate)
            {
                while (s_Pending.Count > 0)
                {
                    Offer offer = s_Pending.Dequeue();
                    Record record = offer.Record;
                    if (record.State != UseState.ReturnQueued || record.Generation != offer.Generation)
                        continue;
                    record.State = UseState.Returning;
                    return record;
                }
                return null;
            }
        }

        internal static bool IsReturning(Message source)
        {
            lock (s_Gate)
                return FindLocked(source)?.State == UseState.Returning;
        }

        internal static void FinishReturn(Record record)
        {
            lock (s_Gate)
            {
                record.State = UseState.Finished;
                RemoveRecordLocked(record);
            }
        }

        internal static void DropPendingReturns()
        {
            lock (s_Gate)
            {
                while (s_Pending.Count > 0)
                {
                    Offer offer = s_Pending.Dequeue();
                    Record record = offer.Record;
                    if (record.State != UseState.ReturnQueued || record.Generation != offer.Generation)
                        continue;
                    record.State = UseState.Abandoned;
                    RemoveRecordLocked(record);
                }
            }
        }

        static void RemoveRecordLocked(Record record)
        {
            for (int index = 0; index < s_Records.Count; index++)
            {
                if (!ReferenceEquals(s_Records[index], record))
                    continue;
                s_Records.RemoveAt(index);
                return;
            }
        }
    }

    static class MessageRecycler
    {
        static readonly object s_LifecycleGate = new object();
        static AutoResetEvent s_Ready;
        static Thread s_Thread;
        static int s_AcceptingOffers = 1;

        internal static bool AcceptingOffers => Volatile.Read(ref s_AcceptingOffers) != 0;

        static void EnsureStartedLocked()
        {
            if (s_Thread != null || !AcceptingOffers)
                return;
            s_Ready = new AutoResetEvent(false);
            s_Thread = new Thread(Run) { IsBackground = true, Name = "ROS message recycler" };
            s_Thread.Start();
        }

        internal static void Signal()
        {
            AutoResetEvent ready;
            lock (s_LifecycleGate)
            {
                if (!AcceptingOffers)
                    return;
                EnsureStartedLocked();
                ready = s_Ready;
            }
            try { ready?.Set(); }
            catch (ObjectDisposedException) { }
        }

        internal static void RequestStop()
        {
            AutoResetEvent ready = null;
            AutoResetEvent retired = null;
            lock (s_LifecycleGate)
            {
                Thread thread = s_Thread;
                if (thread == null || !thread.IsAlive)
                {
                    // A completed worker has no callback left to protect. Release its
                    // identity and keep the domain recycler available for the next
                    // same-AppDomain connection.
                    retired = s_Ready;
                    s_Ready = null;
                    s_Thread = null;
                    Volatile.Write(ref s_AcceptingOffers, 1);
                }
                else
                {
                    // Keep pooling closed until the current callback and worker
                    // finalization have both returned. The worker will reopen the
                    // recycler after it detaches its old wait handle and thread.
                    Volatile.Write(ref s_AcceptingOffers, 0);
                    ready = s_Ready;
                }
            }
            try { retired?.Dispose(); }
            catch (Exception) { }
            try { ready?.Set(); }
            catch (ObjectDisposedException) { }
        }

        internal static void DropPendingReturns()
        {
            MessageUseRegistry.DropPendingReturns();
        }

        static void CompleteWorkerStop()
        {
            AutoResetEvent retired = null;
            lock (s_LifecycleGate)
            {
                if (ReferenceEquals(s_Thread, Thread.CurrentThread))
                {
                    retired = s_Ready;
                    s_Ready = null;
                    s_Thread = null;
                    Volatile.Write(ref s_AcceptingOffers, 1);
                }
            }
            try { retired?.Dispose(); }
            catch (Exception) { }
        }

        static void Run()
        {
            try
            {
                while (true)
                {
                    AutoResetEvent ready;
                    lock (s_LifecycleGate)
                        ready = s_Ready;
                    if (ready == null)
                        return;
                    try { ready.WaitOne(); }
                    catch (ObjectDisposedException) { return; }
                    MessageUseRegistry.Record record;
                    while (AcceptingOffers && (record = MessageUseRegistry.ClaimReturn()) != null)
                    {
                        Exception failure = null;
                        try { record.Pool.AddMessage(record.Source); }
                        catch (Exception exception) { failure = exception; }
                        finally { MessageUseRegistry.FinishReturn(record); }
                        // Add may have stored the object before throwing. Never retry it.
                        if (failure != null)
                            Debug.LogWarning("Message pool recycling failed: " + failure);
                    }
                    if (!AcceptingOffers)
                    {
                        MessageUseRegistry.DropPendingReturns();
                        return;
                    }
                }
            }
            finally
            {
                // A worker fault or a requested stop must not strand the domain
                // recycler in its permanently-disabled state or retain its old roots.
                MessageUseRegistry.DropPendingReturns();
                CompleteWorkerStop();
            }
        }
    }
}
