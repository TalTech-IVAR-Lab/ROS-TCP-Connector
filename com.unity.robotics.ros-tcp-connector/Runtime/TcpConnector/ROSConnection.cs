using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using UnityEngine;
using UnityEngine.Serialization;
using System.Collections.Concurrent;
using System.Threading;
using System.Linq;
using System.IO;

namespace Unity.Robotics.ROSTCPConnector
{
    public class ROSConnection : MonoBehaviour
    {
        public const string k_Version = "v0.7.1";
        public const string k_CompatibleVersionPrefix = "v0.7.";

        [SerializeField]
        ROSConnectionConfig m_ConnectionConfig;
        public ROSConnectionConfig ConnectionConfig
        {
            get => m_ConnectionConfig;
            set
            {
                lock (m_ConnectionLifecycleLock)
                {
                    if (m_Lifecycle != LifecycleState.StoppedAuthorized)
                        throw new InvalidOperationException("ConnectionConfig cannot be replaced after Connect until an explicit Disconnect has completed cleanup.");
                    m_ConnectionConfig = value;
                }
            }
        }
        public string RosIPAddress => m_ConnectionConfig != null ? m_ConnectionConfig.RosIPAddress : null;
        public int RosPort => m_ConnectionConfig != null ? m_ConnectionConfig.RosPort : 0;

        [SerializeField]
        bool m_ConnectOnStart = true;
        public bool ConnectOnStart { get => m_ConnectOnStart; set => m_ConnectOnStart = value; }

        [SerializeField]
        [Tooltip("If nothing has been sent for this long (seconds), send a keepalive message to check the connection is still working.")]
        float m_KeepaliveTime = 1;
        public float KeepaliveTime { get => m_KeepaliveTime; set => m_KeepaliveTime = value; }

        [SerializeField]
        float m_NetworkTimeoutSeconds = 2;
        public float NetworkTimeoutSeconds { get => m_NetworkTimeoutSeconds; set => m_NetworkTimeoutSeconds = value; }

        [SerializeField]
        float m_SleepTimeSeconds = 0.01f;
        public float SleepTimeSeconds { get => m_SleepTimeSeconds; set => m_SleepTimeSeconds = value; }


        [SerializeField]
        [FormerlySerializedAs("showHUD")]
        bool m_ShowHUD = true;
        public bool ShowHud { get => m_ShowHUD; set => m_ShowHUD = value; }

        [SerializeField]
        string[] m_TFTopics = { "/tf" };
        public string[] TFTopics { get => m_TFTopics; set => m_TFTopics = value; }

        const int k_DefaultPublisherQueueSize = 10;
        const bool k_DefaultPublisherLatch = false;

        internal enum RegistrationRole { Subscriber, Publisher, RosService, UnityService }
        internal enum RegistrationPhase { Absent, Queued, Writing, Written }

        // A definition owns complete, immutable wire snapshots for one local role revision.
        // The byte arrays are private and are only consumed by registration units.
        internal sealed class RegistrationDefinition
        {
            internal readonly RosTopicState Topic;
            internal readonly RegistrationRole Role;
            internal readonly long Revision;
            internal readonly string ResponseMessageName;
            readonly byte[] m_RegisterBytes;
            readonly byte[] m_RemoveBytes;

            internal RegistrationDefinition(RosTopicState topic, RegistrationRole role, long revision,
                byte[] registerBytes, byte[] removeBytes, string responseMessageName)
            {
                Topic = topic;
                Role = role;
                Revision = revision;
                ResponseMessageName = responseMessageName;
                m_RegisterBytes = registerBytes;
                m_RemoveBytes = removeBytes;
            }

            internal SysCommandSender CreateSender(bool remove)
            {
                return new SysCommandSender(remove ? m_RemoveBytes : m_RegisterBytes);
            }

            internal bool IsEquivalentTo(RegistrationDefinition other)
            {
                return other != null && Role == other.Role
                    && string.Equals(ResponseMessageName, other.ResponseMessageName, StringComparison.Ordinal)
                    && BytesEqual(m_RegisterBytes, other.m_RegisterBytes)
                    && BytesEqual(m_RemoveBytes, other.m_RemoveBytes);
            }

            static bool BytesEqual(byte[] first, byte[] second)
            {
                if (ReferenceEquals(first, second)) return true;
                if (first == null || second == null || first.Length != second.Length) return false;
                for (int index = 0; index < first.Length; index++)
                    if (first[index] != second[index]) return false;
                return true;
            }
        }

        internal sealed class RegistrationBinding
        {
            internal readonly AttemptContext Attempt;
            internal readonly RosTopicState Topic;
            internal readonly RegistrationRole Role;
            internal RegistrationDefinition Desired;
            internal RegistrationDefinition Written;
            internal RegistrationUnit Active;
            internal RegistrationPhase Phase;

            internal RegistrationBinding(AttemptContext attempt, RosTopicState topic, RegistrationRole role)
            {
                Attempt = attempt;
                Topic = topic;
                Role = role;
                Phase = RegistrationPhase.Absent;
            }
        }

        internal sealed class RegistrationUnit
        {
            internal readonly RegistrationBinding Binding;
            internal readonly RegistrationDefinition Definition;
            internal readonly bool Remove;
            internal readonly SysCommandSender Sender;
            internal RegistrationPhase Phase;
            internal bool Cancelled;

            internal RegistrationUnit(RegistrationBinding binding, RegistrationDefinition definition, bool remove)
            {
                Binding = binding;
                Definition = definition;
                Remove = remove;
                Sender = definition.CreateSender(remove);
                Phase = RegistrationPhase.Queued;
            }
        }

        // GUI window variables
        internal HudPanel m_HudPanel = null;
        public HudPanel HUDPanel => m_HudPanel;

        internal sealed class OutgoingMessageQueue
        {
            internal enum EntryKind { Legacy, Publication, Exact, Raw, Registration }
            internal class Entry
            {
                public readonly AttemptContext Attempt;
                // Kept as a compatibility view for the existing internal test seam.
                // Normal connector admission uses one of the sealed unit fields below.
                public readonly OutgoingMessageSender Sender;
                internal readonly TopicMessageSender.OwnedPublication Publication;
                internal readonly ExactMessageSender Exact;
                internal readonly byte[] Raw;
                internal readonly RegistrationUnit Registration;
                internal readonly EntryKind Kind;
                internal bool RawUnitCounted;
                internal TopicMessageSender.PendingQueue Channel;
                internal LinkedListNode<Entry> GlobalNode;
                internal LinkedListNode<Entry> ChannelNode;
                internal readonly bool ManualClaimOnly;

                public Entry(AttemptContext attempt, OutgoingMessageSender sender)
                {
                    Attempt = attempt;
                    Sender = sender;
                    ManualClaimOnly = false;
                    Kind = EntryKind.Legacy;
                }

                internal Entry(AttemptContext attempt, TopicMessageSender sender,
                    TopicMessageSender.PendingQueue channel, TopicMessageSender.OwnedPublication publication,
                    bool manualClaimOnly = false)
                {
                    Attempt = attempt;
                    Sender = sender;
                    Channel = channel;
                    Publication = publication;
                    ManualClaimOnly = manualClaimOnly;
                    Kind = EntryKind.Publication;
                }

                internal Entry(AttemptContext attempt, ExactMessageSender exact)
                {
                    Attempt = attempt;
                    Sender = exact;
                    Exact = exact;
                    ManualClaimOnly = false;
                    Kind = EntryKind.Exact;
                }

                internal Entry(AttemptContext attempt, SysCommandSender sender, byte[] raw)
                {
                    Attempt = attempt;
                    Sender = sender;
                    Raw = raw;
                    ManualClaimOnly = false;
                    Kind = EntryKind.Raw;
                    RawUnitCounted = true;
                }

                internal Entry(AttemptContext attempt, RegistrationUnit registration)
                {
                    Attempt = attempt;
                    Sender = registration.Sender;
                    Registration = registration;
                    ManualClaimOnly = false;
                    Kind = EntryKind.Registration;
                }
            }

            readonly object m_Gate = new object();
            readonly LinkedList<Entry> m_Entries = new LinkedList<Entry>();
            internal readonly List<TopicMessageSender.PendingQueue> Channels = new List<TopicMessageSender.PendingQueue>();
            readonly AttemptContext m_OwnerAttempt;

            internal TopicMessageSender.PendingQueue Channel(TopicMessageSender sender)
            {
                lock (m_Gate)
                {
                    foreach (var channel in Channels)
                        if (ReferenceEquals(channel.Sender, sender)) return channel;
                    var created = new TopicMessageSender.PendingQueue(sender, this, m_OwnerAttempt);
                    Channels.Add(created);
                    return created;
                }
            }

            public OutgoingMessageQueue() { }

            internal OutgoingMessageQueue(AttemptContext ownerAttempt)
            {
                m_OwnerAttempt = ownerAttempt;
            }

            // Compatibility-only seam for existing lifecycle tests. Connector paths
            // admit sealed publication/exact/raw units through the methods below.
            public void Enqueue(AttemptContext attempt, OutgoingMessageSender outgoingMessageSender)
            {
                lock (m_Gate)
                {
                    Entry entry = new Entry(attempt, outgoingMessageSender);
                    entry.GlobalNode = m_Entries.AddLast(entry);
                }
            }

            internal Entry EnqueuePublication(AttemptContext attempt, TopicMessageSender sender,
                TopicMessageSender.PendingQueue channel, TopicMessageSender.OwnedPublication publication,
                bool manualClaimOnly = false)
            {
                lock (m_Gate)
                {
                    Entry displaced = null;
                    if (channel.Messages.Count >= sender.QueueSize)
                    {
                        displaced = channel.Messages.First.Value;
                        RemoveLocked(displaced);
                        channel.QueueFullWarning = true;
                    }
                    Entry entry = new Entry(attempt, sender, channel, publication, manualClaimOnly);
                    entry.GlobalNode = m_Entries.AddLast(entry);
                    entry.ChannelNode = channel.Messages.AddLast(entry);
                    return displaced;
                }
            }

            internal bool TryDequeueChannel(TopicMessageSender.PendingQueue channel, out Entry entry,
                out bool queueFullWarning)
            {
                lock (m_Gate)
                {
                    queueFullWarning = channel != null && channel.QueueFullWarning;
                    if (channel != null)
                        channel.QueueFullWarning = false;
                    if (channel == null || channel.Messages.Count == 0)
                    {
                        entry = null;
                        return false;
                    }
                    entry = channel.Messages.First.Value;
                    RemoveLocked(entry);
                    return true;
                }
            }

            internal bool TryDequeueChannel(TopicMessageSender sender, out Entry entry,
                out bool queueFullWarning)
            {
                lock (m_Gate)
                {
                    foreach (TopicMessageSender.PendingQueue channel in Channels)
                    {
                        if (!ReferenceEquals(channel.Sender, sender))
                            continue;
                        queueFullWarning = channel.QueueFullWarning;
                        channel.QueueFullWarning = false;
                        if (channel.Messages.Count == 0)
                        {
                            entry = null;
                            return false;
                        }
                        entry = channel.Messages.First.Value;
                        RemoveLocked(entry);
                        return true;
                    }
                    entry = null;
                    queueFullWarning = false;
                    return false;
                }
            }

            internal bool PeekChannel(TopicMessageSender.PendingQueue channel, out Message message)
            {
                lock (m_Gate)
                {
                    if (channel == null || channel.Messages.Count == 0 || channel.Messages.First.Value.Publication == null)
                    {
                        message = null;
                        return false;
                    }
                    message = channel.Messages.First.Value.Publication.Use.Source;
                    return true;
                }
            }

            internal List<Entry> DetachChannel(TopicMessageSender.PendingQueue channel)
            {
                var entries = new List<Entry>();
                lock (m_Gate)
                {
                    if (channel == null)
                        return entries;
                    LinkedListNode<Entry> node = channel.Messages.First;
                    while (node != null)
                    {
                        LinkedListNode<Entry> next = node.Next;
                        Entry entry = node.Value;
                        RemoveLocked(entry);
                        entries.Add(entry);
                        node = next;
                    }
                    channel.QueueFullWarning = false;
                }
                return entries;
            }

            internal void EnqueueExact(AttemptContext attempt, ExactMessageSender exact)
            {
                lock (m_Gate)
                {
                    Entry entry = new Entry(attempt, exact);
                    entry.GlobalNode = m_Entries.AddLast(entry);
                }
            }

            internal void EnqueueRaw(AttemptContext attempt, SysCommandSender sender, byte[] raw)
            {
                lock (m_Gate)
                {
                    Entry entry = new Entry(attempt, sender, raw);
                    entry.GlobalNode = m_Entries.AddLast(entry);
                }
            }

            internal void EnqueueRegistration(AttemptContext attempt, RegistrationUnit registration)
            {
                lock (m_Gate)
                {
                    Entry entry = new Entry(attempt, registration);
                    entry.GlobalNode = m_Entries.AddLast(entry);
                }
            }

            internal void EnqueueRegistrationFirst(AttemptContext attempt, RegistrationUnit registration)
            {
                lock (m_Gate)
                {
                    Entry entry = new Entry(attempt, registration);
                    entry.GlobalNode = m_Entries.AddFirst(entry);
                }
            }

            internal void EnqueueRegistrationAfterPrefix(AttemptContext attempt, RegistrationUnit registration)
            {
                lock (m_Gate)
                {
                    Entry entry = new Entry(attempt, registration);
                    LinkedListNode<Entry> firstNonRegistration = m_Entries.First;
                    while (firstNonRegistration != null
                        && firstNonRegistration.Value.Kind == EntryKind.Registration)
                        firstNonRegistration = firstNonRegistration.Next;
                    entry.GlobalNode = firstNonRegistration == null
                        ? m_Entries.AddLast(entry)
                        : m_Entries.AddBefore(firstNonRegistration, entry);
                }
            }

            internal bool RemoveRegistration(RegistrationUnit registration)
            {
                lock (m_Gate)
                {
                    LinkedListNode<Entry> node = m_Entries.First;
                    while (node != null)
                    {
                        if (ReferenceEquals(node.Value.Registration, registration))
                        {
                            RemoveLocked(node.Value);
                            return true;
                        }
                        node = node.Next;
                    }
                    return false;
                }
            }

            internal bool AttachPreparedPublication(TopicMessageSender.PendingQueue channel)
            {
                lock (m_Gate)
                {
                    if (channel == null || channel.Messages.Count == 0)
                        return false;
                    Entry entry = channel.Messages.First.Value;
                    if (entry.GlobalNode != null)
                        return false;
                    LinkedListNode<Entry> firstNonRegistration = m_Entries.First;
                    while (firstNonRegistration != null
                        && firstNonRegistration.Value.Kind == EntryKind.Registration)
                        firstNonRegistration = firstNonRegistration.Next;
                    entry.GlobalNode = firstNonRegistration == null
                        ? m_Entries.AddLast(entry)
                        : m_Entries.AddBefore(firstNonRegistration, entry);
                    return true;
                }
            }

            public bool TryDequeue(out Entry outgoingMessage)
            {
                lock (m_Gate)
                {
                    LinkedListNode<Entry> node = m_Entries.First;
                    while (node != null && node.Value.ManualClaimOnly)
                        node = node.Next;
                    if (node == null)
                    {
                        outgoingMessage = null;
                        return false;
                    }
                    outgoingMessage = node.Value;
                    RemoveLocked(outgoingMessage);
                    return true;
                }
            }

            public int Count
            {
                get { lock (m_Gate) return m_Entries.Count; }
            }

            void RemoveLocked(Entry entry)
            {
                if (entry.GlobalNode != null)
                {
                    // The node belongs to this queue; detach it before releasing the
                    // queue gate so an evicted unit cannot leave a sender ticket behind.
                    m_Entries.Remove(entry.GlobalNode);
                    entry.GlobalNode = null;
                }
                if (entry.ChannelNode != null)
                {
                    entry.Channel.Messages.Remove(entry.ChannelNode);
                    entry.ChannelNode = null;
                }
            }

            internal void ClearQueuedData(Action<Entry> clear)
            {
                List<Entry> entries;
                lock (m_Gate)
                {
                    entries = new List<Entry>(m_Entries);
                    foreach (TopicMessageSender.PendingQueue channel in Channels)
                    {
                        LinkedListNode<Entry> node = channel.Messages.First;
                        while (node != null)
                        {
                            Entry entry = node.Value;
                            if (entry.GlobalNode == null)
                                entries.Add(entry);
                            node = node.Next;
                        }
                        channel.Messages.Clear();
                        channel.QueueFullWarning = false;
                    }
                    m_Entries.Clear();
                    foreach (Entry entry in entries)
                    {
                        entry.GlobalNode = null;
                        entry.ChannelNode = null;
                    }
                }
                foreach (Entry entry in entries)
                {
                    try { clear(entry); }
                    catch (Exception exception) { Debug.LogException(exception); }
                }
            }
        }

        internal class IncomingMessage
        {
            public readonly AttemptContext Attempt;
            public readonly string Topic;
            public readonly byte[] Contents;

            public IncomingMessage(AttemptContext attempt, string topic, byte[] contents)
            {
                Attempt = attempt;
                Topic = topic;
                Contents = contents;
            }
        }

        readonly object m_ConnectionLifecycleLock = new object();
        enum LifecycleState { StoppedAuthorized, Running, Stopping, StoppedUnauthorized, Disposed }
        LifecycleState m_Lifecycle = LifecycleState.StoppedAuthorized;
        internal sealed class WorkerSession
        {
            internal readonly string Address;
            internal readonly int Port;
            internal readonly float Timeout;
            internal readonly float Keepalive;
            internal readonly int SleepMilliseconds;
            internal readonly CancellationTokenSource Cancellation = new CancellationTokenSource();
            internal readonly WakeSignal WakeSignal = new WakeSignal();
            internal Task Worker;
            internal bool WorkerTerminal;
            internal bool ExplicitStop;
            internal bool StopCompleted;
            internal bool Finalizing;
            internal bool ResourcesDisposed;
            internal StopPlan Stop;
            internal AttemptContext Attempt;

            internal WorkerSession(string address, int port, float timeout, float keepalive, int sleepMilliseconds)
            {
                Address = address;
                Port = port;
                Timeout = timeout;
                Keepalive = keepalive;
                SleepMilliseconds = sleepMilliseconds;
            }
        }
        internal enum AttemptState { OfflineOpen, Connecting, Establishing, Ready, Closing, Closed }

        internal sealed class SocketOwner
        {
            internal readonly TcpClient Client;
            internal readonly TaskCompletionSource<bool> Closed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            int m_CloseClaimed;
            internal SocketOwner(TcpClient client) { Client = client; }
            internal void CloseOnce(Action beforeClose)
            {
                if (Interlocked.CompareExchange(ref m_CloseClaimed, 1, 0) != 0) return;
                try { beforeClose?.Invoke(); }
                finally
                {
                    try
                    {
                        try { Client.Client.Shutdown(SocketShutdown.Both); } catch (Exception) { }
                        Client.Close();
                    }
                    finally { Closed.TrySetResult(true); }
                }
            }
        }

        internal sealed class WakeSignal : IDisposable
        {
            readonly object m_Gate = new object();
            AutoResetEvent m_Event = new AutoResetEvent(false);
            bool m_Disposed;

            internal void Signal()
            {
                AutoResetEvent signal;
                lock (m_Gate)
                {
                    if (m_Disposed) return;
                    signal = m_Event;
                }
                try { signal.Set(); }
                catch (ObjectDisposedException) { }
            }

            internal bool Wait(int milliseconds)
            {
                AutoResetEvent signal;
                lock (m_Gate)
                {
                    if (m_Disposed) return false;
                    signal = m_Event;
                }
                try { return signal.WaitOne(milliseconds); }
                catch (ObjectDisposedException) { return false; }
            }

            public void Dispose()
            {
                AutoResetEvent signal;
                lock (m_Gate)
                {
                    if (m_Disposed) return;
                    m_Disposed = true;
                    signal = m_Event;
                    m_Event = null;
                }
                signal.Dispose();
            }
        }

        internal enum PendingPairKind { UnityServiceRequest, RosServiceResponse }

        internal sealed class PendingPair
        {
            internal readonly AttemptContext Attempt;
            internal readonly PendingPairKind Kind;
            internal readonly int ServiceId;

            internal PendingPair(AttemptContext attempt, PendingPairKind kind, int serviceId)
            {
                Attempt = attempt;
                Kind = kind;
                ServiceId = serviceId;
            }
        }

        internal sealed class AttemptContext
        {
            internal readonly long Id;
            internal WorkerSession Session;
            internal readonly bool OfflineEpoch;
            internal AttemptState State;
            internal bool IsOpen => State != AttemptState.Closing && State != AttemptState.Closed;
            internal readonly OutgoingMessageQueue Outgoing;
            internal int RawUnits;
            internal ConcurrentQueue<IncomingMessage> Incoming = new ConcurrentQueue<IncomingMessage>();
            internal Dictionary<int, ServiceCall> Calls = new Dictionary<int, ServiceCall>();
            internal Dictionary<int, ServiceCall> RetiredCalls;
            internal List<AttemptCallback<string[]>> TopicCallbacks = new List<AttemptCallback<string[]>>();
            internal List<AttemptCallback<Dictionary<string, string>>> TypeCallbacks = new List<AttemptCallback<Dictionary<string, string>>>();
            internal int TopicListOutstanding;
            internal PendingPair PendingPair;
            internal readonly List<RegistrationBinding> Registrations = new List<RegistrationBinding>();
            internal SocketOwner Transport;
            internal TcpClient Client => Transport?.Client;
            internal NetworkStream Stream;
            internal CancellationTokenSource ReaderCancellation;
            internal Task Reader;
            internal AttemptContext(long id, WorkerSession session)
            {
                Id = id;
                Session = session;
                OfflineEpoch = session == null;
                State = OfflineEpoch ? AttemptState.OfflineOpen : AttemptState.Connecting;
                Outgoing = new OutgoingMessageQueue(this);
            }

            internal void BindToSession(WorkerSession session)
            {
                Session = session;
                State = AttemptState.Connecting;
            }
        }

        internal sealed class StopPlan
        {
            internal readonly WorkerSession Session;
            internal readonly SocketOwner Transport;
            internal readonly AttemptContext Attempt;
            internal readonly bool CancelSession;
            internal StopPlan(WorkerSession session, AttemptContext attempt)
            {
                Session = session;
                Attempt = attempt;
                Transport = attempt?.Transport;
                // Once disposal was claimed, the terminal worker needs no further cancellation.
                CancelSession = session != null && !session.Finalizing && !session.ResourcesDisposed;
            }
        }
        WorkerSession m_WorkerSession;
        StopPlan m_StopPlan;
        AttemptContext m_CurrentAttempt;
        long m_NextAttemptId;
        long CurrentAttemptId => m_CurrentAttempt != null && m_CurrentAttempt.IsOpen ? m_CurrentAttempt.Id : 0;
        public bool HasConnectionThread
        {
            get
            {
                lock (m_ConnectionLifecycleLock)
                    return m_WorkerSession != null;
            }
        }

        volatile bool m_HasConnectionError;
        bool m_HasOutputConnectionError;
        public bool HasConnectionError => m_HasConnectionError;

        // only the main thread can access Time.*, so make a copy here
        public static float s_RealTimeSinceStartup = 0.0f;

        internal enum ServiceCallState { Pending, RawReady, Decoding, Succeeded, Invalidated, Faulted }

        // All call state and table ownership use m_ConnectionLifecycleLock. Raw receipt is
        // not terminal: Disconnect must still own the right to fail a blocked decoder's caller.
        internal abstract class ServiceCall
        {
            public int Id;
            public AttemptContext Attempt;
            public string Topic;
            public RosTopicState TopicState;
            public RosTopicState PendingCapacityOwner;
            public RosTopicState DecoderCapacityOwner;
            public bool DecoderClaimed;
            public Func<MessageDeserializer, Message> ResponseDeserializer;
            public RosTopicState.ServiceResponseSnapshot ResponseSnapshot;
            public ServiceCallState State;
            public bool CallbackStarted;
            public readonly TaskCompletionSource<byte[]> RawResponse =
                new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);

            public abstract void CompleteFailure(Exception exception);
        }

        internal sealed class ServiceCall<T> : ServiceCall where T : Message
        {
            public readonly TaskCompletionSource<T> Completion =
                new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);

            public override void CompleteFailure(Exception exception)
            {
                // Release a decoder which has not received raw bytes, too. Its catch observes
                // cancellation even if the public outcome has already been invalidated.
                RawResponse.TrySetCanceled();
                if (exception is OperationCanceledException cancellation)
                    Completion.TrySetCanceled(cancellation.CancellationToken);
                else
                    Completion.TrySetException(exception);
            }
        }

        int m_NextSrvID = 101;

        Action m_ServiceResponseContinuationTestHook;
        // Deterministic lifecycle observation seams. Invoked only outside ownership gates.
        Action m_StopPlanCapturedTestHook;
        Action m_WorkerBodyCompletedTestHook;
        Action m_SessionDisposalTestHook;
        Action m_ReaderTerminatingTestHook;
        Action m_ServiceFailureSelectedTestHook;
        Action m_SocketCloseTestHook;
        Action m_WorkerBeforeCloseTestHook;
        Action m_WorkerSocketCloseReturnedTestHook;
        Action m_BeforeWorkerStartGateTestHook;
        Action m_BeforeClientPublicationTestHook;
        Action m_BeforeConnectAttemptTestHook;
        Action m_BeforeStartupRegistrationsTestHook;
        Action m_BeforeImmediateCommandWriteTestHook;
        Action m_AfterImmediateCommandWriteTestHook;
        Action m_BeforeConfiguredTopicPublicationTestHook;
        Action m_BeforeDiscoveryPlaceholderMutationTestHook;
        Action m_BeforeExistingTopicConfigurationCommitTestHook;
        Action m_BeforeSenderCacheCommitTestHook;
        Action m_BeforePendingPairInstallationTestHook;
        Action m_BeforePendingPairRoutingTestHook;

        public bool listenForTFMessages = true;

        float m_LastMessageReceivedRealtime;
        float m_LastMessageSentRealtime;
        public float LastMessageReceivedRealtime => m_LastMessageReceivedRealtime;
        public float LastMessageSentRealtime => m_LastMessageSentRealtime;

        public bool HasSubscriber(string topic)
        {
            lock (m_ConnectionLifecycleLock)
                return m_Topics.TryGetValue(topic, out RosTopicState info) && info.HasSubscriberCallback;
        }

        internal class CallbackRegistration
        {
            internal readonly AttemptContext BoundAttempt;
            internal readonly AttemptContext Attempt;
            internal bool Active = true;

            protected CallbackRegistration(AttemptContext attempt)
            {
                BoundAttempt = attempt;
                Attempt = attempt;
            }
        }

        internal class CallbackRegistration<T> : CallbackRegistration
        {
            internal readonly Action<T> Callback;

            internal CallbackRegistration(AttemptContext attempt, Action<T> callback)
                : base(attempt)
            {
                Callback = callback;
            }
        }

        internal class AttemptCallback<T> : CallbackRegistration<T>
        {
            internal AttemptCallback(AttemptContext attempt, Action<T> callback)
                : base(attempt, callback)
            {
            }
        }

        internal bool TryBeginCallback(AttemptContext attempt, CallbackRegistration registration)
        {
            if (registration == null)
                return false;
            lock (m_ConnectionLifecycleLock)
            {
                if (!registration.Active)
                    return false;
                if (registration.BoundAttempt != null)
                    return ReferenceEquals(registration.BoundAttempt, attempt) && IsCurrentLocked(attempt);
                return attempt == null || IsCurrentLocked(attempt);
            }
        }

        void RevokeCallbacksLocked(CallbackRegistration[] registrations)
        {
            if (registrations == null)
                return;
            foreach (CallbackRegistration registration in registrations)
            {
                if (registration != null)
                    registration.Active = false;
            }
        }

        internal void RevokeCallbacks(CallbackRegistration[] registrations)
        {
            lock (m_ConnectionLifecycleLock)
                RevokeCallbacksLocked(registrations);
        }

        List<CallbackRegistration<RosTopicState>> m_NewTopicCallbacks =
            new List<CallbackRegistration<RosTopicState>>();

        Dictionary<string, RosTopicState> m_Topics = new Dictionary<string, RosTopicState>();
        readonly Dictionary<RosTopicState, long> m_TopicCreationIds = new Dictionary<RosTopicState, long>();
        long m_NextTopicCreationId;
        readonly List<RegistrationDefinition> m_DesiredRegistrations = new List<RegistrationDefinition>();
        long m_NextRegistrationRevision;

        static byte[] FreezeSerializedCommand(string command, object param)
        {
            var serializer = new MessageSerializer();
            PopulateSysCommand(serializer, command, param);
            List<byte[]> segments = serializer.GetBytesSequence();
            long length = ExactMessageSender.CountBytes(segments);
            var bytes = new byte[checked((int)length)];
            int offset = 0;
            foreach (byte[] segment in segments)
            {
                Buffer.BlockCopy(segment, 0, bytes, offset, segment.Length);
                offset += segment.Length;
            }
            return bytes;
        }

        internal RegistrationDefinition PrepareRegistrationDefinition(RosTopicState topic, RegistrationRole role,
            string messageName, int queueSize = 0, bool latch = false, string responseMessageName = null)
        {
            string registerCommand;
            string removeCommand;
            object registerParam;
            object removeParam = new SysCommand_Topic { topic = topic.Topic };
            switch (role)
            {
                case RegistrationRole.Subscriber:
                    registerCommand = SysCommand.k_SysCommand_Subscribe;
                    removeCommand = SysCommand.k_SysCommand_RemoveSubscriber;
                    registerParam = new SysCommand_TopicAndType { topic = topic.Topic, message_name = messageName };
                    break;
                case RegistrationRole.Publisher:
                    registerCommand = SysCommand.k_SysCommand_Publish;
                    removeCommand = SysCommand.k_SysCommand_RemovePublisher;
                    registerParam = new SysCommand_PublisherRegistration
                    {
                        topic = topic.Topic,
                        message_name = messageName,
                        queue_size = queueSize,
                        latch = latch
                    };
                    break;
                case RegistrationRole.RosService:
                    registerCommand = SysCommand.k_SysCommand_RosService;
                    removeCommand = SysCommand.k_SysCommand_RemoveRosService;
                    registerParam = new SysCommand_TopicAndType { topic = topic.Topic, message_name = messageName };
                    break;
                default:
                    registerCommand = SysCommand.k_SysCommand_UnityService;
                    removeCommand = SysCommand.k_SysCommand_RemoveUnityService;
                    registerParam = new SysCommand_TopicAndType { topic = topic.Topic, message_name = messageName };
                    break;
            }
            byte[] registerBytes = FreezeSerializedCommand(registerCommand, registerParam);
            byte[] removeBytes = FreezeSerializedCommand(removeCommand, removeParam);
            return new RegistrationDefinition(topic, role, Interlocked.Increment(ref m_NextRegistrationRevision),
                registerBytes, removeBytes, responseMessageName);
        }

        internal RosTopicState PublishConfiguredTopic(RosTopicState candidate, RegistrationDefinition[] definitions)
        {
            if (candidate == null)
                throw new ArgumentNullException(nameof(candidate));

            // Test-only synchronization is deliberately before the lifecycle gate;
            // configured candidates are otherwise immutable by this point.
            m_BeforeConfiguredTopicPublicationTestHook?.Invoke();
            return PublishTopicCandidate(candidate, definitions, null, false, out _);
        }

        RosTopicState PublishTopicCandidate(RosTopicState candidate, RegistrationDefinition[] definitions,
            AttemptContext authority, bool requireAuthority, out bool added)
        {
            RosTopicState existing;
            CallbackRegistration<RosTopicState>[] callbacks = null;
            AttemptContext signalAttempt = null;
            added = false;
            lock (m_ConnectionLifecycleLock)
            {
                if (requireAuthority && !IsCurrentLocked(authority))
                    return null;
                if (m_Topics.TryGetValue(candidate.Topic, out existing))
                    return existing;

                // The catalog entry, its complete desired definitions and the listener
                // snapshot are one metadata transaction. Nothing can observe a state
                // before its response topic/sender/role configuration is installed.
                m_Topics.Add(candidate.Topic, candidate);
                m_TopicCreationIds[candidate] = ++m_NextTopicCreationId;
                if (definitions != null)
                {
                    foreach (RegistrationDefinition definition in definitions)
                    {
                        if (definition == null)
                            continue;
                        AttemptContext attempt = m_CurrentAttempt;
                        if (SetDesiredRegistrationLocked(definition, attempt, true))
                            signalAttempt = attempt;
                    }
                }
                callbacks = m_NewTopicCallbacks.ToArray();
                added = true;
            }
            if (signalAttempt != null)
                SignalAttempt(signalAttempt);
            if (requireAuthority)
                NotifyNewTopicForAttempt(candidate, added, callbacks, authority);
            else
                NotifyNewTopic(candidate, added, callbacks);
            return candidate;
        }

        public void ListenForTopics(Action<RosTopicState> callback, bool notifyAllExistingTopics = false)
        {
            RosTopicState[] existingTopics = null;
            var registration = new CallbackRegistration<RosTopicState>(null, callback);
            lock (m_ConnectionLifecycleLock)
            {
                m_NewTopicCallbacks.Add(registration);
                if (notifyAllExistingTopics)
                    existingTopics = m_Topics.Values.ToArray();
            }
            if (existingTopics != null)
                foreach (RosTopicState state in existingTopics)
                {
                    if (!TryBeginCallback(null, registration))
                        continue;
                    try { registration.Callback(state); }
                    catch (Exception exception) { Debug.LogException(exception); }
                }
        }

        RosTopicState AddTopic(string topic, string rosMessageName, bool isService = false)
        {
            RosTopicState newTopic = new RosTopicState(topic, rosMessageName, this, new InternalAPI(this), isService);
            lock (m_ConnectionLifecycleLock)
            {
                if (m_Topics.TryGetValue(topic, out RosTopicState existing))
                    return existing;
                m_Topics.Add(topic, newTopic);
            }
            return newTopic;
        }

        public RosTopicState GetTopic(string topic)
        {
            lock (m_ConnectionLifecycleLock)
            {
                m_Topics.TryGetValue(topic, out RosTopicState info);
                return info;
            }
        }

        public IEnumerable<RosTopicState> AllTopics
        {
            get
            {
                lock (m_ConnectionLifecycleLock)
                    return m_Topics.Values.ToArray();
            }
        }

        public RosTopicState GetOrCreateTopic(string topic, string rosMessageName, bool isService = false)
        {
            RosTopicState candidate = new RosTopicState(topic, rosMessageName, this, new InternalAPI(this), isService);
            RosTopicState state = PublishTopicCandidate(candidate, null, null, false, out bool added);
            if (state == null)
                return null;
            if (!added && !string.Equals(state.RosMessageName, rosMessageName, StringComparison.Ordinal))
            {
                if (state.HasActiveConfiguration)
                    throw new ArgumentException("The configured topic message type conflicts with the requested discovery type.",
                        nameof(rosMessageName));
                state.ChangeRosMessageName(rosMessageName);
            }
            return state;
        }

        RosTopicState GetOrCreateTopicForAttempt(string topic, string rosMessageName, bool isService,
            AttemptContext attempt)
        {
            RosTopicState candidate = new RosTopicState(topic, rosMessageName, this, new InternalAPI(this), isService,
                MessageSubtopic.Default, discoveryPlaceholder: true);
            RosTopicState state = PublishTopicCandidate(candidate, null, attempt, true, out bool added);
            if (state == null)
                return null;
            if (!added && state.IsDiscoveryPlaceholder)
            {
                m_BeforeDiscoveryPlaceholderMutationTestHook?.Invoke();
                state = UpdateDiscoveryPlaceholderForAttempt(state, rosMessageName, attempt);
            }
            return state;
        }

        RosTopicState UpdateDiscoveryPlaceholderForAttempt(RosTopicState expectedState,
            string rosMessageName, AttemptContext authority)
        {
            string previous = null;
            MessageUse retiredCache = null;
            bool changed = false;
            RosTopicState state;
            lock (m_ConnectionLifecycleLock)
            {
                if (!IsCurrentLocked(authority)
                    || !m_Topics.TryGetValue(expectedState.Topic, out state)
                    || !ReferenceEquals(state, expectedState))
                    return null;
                changed = state.TryUpdateDiscoveryPlaceholder(
                    rosMessageName, out previous, out retiredCache);
            }
            retiredCache?.Release(true);
            if (changed && previous != null)
                Debug.LogWarning($"Inconsistent declaration of topic '{state.Topic}': was '{previous}', switching to '{rosMessageName}'.");
            return state;
        }

        void NotifyNewTopic(RosTopicState state, bool added,
            CallbackRegistration<RosTopicState>[] callbacks)
        {
            if (!added || callbacks == null)
                return;
            foreach (CallbackRegistration<RosTopicState> registration in callbacks)
            {
                if (!TryBeginCallback(null, registration))
                    continue;
                try { registration.Callback(state); }
                catch (Exception exception) { Debug.LogException(exception); }
            }
        }

        void NotifyNewTopicForAttempt(RosTopicState state, bool added,
            CallbackRegistration<RosTopicState>[] callbacks, AttemptContext authority)
        {
            if (!added || callbacks == null)
                return;
            foreach (CallbackRegistration<RosTopicState> registration in callbacks)
            {
                if (!TryBeginCallback(authority, registration))
                    continue;
                try { registration.Callback(state); }
                catch (Exception exception) { Debug.LogException(exception); }
            }
        }

        void EnsureExistingTopicLocked(RosTopicState topic)
        {
            if ((topic.Subtopic == MessageSubtopic.Response && ReferenceEquals(topic.Connection, this))
                || (m_Topics.TryGetValue(topic.Topic, out RosTopicState existing)
                    && ReferenceEquals(existing, topic)))
                return;
            throw new InvalidOperationException("The topic is no longer published in this connection catalog.");
        }

        internal void ConfigureExistingSubscriber(RosTopicState topic, Action<Message> callback,
            string expectedMessageName)
        {
            RosTopicState.ExistingTopicConfiguration configuration =
                topic.PrepareSubscriberConfiguration(callback, expectedMessageName);
            RegistrationDefinition definition = PrepareRegistrationDefinition(topic,
                RegistrationRole.Subscriber, configuration.MessageName);
            m_BeforeExistingTopicConfigurationCommitTestHook?.Invoke();

            AttemptContext signalAttempt = null;
            bool shouldRegister = false;
            string previousMessageName = null;
            lock (m_ConnectionLifecycleLock)
            {
                EnsureExistingTopicLocked(topic);
                topic.ApplySubscriberConfiguration(configuration, out shouldRegister,
                    out previousMessageName);
                if (shouldRegister)
                {
                    AttemptContext attempt = m_CurrentAttempt;
                    if (SetDesiredRegistrationLocked(definition, attempt, true))
                        signalAttempt = attempt;
                }
            }
            if (previousMessageName != null)
                Debug.LogWarning($"Inconsistent declaration of topic '{topic.Topic}': was '{previousMessageName}', switching to '{expectedMessageName}'.");
            SignalAttempt(signalAttempt);
        }

        internal void ConfigureExistingPublisher(RosTopicState topic, int queueSize, bool latch,
            string expectedMessageName)
        {
            RosTopicState.ExistingTopicConfiguration configuration =
                topic.PreparePublisherConfiguration(queueSize, latch, expectedMessageName);
            if (configuration.Sender == null)
            {
                Debug.LogWarning($"Publisher for topic {topic.Topic} registered twice!");
                return;
            }

            TopicMessageSender displaced = null;
            bool senderTransferred = false;
            AttemptContext signalAttempt = null;
            bool applied;
            try
            {
                RegistrationDefinition definition = PrepareRegistrationDefinition(topic,
                    RegistrationRole.Publisher, configuration.MessageName, queueSize, latch);
                m_BeforeExistingTopicConfigurationCommitTestHook?.Invoke();
                lock (m_ConnectionLifecycleLock)
                {
                    EnsureExistingTopicLocked(topic);
                    applied = topic.ApplyPublisherConfiguration(configuration, out displaced);
                    if (applied)
                    {
                        senderTransferred = true;
                        AttemptContext attempt = m_CurrentAttempt;
                        if (SetDesiredRegistrationLocked(definition, attempt, true))
                            signalAttempt = attempt;
                    }
                }
                if (applied)
                    displaced?.Dispose();
                else
                    Debug.LogWarning($"Publisher for topic {topic.Topic} registered twice!");
                SignalAttempt(signalAttempt);
            }
            finally
            {
                if (!senderTransferred)
                    configuration.Sender.Dispose();
            }
        }

        internal void ConfigureExistingUnityService(RosTopicState topic,
            Func<Message, Message> implementation, Func<Message, Task<Message>> implementationAsync,
            string responseMessageName, int queueSize, string expectedRequestName)
        {
            RosTopicState.ExistingTopicConfiguration configuration = topic.PrepareUnityServiceConfiguration(
                implementation, implementationAsync, responseMessageName, queueSize, expectedRequestName);

            TopicMessageSender displaced = null;
            bool senderTransferred = false;
            string previousResponseName = null;
            TopicMessageSender responseCacheOwner = null;
            AttemptContext signalAttempt = null;
            try
            {
                RegistrationDefinition definition = PrepareRegistrationDefinition(topic,
                    RegistrationRole.UnityService, configuration.MessageName,
                    responseMessageName: responseMessageName);
                m_BeforeExistingTopicConfigurationCommitTestHook?.Invoke();
                lock (m_ConnectionLifecycleLock)
                {
                    EnsureExistingTopicLocked(topic);
                    topic.ApplyUnityServiceConfiguration(configuration, out displaced,
                        out RosTopicState responseTopic);
                    senderTransferred = true;
                    if (responseTopic != null)
                        responseTopic.ApplyMessageNameForTransaction(responseMessageName,
                            out previousResponseName, out responseCacheOwner);
                    AttemptContext attempt = m_CurrentAttempt;
                    if (RemoveDesiredRegistrationLocked(topic, RegistrationRole.Subscriber, attempt))
                        signalAttempt = attempt;
                    if (SetDesiredRegistrationLocked(definition, attempt, false))
                        signalAttempt = attempt;
                }
                responseCacheOwner?.InvalidateCache()?.Release(true);
                displaced?.Dispose();
                if (previousResponseName != null)
                    Debug.LogWarning($"Inconsistent declaration of topic '{topic.Topic}': was '{previousResponseName}', switching to '{responseMessageName}'.");
                SignalAttempt(signalAttempt);
            }
            finally
            {
                if (!senderTransferred)
                    configuration.Sender.Dispose();
            }
        }

        internal void ConfigureExistingRosService(RosTopicState topic, string requestMessageName,
            string responseMessageName, int queueSize)
        {
            RosTopicState.ExistingTopicConfiguration configuration = topic.PrepareRosServiceConfiguration(
                responseMessageName, queueSize, requestMessageName);

            TopicMessageSender displaced = null;
            bool senderTransferred = false;
            string previousResponseName = null;
            TopicMessageSender responseCacheOwner = null;
            AttemptContext signalAttempt = null;
            try
            {
                RegistrationDefinition definition = PrepareRegistrationDefinition(topic,
                    RegistrationRole.RosService, configuration.MessageName,
                    responseMessageName: responseMessageName);
                m_BeforeExistingTopicConfigurationCommitTestHook?.Invoke();
                lock (m_ConnectionLifecycleLock)
                {
                    EnsureExistingTopicLocked(topic);
                    topic.ApplyRosServiceConfiguration(configuration, out displaced,
                        out RosTopicState responseTopic);
                    senderTransferred = true;
                    if (responseTopic != null)
                        responseTopic.ApplyMessageNameForTransaction(responseMessageName,
                            out previousResponseName, out responseCacheOwner);
                    AttemptContext attempt = m_CurrentAttempt;
                    if (RemoveDesiredRegistrationLocked(topic, RegistrationRole.Subscriber, attempt))
                        signalAttempt = attempt;
                    if (SetDesiredRegistrationLocked(definition, attempt, false))
                        signalAttempt = attempt;
                }
                responseCacheOwner?.InvalidateCache()?.Release(true);
                displaced?.Dispose();
                if (previousResponseName != null)
                    Debug.LogWarning($"Inconsistent declaration of topic '{topic.Topic}': was '{previousResponseName}', switching to '{responseMessageName}'.");
                SignalAttempt(signalAttempt);
            }
            finally
            {
                if (!senderTransferred)
                    configuration.Sender.Dispose();
            }
        }

        public void Subscribe<T>(string topic, Action<T> callback) where T : Message
        {
            string rosMessageName = MessageRegistry.GetRosMessageName<T>();
            AddSubscriberInternal(topic, rosMessageName, (Message msg) =>
            {
                if (msg.RosMessageName == rosMessageName)
                {
                    callback((T)msg);
                }
                else
                {
                    Debug.LogError($"Subscriber to '{topic}' expected '{rosMessageName}' but received '{msg.RosMessageName}'!?");
                }
            });
        }

        public void Unsubscribe(string topic)
        {
            RosTopicState info = GetTopic(topic);
            if (info != null)
                info.UnsubscribeAll();
        }

        // Version for when the message type is unknown at compile time
        public void SubscribeByMessageName(string topic, string rosMessageName, Action<Message> callback)
        {
            var constructor = MessageRegistry.GetDeserializeFunction(rosMessageName);
            if (constructor == null)
            {
                Debug.LogError($"Failed to subscribe to topic {topic} - no class has RosMessageName \"{rosMessageName}\"!");
                return;
            }

            AddSubscriberInternal(topic, rosMessageName, callback);
        }

        void AddSubscriberInternal(string topic, string rosMessageName, Action<Message> callback)
        {
            RosTopicState existing = GetTopic(topic);
            if (existing != null)
            {
                existing.AddSubscriber(callback, rosMessageName);
                return;
            }

            RosTopicState candidate = new RosTopicState(topic, rosMessageName, this, new InternalAPI(this), false);
            bool shouldRegister = candidate.AddSubscriberLocal(callback);
            RegistrationDefinition definition = shouldRegister
                ? PrepareRegistrationDefinition(candidate, RegistrationRole.Subscriber, rosMessageName) : null;
            RosTopicState published = PublishConfiguredTopic(candidate,
                definition == null ? null : new[] { definition });
            if (!ReferenceEquals(published, candidate))
            {
                candidate.MessageSender?.Dispose();
                published.AddSubscriber(callback, rosMessageName);
            }
        }

        // Implement a service in Unity
        public void ImplementService<TRequest, TResponse>(string topic, Func<TRequest, TResponse> callback, int? queueSize = null)
            where TRequest : Message
            where TResponse : Message
        {
            string requestName = MessageRegistry.GetRosMessageName<TRequest>();
            string responseName = MessageRegistry.GetRosMessageName<TResponse>();
            int resolvedQueueSize = queueSize.GetValueOrDefault(k_DefaultPublisherQueueSize);
            RosTopicState existing = GetTopic(topic);
            if (existing != null)
            {
                existing.ImplementService(callback, resolvedQueueSize, requestName);
                return;
            }

            RosTopicState candidate = new RosTopicState(topic, requestName, this, new InternalAPI(this), true);
            TopicMessageSender displaced;
            candidate.ConfigureUnityServiceLocal((Message msg) => callback((TRequest)msg), null,
                responseName, resolvedQueueSize, out displaced);
            displaced?.Dispose();
            RegistrationDefinition definition = PrepareRegistrationDefinition(candidate, RegistrationRole.UnityService,
                requestName, responseMessageName: responseName);
            RosTopicState published = PublishConfiguredTopic(candidate, new[] { definition });
            if (!ReferenceEquals(published, candidate))
            {
                candidate.MessageSender?.Dispose();
                published.ImplementService(callback, resolvedQueueSize);
            }
        }

        // Implement a service in Unity
        public void ImplementService<TRequest, TResponse>(string topic, Func<TRequest, Task<TResponse>> callback, int? queueSize = null)
            where TRequest : Message
            where TResponse : Message
        {
            string requestName = MessageRegistry.GetRosMessageName<TRequest>();
            string responseName = MessageRegistry.GetRosMessageName<TResponse>();
            int resolvedQueueSize = queueSize.GetValueOrDefault(k_DefaultPublisherQueueSize);
            RosTopicState existing = GetTopic(topic);
            if (existing != null)
            {
                existing.ImplementService(callback, resolvedQueueSize, requestName);
                return;
            }

            RosTopicState candidate = new RosTopicState(topic, requestName, this, new InternalAPI(this), true);
            TopicMessageSender displaced;
            candidate.ConfigureUnityServiceLocal(null, async (Message msg) => await callback((TRequest)msg),
                responseName, resolvedQueueSize, out displaced);
            displaced?.Dispose();
            RegistrationDefinition definition = PrepareRegistrationDefinition(candidate, RegistrationRole.UnityService,
                requestName, responseMessageName: responseName);
            RosTopicState published = PublishConfiguredTopic(candidate, new[] { definition });
            if (!ReferenceEquals(published, candidate))
            {
                candidate.MessageSender?.Dispose();
                published.ImplementService(callback, resolvedQueueSize);
            }
        }

        // Send a request to a ros service
        public async void SendServiceMessage<RESPONSE>(string rosServiceName, Message serviceRequest, Action<RESPONSE> callback) where RESPONSE : Message, new()
        {
            try
            {
                ServiceCall<RESPONSE> call = StartServiceCall<RESPONSE>(rosServiceName, serviceRequest);
                RESPONSE response = await call.Completion.Task;
                if (!TryBeginServiceCallback(call))
                    throw new IOException("The ROS service callback belongs to an inactive connection attempt.");
                // The permit may precede Disconnect even if this thread is descheduled here.
                // Invoke directly: no await/Post and no lifecycle/ownership lock around user code.
                callback(response);
            }
            catch (Exception e)
            {
                Debug.LogError("Exception sending service request or invoking service callback: " + e);
            }
        }

        // Return the typed completion itself, not the decoder task. Preparation still runs
        // synchronously, but (as with the former async entry point) its errors fault the Task.
        public Task<RESPONSE> SendServiceMessage<RESPONSE>(string rosServiceName, Message serviceRequest) where RESPONSE : Message, new()
        {
            return StartServiceCall<RESPONSE>(rosServiceName, serviceRequest).Completion.Task;
        }

        ServiceCall<RESPONSE> StartServiceCall<RESPONSE>(string rosServiceName, Message serviceRequest) where RESPONSE : Message, new()
        {
            var call = new ServiceCall<RESPONSE> { Topic = rosServiceName };
            MessageUse use = null;
            try
            {
                // Preserve the Task API's existing null-request failure type/timing.
                if (ReferenceEquals(serviceRequest, null))
                    throw new NullReferenceException("The ROS service request is null.");
                RosTopicState configuredTopic = GetTopic(rosServiceName);
                TopicMessageSender configuredSender = configuredTopic?.ServiceRequestSenderSnapshot();
                if (configuredTopic != null && configuredTopic.IsRosService)
                {
                    if (!configuredTopic.TryReservePendingServiceCall())
                        throw new IOException("The ROS service request pending-call capacity is full.");
                    call.PendingCapacityOwner = configuredTopic;
                }
                use = configuredSender != null ? configuredSender.AcquireMessageUse(serviceRequest)
                    : MessageUseRegistry.Acquire(serviceRequest, null);
                RosTopicState topicState = GetOrCreateTopic(rosServiceName, serviceRequest.RosMessageName, isService: true);
                TopicMessageSender serviceSender = topicState.ServiceRequestSenderSnapshot();
                call.TopicState = topicState;
                if (call.PendingCapacityOwner == null && topicState.IsRosService)
                {
                    if (!topicState.TryReservePendingServiceCall())
                        throw new IOException("The ROS service request pending-call capacity is full.");
                    call.PendingCapacityOwner = topicState;
                }
                if (!topicState.TryReserveServiceDecoder())
                    throw new IOException("The ROS service response decoder capacity is full.");
                call.DecoderCapacityOwner = topicState;
                lock (m_ConnectionLifecycleLock)
                {
                    call.Attempt = m_CurrentAttempt;
                    if (!IsLiveAttemptLocked(call.Attempt))
                        throw new IOException("A ROS service request requires a live connection attempt.");
                }
                topicState.NotifyServiceRequest(serviceRequest, call.Attempt);
                call.Id = Interlocked.Increment(ref m_NextSrvID) - 1;
                ExactMessageSender pair = topicState.CreateServiceRequest(use, call.Id, serviceSender);
                if (pair == null)
                    throw new IOException("The ROS service request queue is full.");
                use = null; // The exact capsule owns the same use acquired before notification.
                bool admitted = false;
                try
                {
                    // Snapshot the requested generic decoder separately from the observer's
                    // name/subtopic decoder. Do not change their types or notification order.
                    call.ResponseDeserializer = MessageRegistry.GetDeserializeFunction<RESPONSE>();
                    lock (m_ConnectionLifecycleLock)
                    {
                        if (!IsCurrentLocked(call.Attempt))
                            throw new IOException("The ROS service request connection attempt is no longer active.");
                        call.ResponseSnapshot = topicState.CaptureServiceResponseSnapshot();
                        call.Attempt.Calls.Add(call.Id, call);
                        call.Attempt.Outgoing.EnqueueExact(call.Attempt, pair);
                        admitted = true;
                    }
                    SignalAttempt(call.Attempt);
                }
                finally
                {
                    if (!admitted)
                        pair.AbortUnqueued();
                }

                Task decoder = DecodeServiceResponse(call);
                // The public Task can finish while this helper still runs. Observe even an
                // unexpected helper fault independently of whether anyone awaits that Task.
                decoder.ContinueWith(completed => { Exception observed = completed.Exception; },
                    CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
            catch (Exception exception)
            {
                FailServiceCall(call, exception);
            }
            finally { use?.Release(false); }
            return call;
        }

        bool IsServiceCallCurrentLocked(ServiceCall call)
        {
            return IsCurrentLocked(call.Attempt)
                && call.Attempt.Calls.TryGetValue(call.Id, out ServiceCall registered) && ReferenceEquals(call, registered);
        }

        bool TryBeginServiceDecode(ServiceCall call)
        {
            lock (m_ConnectionLifecycleLock)
            {
                if (call.State != ServiceCallState.RawReady || !IsServiceCallCurrentLocked(call))
                    return false;
                call.State = ServiceCallState.Decoding;
                call.DecoderClaimed = true;
                return true;
            }
        }

        bool TryContinueServiceDecode(ServiceCall call)
        {
            lock (m_ConnectionLifecycleLock)
                return call.State == ServiceCallState.Decoding && IsServiceCallCurrentLocked(call);
        }

        bool TryBeginServiceCallback(ServiceCall call)
        {
            lock (m_ConnectionLifecycleLock)
            {
                if (call.State != ServiceCallState.Succeeded || call.CallbackStarted
                    || !IsCurrentLocked(call.Attempt))
                    return false;
                call.CallbackStarted = true;
                return true;
            }
        }

        bool TryCommitServiceResult(ServiceCall call)
        {
            RosTopicState pendingCapacityOwner;
            lock (m_ConnectionLifecycleLock)
            {
                if (call.State != ServiceCallState.Decoding || !IsServiceCallCurrentLocked(call))
                    return false;
                // This transition, not scheduling Task continuations, competes with revocation.
                call.State = ServiceCallState.Succeeded;
                call.Attempt.Calls.Remove(call.Id);
                pendingCapacityOwner = call.PendingCapacityOwner;
                call.PendingCapacityOwner = null;
            }
            pendingCapacityOwner?.ReleasePendingServiceCall();
            return true;
        }

        void FailServiceCall(ServiceCall call, Exception exception)
        {
            RosTopicState pendingCapacityOwner;
            RosTopicState decoderCapacityOwner;
            lock (m_ConnectionLifecycleLock)
            {
                if (call.State == ServiceCallState.Succeeded || call.State == ServiceCallState.Invalidated
                    || call.State == ServiceCallState.Faulted)
                    return;
                call.State = ServiceCallState.Faulted;
                if (call.Attempt != null && call.Attempt.Calls.TryGetValue(call.Id, out ServiceCall registered) && ReferenceEquals(call, registered))
                    call.Attempt.Calls.Remove(call.Id);
                pendingCapacityOwner = call.PendingCapacityOwner;
                call.PendingCapacityOwner = null;
                decoderCapacityOwner = call.DecoderClaimed ? null : call.DecoderCapacityOwner;
                if (decoderCapacityOwner != null)
                    call.DecoderCapacityOwner = null;
            }
            pendingCapacityOwner?.ReleasePendingServiceCall();
            decoderCapacityOwner?.ReleaseServiceDecoder();
            call.CompleteFailure(exception);
        }

        async Task DecodeServiceResponse<RESPONSE>(ServiceCall<RESPONSE> call) where RESPONSE : Message
        {
            try
            {
                byte[] rawResponse = await call.RawResponse.Task;
                m_ServiceResponseContinuationTestHook?.Invoke();
                if (!TryBeginServiceDecode(call))
                    return;
                if (call.ResponseSnapshot != null)
                    call.ResponseSnapshot.Topic.OnServiceResponseForAttempt(rawResponse, call.Attempt, call.ResponseSnapshot);
                else
                    call.TopicState.OnMessageReceivedForAttempt(rawResponse, call.Attempt);
                if (!TryContinueServiceDecode(call))
                    return;
                var messageDeserializer = new MessageDeserializer();
                messageDeserializer.InitWithBuffer(rawResponse);
                RESPONSE result = (RESPONSE)call.ResponseDeserializer(messageDeserializer);
                if (TryCommitServiceResult(call))
                    call.Completion.TrySetResult(result);
            }
            catch (Exception exception)
            {
                // Catch observes decoder/observer faults even when invalidation already won.
                FailServiceCall(call, exception);
            }
            finally { ReleaseServiceDecoderTicket(call); }
        }

        void ReleaseServiceDecoderTicket(ServiceCall call)
        {
            RosTopicState decoderCapacityOwner;
            lock (m_ConnectionLifecycleLock)
            {
                decoderCapacityOwner = call.DecoderCapacityOwner;
                call.DecoderCapacityOwner = null;
            }
            decoderCapacityOwner?.ReleaseServiceDecoder();
        }

        SysCommandSender PrepareTopicListCommand()
        {
            var serializer = new MessageSerializer();
            PopulateSysCommand(serializer, SysCommand.k_SysCommand_TopicList, new SysCommand_TopicsRequest());
            return new SysCommandSender(serializer.GetBytesSequence());
        }

        AttemptContext CaptureLiveAttempt(string errorMessage)
        {
            lock (m_ConnectionLifecycleLock)
            {
                if (!IsLiveAttemptLocked(m_CurrentAttempt))
                    throw new IOException(errorMessage);
                return m_CurrentAttempt;
            }
        }

        bool TryCommitTopicListRequest(AttemptContext attempt, SysCommandSender sender,
            Action<string[]> topicCallback, Action<Dictionary<string, string>> typeCallback)
        {
            lock (m_ConnectionLifecycleLock)
            {
                if (!IsCurrentLocked(attempt) || attempt.State == AttemptState.OfflineOpen
                    || attempt.TopicListOutstanding >= k_MaxTopicListRequests)
                    return false;

                int topicCount = attempt.TopicCallbacks.Count;
                int typeCount = attempt.TypeCallbacks.Count;
                try
                {
                    if (topicCallback != null)
                        attempt.TopicCallbacks.Add(new AttemptCallback<string[]>(attempt, topicCallback));
                    else
                        attempt.TypeCallbacks.Add(new AttemptCallback<Dictionary<string, string>>(attempt, typeCallback));
                    attempt.Outgoing.Enqueue(attempt, sender);
                    attempt.TopicListOutstanding++;
                    return true;
                }
                catch
                {
                    while (attempt.TopicCallbacks.Count > topicCount)
                        attempt.TopicCallbacks.RemoveAt(attempt.TopicCallbacks.Count - 1);
                    while (attempt.TypeCallbacks.Count > typeCount)
                        attempt.TypeCallbacks.RemoveAt(attempt.TypeCallbacks.Count - 1);
                    throw;
                }
            }
        }

        public void GetTopicList(Action<string[]> callback)
        {
            AttemptContext attempt = CaptureLiveAttempt("A topic-list request requires a live connection attempt.");
            SysCommandSender sender = PrepareTopicListCommand();
            if (!TryCommitTopicListRequest(attempt, sender, callback, null))
                throw new IOException("The topic-list attempt is no longer active or its request capacity is full.");
            SignalAttempt(attempt);
        }

        public void GetTopicAndTypeList(Action<Dictionary<string, string>> callback)
        {
            AttemptContext attempt = CaptureLiveAttempt("A topics-and-types request requires a live connection attempt.");
            SysCommandSender sender = PrepareTopicListCommand();
            if (!TryCommitTopicListRequest(attempt, sender, null, callback))
                throw new IOException("The topic-list attempt is no longer active or its request capacity is full.");
            SignalAttempt(attempt);
        }

        [Obsolete("Calling Subscribe now implicitly registers a subscriber")]
        public void RegisterSubscriber(string topic, string rosMessageName)
        {
        }

        public RosTopicState RegisterPublisher<T>(string rosTopicName,
            int? queue_size = null, bool? latch = null) where T : Message
        {
            return RegisterPublisher(rosTopicName, MessageRegistry.GetRosMessageName<T>(), queue_size, latch);
        }

        public RosTopicState RegisterPublisher(string rosTopicName, string rosMessageName,
            int? queueSize = null, bool? latch = null)
        {
            if (string.IsNullOrEmpty(rosMessageName))
                throw new ArgumentException("rosMessageName cannot be null or empty.", nameof(rosMessageName));
            int resolvedQueueSize = queueSize.GetValueOrDefault(k_DefaultPublisherQueueSize);
            bool resolvedLatch = latch.GetValueOrDefault(k_DefaultPublisherLatch);
            RosTopicState existing = GetTopic(rosTopicName);
            if (existing != null)
            {
                existing.RegisterPublisher(resolvedQueueSize, resolvedLatch, rosMessageName);
                return existing;
            }

            RosTopicState candidate = new RosTopicState(rosTopicName, rosMessageName, this,
                new InternalAPI(this), false);
            RegistrationDefinition definition = PrepareRegistrationDefinition(candidate, RegistrationRole.Publisher,
                rosMessageName, resolvedQueueSize, resolvedLatch);
            TopicMessageSender displaced;
            candidate.ConfigurePublisherLocal(resolvedQueueSize, resolvedLatch, out displaced);
            displaced?.Dispose();
            RosTopicState published = PublishConfiguredTopic(candidate, new[] { definition });
            if (!ReferenceEquals(published, candidate))
            {
                candidate.MessageSender?.Dispose();
                published.RegisterPublisher(resolvedQueueSize, resolvedLatch, rosMessageName);
            }
            return published;
        }

        public void RegisterRosService<TRequest, TResponse>(string topic) where TRequest : Message where TResponse : Message
        {
            RegisterRosService(topic, MessageRegistry.GetRosMessageName<TRequest>(), MessageRegistry.GetRosMessageName<TResponse>());
        }

        public void RegisterRosService(string topic, string requestMessageName, string responseMessageName, int? queueSize = null)
        {
            if (string.IsNullOrEmpty(requestMessageName))
                throw new ArgumentException("requestMessageName cannot be null or empty.", nameof(requestMessageName));
            if (string.IsNullOrEmpty(responseMessageName))
                throw new ArgumentException("responseMessageName cannot be null or empty.", nameof(responseMessageName));
            int resolvedQueueSize = queueSize.GetValueOrDefault(k_DefaultPublisherQueueSize);
            RosTopicState existing = GetTopic(topic);
            if (existing != null)
            {
                existing.RegisterRosService(requestMessageName, responseMessageName, resolvedQueueSize);
                return;
            }

            RosTopicState candidate = new RosTopicState(topic, requestMessageName, this,
                new InternalAPI(this), true);
            TopicMessageSender displaced;
            candidate.ConfigureRosServiceLocal(responseMessageName, resolvedQueueSize, out displaced);
            displaced?.Dispose();
            RegistrationDefinition definition = PrepareRegistrationDefinition(candidate, RegistrationRole.RosService,
                requestMessageName, responseMessageName: responseMessageName);
            RosTopicState published = PublishConfiguredTopic(candidate, new[] { definition });
            if (!ReferenceEquals(published, candidate))
            {
                candidate.MessageSender?.Dispose();
                published.RegisterRosService(requestMessageName, responseMessageName, resolvedQueueSize);
            }
        }

        [Obsolete("Calling ImplementUnityService now implicitly registers it")]
        public void RegisterUnityService(string topic, string rosMessageName)
        {
        }

        internal struct InternalAPI
        {
            ROSConnection m_Self;

            public InternalAPI(ROSConnection self) { m_Self = self; }

            public bool HasAcceptingAttempt => m_Self.HasAcceptingAttempt;

            public bool TryGetAcceptingAttempt(out AttemptContext attempt)
            {
                return m_Self.TryGetAcceptingAttempt(out attempt);
            }

            public void SendServiceRequest(int serviceId)
            {
                m_Self.QueueSysCommand(SysCommand.k_SysCommand_ServiceRequest, new SysCommand_Service { srv_id = serviceId });
            }

            public bool AddSenderToQueue(OutgoingMessageSender sender)
            {
                return m_Self.TryEnqueueSender(sender);
            }

            public bool TryGetPublicationAttempt(out AttemptContext attempt)
            {
                return m_Self.TryGetPublicationAttempt(out attempt);
            }

            public bool TryBeginPublication(TopicMessageSender sender, out AttemptContext attempt,
                out TopicMessageSender.PendingQueue queue)
            {
                return m_Self.TryBeginPublication(sender, out attempt, out queue);
            }

            public void AbortPublicationPreparation(TopicMessageSender.PendingQueue queue)
            {
                m_Self.AbortPublicationPreparation(queue);
            }

            public bool CommitPublication(TopicMessageSender sender, AttemptContext attempt,
                TopicMessageSender.PendingQueue queue, TopicMessageSender.OwnedPublication publication)
            {
                return m_Self.CommitPublication(sender, attempt, queue, publication);
            }

            public bool QueueMessage(TopicMessageSender sender, MessageUse use, AttemptContext expectedAttempt)
            {
                return m_Self.TryQueueMessage(sender, use, expectedAttempt);
            }

            public bool QueueServicePairForUse(string command, object param, TopicMessageSender sender, MessageUse use, AttemptContext expectedAttempt)
            {
                return m_Self.TryQueueServicePairForUse(command, param, sender, use, expectedAttempt);
            }

            public ExactMessageSender CreateServicePair(string command, object param, TopicMessageSender sender, MessageUse use)
            {
                var serializer = new MessageSerializer();
                PopulateSysCommand(serializer, command, param);
                return sender.CreateExactSenderForUse(use, serializer.GetBytesSequence());
            }

            public bool QueueLatch(TopicMessageSender sender, AttemptContext attempt)
            {
                return m_Self.TryQueueLatch(sender, attempt);
            }
        }

        static ROSConnection _instance;

        public static ROSConnection GetOrCreateInstance()
        {
            if (_instance == null)
            {
                // Prefer to use the ROSConnection in the scene, if any
                _instance = FindObjectOfType<ROSConnection>();
                if (_instance != null)
                    return _instance;

                GameObject prefab = Resources.Load<GameObject>("ROSConnectionPrefab");
                if (prefab == null)
                {
                    Debug.LogWarning("No settings for ROSConnection.instance! Open \"ROS Settings\" from the Robotics menu to configure it.");
                    GameObject instance = new GameObject("ROSConnection");
                    _instance = instance.AddComponent<ROSConnection>();
                }
                else
                {
                    _instance = Instantiate(prefab).GetComponent<ROSConnection>();
                }
            }
            return _instance;
        }

        [Obsolete("Please call ROSConnection.GetOrCreateInstance()")]
        public static ROSConnection instance
        {
            get
            {
                return GetOrCreateInstance();
            }
        }

        void Awake()
        {
            if (_instance == null)
                _instance = this;
            ConnectorDomainLifetime.Register(this);
        }

        void Start()
        {
            InitializeHUD();

            HudPanel.RegisterHeader(DrawHeaderGUI);

            if (listenForTFMessages)
                TFSystem.GetOrCreateInstance();

            if (ConnectOnStart)
                Connect();
        }

        public void Connect()
        {
            ROSConnectionConfig config;
            lock (m_ConnectionLifecycleLock)
            {
                if (m_WorkerSession != null || m_Lifecycle == LifecycleState.Stopping || m_Lifecycle == LifecycleState.Disposed)
                    return;
                config = m_ConnectionConfig;
            }
            // Unity object validation and scalar reads stay on the calling/main thread, outside G.
            if (config == null || !config.IsValid)
                throw new InvalidOperationException("ROSConnection requires a valid ROSConnectionConfig before connecting.");
            var session = new WorkerSession(config.RosIPAddress, config.RosPort, m_NetworkTimeoutSeconds,
                m_KeepaliveTime, (int)(m_SleepTimeSeconds * 1000.0f));
            AttemptContext firstAttempt;
            bool reserved;
            lock (m_ConnectionLifecycleLock)
            {
                reserved = m_WorkerSession == null && m_Lifecycle != LifecycleState.Stopping
                    && m_Lifecycle != LifecycleState.Disposed && ReferenceEquals(config, m_ConnectionConfig);
                if (reserved && m_Lifecycle == LifecycleState.StoppedAuthorized
                    && m_CurrentAttempt != null && m_CurrentAttempt.OfflineEpoch && IsCurrentLocked(m_CurrentAttempt))
                {
                    // Bind the one offline epoch to the first attempt. Its outbox is
                    // intentionally retained; later retry attempts get new roots.
                    firstAttempt = m_CurrentAttempt;
                    firstAttempt.BindToSession(session);
                }
                else
                {
                    firstAttempt = reserved ? new AttemptContext(++m_NextAttemptId, session) : null;
                }
                if (reserved)
                {
                    m_WorkerSession = session;
                    m_Lifecycle = LifecycleState.Running;
                    session.Attempt = firstAttempt;
                    m_CurrentAttempt = firstAttempt;
                }
            }
            if (!reserved)
            {
                session.Cancellation.Dispose();
                session.WakeSignal.Dispose();
                return;
            }
            var startGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            try
            {
                Task worker = Task.Run(async () =>
                {
                    m_BeforeWorkerStartGateTestHook?.Invoke();
                    await startGate.Task;
                    await ConnectionThread(session, firstAttempt);
                });
                lock (m_ConnectionLifecycleLock)
                    session.Worker = worker;
                worker.ContinueWith(completed => CompleteConnectionWorker(session, completed),
                    CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                startGate.SetResult(true);
            }
            catch
            {
                CompleteConnectionWorker(session, null);
                throw;
            }
        }

        void CompleteConnectionWorker(WorkerSession session, Task completed)
        {
            Exception fault = completed != null && completed.IsFaulted ? completed.Exception : null;
            lock (m_ConnectionLifecycleLock)
            {
                if (!ReferenceEquals(m_WorkerSession, session))
                    return;
                session.WorkerTerminal = true;
            }
            TryFinalizeSession(session);
            if (fault != null) Debug.LogException(fault);
            m_WorkerBodyCompletedTestHook?.Invoke();
        }

        void TryFinalizeSession(WorkerSession session)
        {
            lock (m_ConnectionLifecycleLock)
            {
                if (!ReferenceEquals(m_WorkerSession, session) || session.Finalizing || !session.WorkerTerminal
                    || (session.ExplicitStop && !session.StopCompleted))
                    return;
                if (session.ResourcesDisposed)
                {
                    ReleaseSessionLocked(session);
                    return;
                }
                session.Finalizing = true;
            }
            // A stop plan pins this CTS until its Cancel call and mandatory cleanup finish.
            Exception disposalFailure = null;
            try
            {
                try { m_SessionDisposalTestHook?.Invoke(); }
                catch (Exception exception) { disposalFailure = exception; }
                try { session.Cancellation.Dispose(); }
                catch (Exception exception) { disposalFailure = disposalFailure ?? exception; }
                try { session.WakeSignal.Dispose(); }
                catch (Exception exception) { disposalFailure = disposalFailure ?? exception; }
            }
            finally
            {
                lock (m_ConnectionLifecycleLock)
                {
                    session.ResourcesDisposed = true;
                    session.Finalizing = false;
                    if (ReferenceEquals(m_WorkerSession, session) && (!session.ExplicitStop || session.StopCompleted))
                        ReleaseSessionLocked(session);
                }
            }
            if (disposalFailure != null)
                Debug.Log("Session resource disposal failed: " + disposalFailure);
        }

        void ReleaseSessionLocked(WorkerSession session)
        {
            m_WorkerSession = null;
            if (m_Lifecycle != LifecycleState.Disposed)
                m_Lifecycle = session.ExplicitStop ? LifecycleState.StoppedAuthorized : LifecycleState.StoppedUnauthorized;
        }

        // NB this callback is not running on the main thread, be cautious about modifying data here
        void OnConnectionStartedCallback(NetworkStream stream, AttemptContext attempt)
        {
            // All role registrations are admitted through the same attempt/revision
            // reconciler. It prepends the stable registration prefix before any
            // offline publication or raw command already in this attempt's outbox.
            ReconcileRegistrations(attempt);
        }

        bool IsCurrentLocked(AttemptContext attempt)
        {
            return attempt != null && ReferenceEquals(m_CurrentAttempt, attempt) && attempt.IsOpen;
        }

        bool IsLiveAttemptLocked(AttemptContext attempt)
        {
            return IsCurrentLocked(attempt) && attempt.State != AttemptState.OfflineOpen;
        }

        void RevokeAttemptLocked(AttemptContext attempt)
        {
            if (attempt != null && attempt.IsOpen) attempt.State = AttemptState.Closing;
        }

        void RetireAttempt(AttemptContext attempt)
        {
            if (attempt == null) return;
            Dictionary<int, ServiceCall> calls;
            lock (m_ConnectionLifecycleLock)
            {
                RevokeAttemptLocked(attempt);
                if (attempt.RetiredCalls == null)
                {
                    attempt.RetiredCalls = attempt.Calls;
                    attempt.Calls = new Dictionary<int, ServiceCall>();
                }
                calls = attempt.RetiredCalls;
                attempt.Incoming = new ConcurrentQueue<IncomingMessage>();
                attempt.PendingPair = null;
                RevokeCallbacksLocked(attempt.TopicCallbacks.ToArray());
                RevokeCallbacksLocked(attempt.TypeCallbacks.ToArray());
                attempt.TopicCallbacks = new List<AttemptCallback<string[]>>();
                attempt.TypeCallbacks = new List<AttemptCallback<Dictionary<string, string>>>();
                attempt.TopicListOutstanding = 0;
            }
            // Only these detached roots can be terminalized. A late A cleanup has no
            // reference to B's mailbox, pair parser, waiters, registration or channel queues.
            foreach (ServiceCall call in calls.Values)
            {
                bool complete;
                bool invalidated;
                RosTopicState pendingCapacityOwner;
                RosTopicState decoderCapacityOwner;
                lock (m_ConnectionLifecycleLock)
                {
                    complete = call.State != ServiceCallState.Succeeded && call.State != ServiceCallState.Faulted
                        && call.State != ServiceCallState.Invalidated;
                    if (complete) call.State = ServiceCallState.Invalidated;
                    invalidated = call.State == ServiceCallState.Invalidated;
                    pendingCapacityOwner = call.PendingCapacityOwner;
                    call.PendingCapacityOwner = null;
                    decoderCapacityOwner = complete && !call.DecoderClaimed ? call.DecoderCapacityOwner : null;
                    if (decoderCapacityOwner != null)
                        call.DecoderCapacityOwner = null;
                }
                pendingCapacityOwner?.ReleasePendingServiceCall();
                decoderCapacityOwner?.ReleaseServiceDecoder();
                if (invalidated)
                {
                    // A second stop owner can deliver the already-selected terminal result
                    // without waiting for the selecting thread. TCS commits remain one-shot.
                    try { if (complete) m_ServiceFailureSelectedTestHook?.Invoke(); }
                    finally { call.CompleteFailure(new IOException("ROS connection was lost before the service response completed.")); }
                }
            }
            lock (m_ConnectionLifecycleLock)
                if (ReferenceEquals(attempt.RetiredCalls, calls)) attempt.RetiredCalls = null;
            attempt.Outgoing.ClearQueuedData(ClearOutgoingEntry);
        }

        internal TopicMessageSender.PendingQueue GetSenderQueue(TopicMessageSender sender)
        {
            lock (m_ConnectionLifecycleLock)
                return IsCurrentLocked(m_CurrentAttempt) ? m_CurrentAttempt.Outgoing.Channel(sender) : null;
        }

        internal void SetDesiredRegistration(RegistrationDefinition definition)
        {
            if (definition == null)
                return;
            AttemptContext signalAttempt = null;
            lock (m_ConnectionLifecycleLock)
            {
                AttemptContext attempt = m_CurrentAttempt;
                if (SetDesiredRegistrationLocked(definition, attempt, true))
                    signalAttempt = attempt;
            }
            SignalAttempt(signalAttempt);
        }

        bool SetDesiredRegistrationLocked(RegistrationDefinition definition, AttemptContext attempt, bool prepend)
        {
            int index = -1;
            for (int i = 0; i < m_DesiredRegistrations.Count; i++)
            {
                RegistrationDefinition current = m_DesiredRegistrations[i];
                if (ReferenceEquals(current.Topic, definition.Topic) && current.Role == definition.Role)
                {
                    index = i;
                    break;
                }
            }
            RegistrationDefinition effective = definition;
            if (index >= 0 && m_DesiredRegistrations[index].IsEquivalentTo(definition))
                effective = m_DesiredRegistrations[index];
            else if (index >= 0)
                m_DesiredRegistrations[index] = definition;
            else
                m_DesiredRegistrations.Add(definition);

            return IsCurrentLocked(attempt)
                && EnsureRegistrationLocked(attempt, effective.Topic, effective.Role, effective, prepend);
        }

        internal void ReplaceDesiredRegistration(RosTopicState topic, RegistrationRole removedRole,
            RegistrationDefinition replacement)
        {
            if (replacement == null)
                return;
            AttemptContext signalAttempt = null;
            lock (m_ConnectionLifecycleLock)
            {
                AttemptContext attempt = m_CurrentAttempt;
                if (RemoveDesiredRegistrationLocked(topic, removedRole, attempt))
                    signalAttempt = attempt;
                if (SetDesiredRegistrationLocked(replacement, attempt, false))
                    signalAttempt = attempt;
            }
            SignalAttempt(signalAttempt);
        }

        internal void RemoveDesiredRegistration(RosTopicState topic, RegistrationRole role)
        {
            AttemptContext signalAttempt = null;
            lock (m_ConnectionLifecycleLock)
            {
                AttemptContext attempt = m_CurrentAttempt;
                if (RemoveDesiredRegistrationLocked(topic, role, attempt))
                    signalAttempt = attempt;
            }
            SignalAttempt(signalAttempt);
        }

        bool RemoveDesiredRegistrationLocked(RosTopicState topic, RegistrationRole role, AttemptContext attempt)
        {
            for (int i = 0; i < m_DesiredRegistrations.Count; i++)
            {
                RegistrationDefinition current = m_DesiredRegistrations[i];
                if (ReferenceEquals(current.Topic, topic) && current.Role == role)
                {
                    m_DesiredRegistrations.RemoveAt(i);
                    break;
                }
            }
            return IsCurrentLocked(attempt)
                && EnsureRegistrationLocked(attempt, topic, role, null, true);
        }

        RegistrationBinding FindRegistrationBindingLocked(AttemptContext attempt, RosTopicState topic,
            RegistrationRole role)
        {
            foreach (RegistrationBinding binding in attempt.Registrations)
                if (ReferenceEquals(binding.Topic, topic) && binding.Role == role)
                    return binding;
            return null;
        }

        bool EnsureRegistrationLocked(AttemptContext attempt, RosTopicState topic, RegistrationRole role,
            RegistrationDefinition desired, bool prepend)
        {
            if (!IsCurrentLocked(attempt) || attempt.State == AttemptState.OfflineOpen)
                return false;

            RegistrationBinding binding = FindRegistrationBindingLocked(attempt, topic, role);
            if (binding == null)
            {
                binding = new RegistrationBinding(attempt, topic, role);
                attempt.Registrations.Add(binding);
            }
            binding.Desired = desired;

            RegistrationUnit active = binding.Active;
            if (active != null)
            {
                bool stillWanted = (!active.Remove && desired != null && ReferenceEquals(active.Definition, desired))
                    || (active.Remove && desired == null && ReferenceEquals(active.Definition, binding.Written));
                if (stillWanted)
                    return false;
                if (active.Phase == RegistrationPhase.Writing)
                    return false;
                attempt.Outgoing.RemoveRegistration(active);
                active.Cancelled = true;
                active.Phase = RegistrationPhase.Absent;
                binding.Active = null;
                binding.Phase = binding.Written == null ? RegistrationPhase.Absent : RegistrationPhase.Written;
            }

            if (desired != null && ReferenceEquals(binding.Written, desired))
            {
                binding.Phase = RegistrationPhase.Written;
                return false;
            }

            RegistrationUnit next = null;
            if (binding.Written != null)
                next = new RegistrationUnit(binding, binding.Written, true);
            else if (desired != null)
                next = new RegistrationUnit(binding, desired, false);
            else
            {
                binding.Phase = RegistrationPhase.Absent;
                return false;
            }

            binding.Active = next;
            binding.Phase = RegistrationPhase.Queued;
            if (prepend)
                attempt.Outgoing.EnqueueRegistrationFirst(attempt, next);
            else
                attempt.Outgoing.EnqueueRegistrationAfterPrefix(attempt, next);
            return true;
        }

        internal void ReconcileRegistrations(AttemptContext attempt)
        {
            bool signal = false;
            RosTopicState[] topics;
            lock (m_ConnectionLifecycleLock)
            {
                if (!IsCurrentLocked(attempt) || attempt.State == AttemptState.OfflineOpen)
                    return;
                var definitions = new List<RegistrationDefinition>(m_DesiredRegistrations);
                for (int i = definitions.Count - 1; i >= 0; i--)
                {
                    RegistrationDefinition definition = definitions[i];
                    signal |= EnsureRegistrationLocked(attempt, definition.Topic, definition.Role, definition, true);
                }
                topics = m_Topics.Values.ToArray();
            }
            // Latch replay uses the same reconciler entry point as registration
            // admission, but its sender/cache work is deliberately outside G.
            foreach (RosTopicState topic in topics)
            {
                if (!topic.TryGetLatchedPublisher(out TopicMessageSender sender))
                    continue;
                signal |= TryQueueLatchCore(sender, attempt);
            }
            if (signal)
                SignalAttempt(attempt);
        }

        internal bool TryBeginRegistrationWrite(RegistrationUnit registration, AttemptContext attempt)
        {
            lock (m_ConnectionLifecycleLock)
            {
                if (registration == null || registration.Cancelled || !IsCurrentLocked(attempt)
                    || !ReferenceEquals(registration.Binding.Attempt, attempt)
                    || !ReferenceEquals(registration.Binding.Active, registration)
                    || registration.Phase != RegistrationPhase.Queued)
                    return false;
                registration.Phase = RegistrationPhase.Writing;
                registration.Binding.Phase = RegistrationPhase.Writing;
                return true;
            }
        }

        internal void CompleteRegistrationWrite(RegistrationUnit registration, AttemptContext attempt, bool success)
        {
            bool signal = false;
            lock (m_ConnectionLifecycleLock)
            {
                if (registration == null || !ReferenceEquals(registration.Binding.Attempt, attempt)
                    || !ReferenceEquals(registration.Binding.Active, registration))
                    return;

                registration.Phase = success ? RegistrationPhase.Written : RegistrationPhase.Absent;
                registration.Binding.Active = null;
                if (success)
                {
                    if (registration.Remove)
                    {
                        if (ReferenceEquals(registration.Binding.Written, registration.Definition))
                            registration.Binding.Written = null;
                    }
                    else
                        registration.Binding.Written = registration.Definition;
                }
                registration.Binding.Phase = registration.Binding.Written == null
                    ? RegistrationPhase.Absent : RegistrationPhase.Written;
                if (success && registration.Remove && registration.Binding.Desired == null)
                    registration.Binding.Phase = RegistrationPhase.Absent;
                if (IsCurrentLocked(attempt))
                    signal = EnsureRegistrationLocked(attempt, registration.Binding.Topic, registration.Binding.Role,
                        registration.Binding.Desired, true);
            }
            if (signal)
                SignalAttempt(attempt);
        }

        internal void CancelRegistrationUnit(RegistrationUnit registration)
        {
            if (registration == null)
                return;
            lock (m_ConnectionLifecycleLock)
            {
                registration.Cancelled = true;
                registration.Phase = RegistrationPhase.Absent;
                if (ReferenceEquals(registration.Binding.Active, registration))
                {
                    registration.Binding.Active = null;
                    registration.Binding.Phase = registration.Binding.Written == null
                        ? RegistrationPhase.Absent : RegistrationPhase.Written;
                }
            }
        }

        internal bool RegistrationSent(RosTopicState topic, bool publisher)
        {
            RegistrationRole role = publisher ? RegistrationRole.Publisher : RegistrationRole.Subscriber;
            lock (m_ConnectionLifecycleLock)
            {
                if (!IsCurrentLocked(m_CurrentAttempt)) return false;
                RegistrationBinding binding = FindRegistrationBindingLocked(m_CurrentAttempt, topic, role);
                return binding != null && binding.Phase == RegistrationPhase.Written
                    && ReferenceEquals(binding.Written, binding.Desired);
            }
        }

        public void Disconnect()
        {
            StopPlan plan;
            lock (m_ConnectionLifecycleLock)
            {
                WorkerSession session = m_WorkerSession;
                if (m_StopPlan != null || (session != null && session.ExplicitStop))
                    return;
                AttemptContext attempt = m_CurrentAttempt;
                RevokeAttemptLocked(attempt);
                plan = new StopPlan(session, attempt);
                m_StopPlan = plan;
                if (session != null)
                {
                    session.ExplicitStop = true;
                    session.Stop = plan;
                }
                if (m_Lifecycle != LifecycleState.Disposed)
                    m_Lifecycle = LifecycleState.Stopping;
            }
            Exception cancellationFailure = null;
            try
            {
                m_StopPlanCapturedTestHook?.Invoke();
                plan.Session?.WakeSignal.Signal();
                // Closing the captured socket must precede synchronous cancellation callbacks.
                try { plan.Transport?.CloseOnce(m_SocketCloseTestHook); }
                catch (Exception exception) { cancellationFailure = exception; }
                try { if (plan.CancelSession) plan.Session.Cancellation.Cancel(); }
                catch (Exception exception) { cancellationFailure = exception; }
            }
            finally
            {
                try
                {
                    RetireAttempt(plan.Attempt);
                }
                finally
                {
                    lock (m_ConnectionLifecycleLock)
                    {
                        if (ReferenceEquals(m_StopPlan, plan)) m_StopPlan = null;
                        if (plan.Session != null) plan.Session.StopCompleted = true;
                        else
                        {
                            if (plan.Attempt != null)
                            {
                                plan.Attempt.State = AttemptState.Closed;
                                if (ReferenceEquals(m_CurrentAttempt, plan.Attempt))
                                    m_CurrentAttempt = null;
                            }
                            if (m_Lifecycle != LifecycleState.Disposed) m_Lifecycle = LifecycleState.StoppedAuthorized;
                        }
                    }
                    if (plan.Session != null) TryFinalizeSession(plan.Session);
                }
            }
            if (cancellationFailure != null) throw cancellationFailure;
        }

        void OnValidate()
        {
            // the prefab is not the instance!
            if (gameObject.scene.name == null)
                return;

            if (_instance == null)
                _instance = this;
        }


        void Update()
        {
            s_RealTimeSinceStartup = Time.realtimeSinceStartup;

            AttemptContext attempt;
            lock (m_ConnectionLifecycleLock) attempt = m_CurrentAttempt;
            if (attempt == null) return;
            IncomingMessage data;
            while (attempt.Incoming.TryDequeue(out data))
                DispatchIncomingMessage(data);
        }

        void DispatchIncomingMessage(IncomingMessage data)
        {
            AttemptContext attempt;
            PendingPair pendingPair;
            lock (m_ConnectionLifecycleLock)
            {
                if (!IsCurrentLocked(data.Attempt))
                    return;
                attempt = data.Attempt;
                pendingPair = attempt.PendingPair;
                attempt.PendingPair = null;
            }

            string topic = data.Topic;
            byte[] contents = data.Contents;
            m_LastMessageReceivedRealtime = Time.realtimeSinceStartup;

            if (pendingPair != null)
            {
                HandlePendingPair(pendingPair, topic, contents);
            }
            else if (topic.StartsWith("__"))
            {
                ReceiveSysCommand(data.Attempt, topic, Encoding.UTF8.GetString(contents));
            }
                else
                {
                    RosTopicState topicInfo = GetTopic(topic);
                    // if this is null, we have received a message on a topic we've never heard of...!?
                    // all we can do is ignore it, we don't even know what type it is
                    if (topicInfo != null)
                    {
                        try
                        {
                            //Add a try catch so that bad logic from one received message doesn't
                            //cause the Update method to exit without processing other received messages.
                            topicInfo.OnMessageReceivedForAttempt(contents, data.Attempt);
                        }
                        catch (Exception e)
                        {
                            Debug.LogException(e);
                        }

                    }
                }
        }

        float m_LastTopicsRequestRealtime = -1;
        const float k_TimeBetweenTopicsUpdates = 5.0f;
        const int k_MaxTopicListRequests = 256;

        public void RefreshTopicsList()
        {
            // cap the rate of requests
            if (m_LastTopicsRequestRealtime != -1 && s_RealTimeSinceStartup - m_LastTopicsRequestRealtime <= k_TimeBetweenTopicsUpdates)
                return;

            AttemptContext attempt;
            lock (m_ConnectionLifecycleLock)
                attempt = m_CurrentAttempt != null && m_CurrentAttempt.IsOpen ? m_CurrentAttempt : null;
            if (attempt == null)
                return;
            m_LastTopicsRequestRealtime = s_RealTimeSinceStartup;
            SysCommandSender sender = PrepareTopicListCommand();
            if (!TryCommitTopicListRequest(attempt, sender, null, (data) =>
            {
                foreach (KeyValuePair<string, string> kv in data)
                {
                    if (!IsAttemptAccepting(attempt))
                        break;
                    RosTopicState state = GetOrCreateTopicForAttempt(kv.Key, kv.Value, false, attempt);
                    if (state == null || !IsAttemptAccepting(attempt))
                        break;
                }
            }))
                return;
            SignalAttempt(attempt);
        }

        void HandlePendingPair(PendingPair pendingPair, string serviceTopic, byte[] contents)
        {
            if (!IsAttemptAccepting(pendingPair.Attempt))
                return;

            // Pair state is consumed before this hook and all routing happens after it.
            // The hook is intentionally outside the lifecycle gate so invalidation can win
            // without waiting for a user-controlled routing boundary.
            m_BeforePendingPairRoutingTestHook?.Invoke();
            if (!IsAttemptAccepting(pendingPair.Attempt))
                return;

            if (pendingPair.Kind == PendingPairKind.UnityServiceRequest)
            {
                RosTopicState topicState = GetTopic(serviceTopic);
                if (topicState == null)
                {
                    Debug.LogError($"Unity service {serviceTopic} has not been implemented!");
                    return;
                }
                topicState.HandleUnityServiceRequest(contents, pendingPair.ServiceId, pendingPair.Attempt);
                return;
            }

            ServiceCall call = null;
            bool routed;
            lock (m_ConnectionLifecycleLock)
            {
                routed = IsCurrentLocked(pendingPair.Attempt)
                    && pendingPair.Attempt.Calls.TryGetValue(pendingPair.ServiceId, out call)
                    && ReferenceEquals(call.Attempt, pendingPair.Attempt)
                    && call.State == ServiceCallState.Pending
                    && string.Equals(call.Topic, serviceTopic, StringComparison.Ordinal);
                if (routed)
                    call.State = ServiceCallState.RawReady;
            }
            if (routed)
                call.RawResponse.TrySetResult(contents);
            else
                Debug.LogError($"Unable to route service response on \"{serviceTopic}\"! SrvID {pendingPair.ServiceId} does not exist.");
        }

        void InstallPendingPair(AttemptContext attempt, PendingPairKind kind, int serviceId)
        {
            // Parsing is complete, but the pair is not admitted until this fresh authority
            // check. No delegate or user callback is stored in the attempt parser state.
            m_BeforePendingPairInstallationTestHook?.Invoke();
            lock (m_ConnectionLifecycleLock)
            {
                if (!IsCurrentLocked(attempt))
                    return;
                attempt.PendingPair = new PendingPair(attempt, kind, serviceId);
            }
        }

        void ReceiveSysCommand(AttemptContext attempt, string topic, string json)
        {
            lock (m_ConnectionLifecycleLock)
            {
                if (!IsCurrentLocked(attempt)) return;
            }
            switch (topic)
            {
                case SysCommand.k_SysCommand_Handshake:
                    {
                        var handshakeCommand = JsonUtility.FromJson<SysCommand_Handshake>(json);
                        if (handshakeCommand.version == null)
                        {
                            Debug.LogError($"Corrupted or unreadable ROS-TCP-Endpoint version data! Expected: {k_Version}");
                        }
                        else if (!handshakeCommand.version.StartsWith(k_CompatibleVersionPrefix))
                        {
                            Debug.LogError($"Incompatible ROS-TCP-Endpoint version: {handshakeCommand.version}. Expected: {k_Version}");
                        }

                        var handshakeMetadata = JsonUtility.FromJson<SysCommand_Handshake_Metadata>(handshakeCommand.metadata);
#if ROS2
                        if (handshakeMetadata.protocol != "ROS2")
                        {
                            Debug.LogError($"Incompatible protocol: ROS-TCP-Endpoint is using {handshakeMetadata.protocol}, but Unity is in ROS2 mode. Switch it from the Robotics/Ros Settings menu.");
                        }
#else
                        if (handshakeMetadata.protocol != "ROS1")
                        {
                            Debug.LogError($"Incompatible protocol: ROS-TCP-Endpoint is using {handshakeMetadata.protocol}, but Unity is in ROS1 mode. Switch it from the Robotics/Ros Settings menu.");
                        }
#endif
                    }
                    break;
                case SysCommand.k_SysCommand_Log:
                    {
                        var logCommand = JsonUtility.FromJson<SysCommand_Log>(json);
                        Debug.Log(logCommand.text);
                    }
                    break;
                case SysCommand.k_SysCommand_Warning:
                    {
                        var logCommand = JsonUtility.FromJson<SysCommand_Log>(json);
                        Debug.LogWarning(logCommand.text);
                    }
                    break;
                case SysCommand.k_SysCommand_Error:
                    {
                        var logCommand = JsonUtility.FromJson<SysCommand_Log>(json);
                        Debug.LogError(logCommand.text);
                    }
                    break;
                case SysCommand.k_SysCommand_ServiceRequest:
                    {
                        var serviceCommand = JsonUtility.FromJson<SysCommand_Service>(json);
                        // The next incoming message is a request for a Unity service. Keep
                        // only parser data in the attempt; all routing remains outside G.
                        InstallPendingPair(attempt, PendingPairKind.UnityServiceRequest, serviceCommand.srv_id);
                    }
                    break;

                case SysCommand.k_SysCommand_ServiceResponse:
                    {
                        // the next incoming message will be a response from a ros service
                        var serviceCommand = JsonUtility.FromJson<SysCommand_Service>(json);
                        InstallPendingPair(attempt, PendingPairKind.RosServiceResponse, serviceCommand.srv_id);
                    }
                    break;

                case SysCommand.k_SysCommand_TopicList:
                    {
                        var topicsResponse = JsonUtility.FromJson<SysCommand_TopicsResponse>(json);
                        AttemptCallback<Dictionary<string, string>>[] topicsAndTypesCallbacks;
                        AttemptCallback<string[]>[] topicsCallbacks;
                        lock (m_ConnectionLifecycleLock)
                        {
                            if (!IsCurrentLocked(attempt)) return;
                            // Detach every callback kind as one response transaction. A callback
                            // registered while either batch is delivered belongs to a later
                            // response and cannot be swallowed by the second detach.
                            topicsAndTypesCallbacks = attempt.TypeCallbacks.ToArray();
                            topicsCallbacks = attempt.TopicCallbacks.ToArray();
                            attempt.TypeCallbacks.Clear();
                            attempt.TopicCallbacks.Clear();
                            attempt.TopicListOutstanding -= topicsAndTypesCallbacks.Length + topicsCallbacks.Length;
                            if (attempt.TopicListOutstanding < 0)
                                attempt.TopicListOutstanding = 0;
                        }

                        Dictionary<string, string> callbackParam = null;
                        if (topicsAndTypesCallbacks.Length > 0)
                        {
                            callbackParam = new Dictionary<string, string>();
                            for (int idx = 0; idx < topicsResponse.topics.Length; ++idx)
                                callbackParam[topicsResponse.topics[idx]] = topicsResponse.types[idx];
                            foreach (AttemptCallback<Dictionary<string, string>> registration in topicsAndTypesCallbacks)
                            {
                                if (!TryBeginCallback(attempt, registration))
                                    continue;
                                registration.Callback(callbackParam);
                            }
                        }
                        foreach (AttemptCallback<string[]> registration in topicsCallbacks)
                        {
                            if (!TryBeginCallback(attempt, registration))
                                continue;
                            registration.Callback(topicsResponse.topics);
                        }
                    }
                    break;
            }
        }

        internal bool IsAttemptAccepting(AttemptContext attempt)
        {
            lock (m_ConnectionLifecycleLock)
                return IsCurrentLocked(attempt);
        }

        internal bool HasAcceptingAttempt
        {
            get
            {
                lock (m_ConnectionLifecycleLock)
                    return IsLiveAttemptLocked(m_CurrentAttempt);
            }
        }

        internal bool TryGetPublicationAttempt(out AttemptContext attempt)
        {
            lock (m_ConnectionLifecycleLock)
            {
                if (IsCurrentLocked(m_CurrentAttempt))
                {
                    attempt = m_CurrentAttempt;
                    return true;
                }
                if (m_WorkerSession == null && m_StopPlan == null && m_Lifecycle == LifecycleState.StoppedAuthorized)
                {
                    attempt = new AttemptContext(++m_NextAttemptId, null);
                    m_CurrentAttempt = attempt;
                    return true;
                }
                attempt = null;
                return false;
            }
        }

        internal bool TryBeginPublication(TopicMessageSender sender, out AttemptContext attempt,
            out TopicMessageSender.PendingQueue queue)
        {
            lock (m_ConnectionLifecycleLock)
            {
                if (!IsCurrentLocked(m_CurrentAttempt)
                    && !(m_WorkerSession == null && m_StopPlan == null && m_Lifecycle == LifecycleState.StoppedAuthorized))
                {
                    attempt = null;
                    queue = null;
                    return false;
                }
                if (!IsCurrentLocked(m_CurrentAttempt))
                    m_CurrentAttempt = new AttemptContext(++m_NextAttemptId, null);
                attempt = m_CurrentAttempt;
            }
            queue = attempt.Outgoing.Channel(sender);
            return sender.TryBeginPreparation(queue);
        }

        internal void AbortPublicationPreparation(TopicMessageSender.PendingQueue queue)
        {
            if (queue != null)
                queue.Sender.EndPreparation(queue);
        }

        internal bool CommitPublication(TopicMessageSender sender, AttemptContext attempt,
            TopicMessageSender.PendingQueue queue, TopicMessageSender.OwnedPublication publication)
        {
            MessageUse displaced;
            bool admitted = CommitPublication(sender, attempt, queue, publication, out displaced, signalAttempt: true);
            displaced?.Release(true);
            return admitted;
        }

        internal bool CommitPublication(TopicMessageSender sender, AttemptContext attempt,
            TopicMessageSender.PendingQueue queue, TopicMessageSender.OwnedPublication publication,
            out MessageUse displacedUse, bool signalAttempt)
        {
            displacedUse = null;
            sender.EndPreparation(queue);
            if (!sender.TryStampPublication(publication))
                return false;

            OutgoingMessageQueue.Entry displaced = null;
            bool admitted;
            lock (m_ConnectionLifecycleLock)
            {
                admitted = IsCurrentLocked(attempt);
                if (admitted)
                    displaced = attempt.Outgoing.EnqueuePublication(attempt, sender, queue, publication,
                        manualClaimOnly: !signalAttempt);
            }
            if (displaced != null)
                displacedUse = sender.ReleasePublicationHandle(displaced.Publication);
            if (admitted && signalAttempt)
                SignalAttempt(attempt);
            return admitted;
        }

        void SignalAttempt(AttemptContext attempt)
        {
            attempt?.Session?.WakeSignal.Signal();
        }

        const int k_MaxRawCommandUnits = 256;

        bool TryBeginRawCommand(out AttemptContext attempt)
        {
            lock (m_ConnectionLifecycleLock)
            {
                if (!IsCurrentLocked(m_CurrentAttempt))
                {
                    if (m_WorkerSession != null || m_StopPlan != null || m_Lifecycle != LifecycleState.StoppedAuthorized)
                    {
                        attempt = null;
                        return false;
                    }
                    m_CurrentAttempt = new AttemptContext(++m_NextAttemptId, null);
                }
                attempt = m_CurrentAttempt;
                if (attempt.RawUnits >= k_MaxRawCommandUnits)
                {
                    attempt = null;
                    return false;
                }
                attempt.RawUnits++;
                return true;
            }
        }

        void AbortRawCommand(AttemptContext attempt)
        {
            if (attempt == null) return;
            lock (m_ConnectionLifecycleLock)
            {
                if (attempt.RawUnits > 0)
                    attempt.RawUnits--;
            }
        }

        bool TryEnqueueSender(OutgoingMessageSender sender)
        {
            AttemptContext attempt;
            lock (m_ConnectionLifecycleLock)
            {
                attempt = m_CurrentAttempt;
                if (!IsCurrentLocked(attempt))
                    return false;
                attempt.Outgoing.Enqueue(attempt, sender);
            }
            SignalAttempt(attempt);
            return true;
        }

        bool TryQueueMessage(TopicMessageSender sender, MessageUse use, AttemptContext expectedAttempt)
        {
            if (expectedAttempt == null)
                return false;
            TopicMessageSender.PendingQueue queue;
            lock (m_ConnectionLifecycleLock)
            {
                if (!IsCurrentLocked(expectedAttempt))
                    return false;
            }
            queue = expectedAttempt.Outgoing.Channel(sender);
            if (!sender.TryBeginPreparation(queue))
                return false;
            TopicMessageSender.OwnedPublication publication = null;
            try
            {
                publication = sender.PreparePublication(use);
                use = null;
                bool admitted = CommitPublication(sender, expectedAttempt, queue, publication);
                if (admitted)
                {
                    publication = null;
                    return true;
                }
                return false;
            }
            finally
            {
                if (publication != null)
                    sender.AbortPublication(publication, false);
                if (use != null)
                    use.Release(false);
            }
        }

        bool TryQueueServicePair(string command, object param, TopicMessageSender sender, Message message, AttemptContext expectedAttempt)
        {
            return TryQueueServicePairForUse(command, param, sender, sender.AcquireMessageUse(message), expectedAttempt);
        }

        bool TryQueueServicePairForUse(string command, object param, TopicMessageSender sender, MessageUse use, AttemptContext expectedAttempt)
        {
            ExactMessageSender pair = null;
            bool admitted = false;
            try
            {
                var serializer = new MessageSerializer();
                PopulateSysCommand(serializer, command, param);
                pair = sender.CreateExactSenderForUse(use, serializer.GetBytesSequence());
                if (pair == null)
                    return false;
                lock (m_ConnectionLifecycleLock)
                {
                    if (!IsLiveAttemptLocked(expectedAttempt))
                        return false;
                    expectedAttempt.Outgoing.EnqueueExact(expectedAttempt, pair);
                    admitted = true;
                }
                SignalAttempt(expectedAttempt);
                return true;
            }
            finally
            {
                // Metadata admission ends before releasing any capsule/capacity owner.
                if (!admitted)
                {
                    if (pair != null) pair.AbortUnqueued();
                    else use.Release(false);
                }
            }
        }

        internal bool TryGetAcceptingAttempt(out AttemptContext attempt)
        {
            lock (m_ConnectionLifecycleLock)
            {
                attempt = IsLiveAttemptLocked(m_CurrentAttempt) ? m_CurrentAttempt : null;
                return attempt != null;
            }
        }

        internal bool TryClaimPublication(TopicMessageSender sender, out OutgoingMessageQueue.Entry entry,
            out bool queueFullWarning)
        {
            lock (m_ConnectionLifecycleLock)
            {
                if (!IsLiveAttemptLocked(m_CurrentAttempt))
                {
                    entry = null;
                    queueFullWarning = false;
                    return false;
                }
                return m_CurrentAttempt.Outgoing.TryDequeueChannel(sender, out entry, out queueFullWarning);
            }
        }

        internal MessageUse TryCommitSenderCache(TopicMessageSender sender, TopicMessageSender.OwnedPublication publication, AttemptContext attempt)
        {
            lock (m_ConnectionLifecycleLock)
            {
                if (!IsCurrentLocked(attempt))
                    return null;
            }
            m_BeforeSenderCacheCommitTestHook?.Invoke();
            lock (m_ConnectionLifecycleLock)
            {
                if (!IsCurrentLocked(attempt))
                    return null;
                return sender.CommitSuccessfulPublication(publication);
            }
        }

        bool TryQueueLatch(TopicMessageSender sender, AttemptContext expectedAttempt)
        {
            if (!TryQueueLatchCore(sender, expectedAttempt))
                return false;
            SignalAttempt(expectedAttempt);
            return true;
        }

        bool TryQueueLatchCore(TopicMessageSender sender, AttemptContext expectedAttempt)
        {
            lock (m_ConnectionLifecycleLock)
            {
                if (!IsLiveAttemptLocked(expectedAttempt))
                    return false;
                TopicMessageSender.PendingQueue queue = expectedAttempt.Outgoing.Channel(sender);
                if (!sender.PrepareLatchForOwner(expectedAttempt, queue))
                    return false;
                if (!expectedAttempt.Outgoing.AttachPreparedPublication(queue))
                    throw new InvalidOperationException("A prepared latch publication was not present in its epoch outbox.");
            }
            return true;
        }

        static void SendKeepalive(NetworkStream stream)
        {
            // 8 zeroes = a ros message with topic "" and no message data.
            stream.Write(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }, 0, 8);
        }

        void ReleaseRawUnit(OutgoingMessageQueue.Entry entry)
        {
            if (entry == null || entry.Attempt == null) return;
            lock (m_ConnectionLifecycleLock)
            {
                if (!entry.RawUnitCounted)
                    return;
                entry.RawUnitCounted = false;
                if (entry.Attempt.RawUnits > 0)
                    entry.Attempt.RawUnits--;
            }
        }

        void ClearOutgoingEntry(OutgoingMessageQueue.Entry outgoingMessage)
        {
            switch (outgoingMessage.Kind)
            {
                case OutgoingMessageQueue.EntryKind.Publication:
                    ((TopicMessageSender)outgoingMessage.Sender).AbortPublication(outgoingMessage.Publication, true);
                    break;
                case OutgoingMessageQueue.EntryKind.Exact:
                    outgoingMessage.Exact.ClearAllQueuedData();
                    break;
                case OutgoingMessageQueue.EntryKind.Raw:
                    ReleaseRawUnit(outgoingMessage);
                    outgoingMessage.Sender.ClearAllQueuedData();
                    break;
                case OutgoingMessageQueue.EntryKind.Registration:
                    CancelRegistrationUnit(outgoingMessage.Registration);
                    break;
                default:
                    outgoingMessage.Sender.ClearAllQueuedData();
                    break;
            }
        }

        async Task ConnectTcpClient(SocketOwner owner, string address, int port, CancellationToken token)
        {
            using (token.Register(() => owner.CloseOnce(m_SocketCloseTestHook)))
            {
                try
                {
                    m_BeforeConnectAttemptTestHook?.Invoke();
                    token.ThrowIfCancellationRequested();
                    await owner.Client.ConnectAsync(address, port);
                }
                catch (Exception) when (token.IsCancellationRequested)
                {
                    throw new OperationCanceledException(token);
                }
            }
            token.ThrowIfCancellationRequested();
        }

        bool IsSessionRunning(WorkerSession session)
        {
            lock (m_ConnectionLifecycleLock)
                return ReferenceEquals(m_WorkerSession, session) && m_Lifecycle == LifecycleState.Running;
        }

        async Task ConnectionThread(WorkerSession session, AttemptContext firstAttempt)
        {
            string rosIPAddress = session.Address;
            int rosPort = session.Port;
            float networkTimeoutSeconds = session.Timeout;
            float keepaliveTime = session.Keepalive;
            int sleepMilliseconds = session.SleepMilliseconds;
            CancellationToken token = session.Cancellation.Token;

            //Debug.Log("ConnectionThread begins");
            int nextReaderIdx = 101;
            int nextReconnectionDelay = 1000;
            MessageSerializer messageSerializer = new MessageSerializer();

            AttemptContext reservedAttempt = firstAttempt;
            while (IsSessionRunning(session) && !token.IsCancellationRequested)
            {
                TcpClient client = null;
                SocketOwner transport = null;
                CancellationTokenSource readerCancellation = null;
                Task readerTask = null;
                bool delayBeforeReconnect = false;
                AttemptContext attempt = reservedAttempt;
                reservedAttempt = null;
                if (attempt == null)
                {
                    lock (m_ConnectionLifecycleLock)
                    {
                        if (!ReferenceEquals(m_WorkerSession, session) || m_Lifecycle != LifecycleState.Running) break;
                        attempt = new AttemptContext(++m_NextAttemptId, session);
                        session.Attempt = attempt;
                        m_CurrentAttempt = attempt;
                    }
                }
                OutgoingMessageQueue outgoingQueue = attempt.Outgoing;
                ConcurrentQueue<IncomingMessage> incomingQueue = attempt.Incoming;

                try
                {
                    m_HasConnectionError = true; // until we actually see a reply back, assume there's a problem

                    client = new TcpClient();
                    transport = new SocketOwner(client);
                    m_BeforeClientPublicationTestHook?.Invoke();
                    lock (m_ConnectionLifecycleLock)
                    {
                        if (!ReferenceEquals(m_WorkerSession, session) || m_Lifecycle != LifecycleState.Running || !IsCurrentLocked(attempt))
                            throw new OperationCanceledException(token);
                        attempt.Transport = transport;
                    }
                    token.ThrowIfCancellationRequested();
                    await ConnectTcpClient(transport, rosIPAddress, rosPort, token);

                    NetworkStream networkStream = client.GetStream();
                    networkStream.ReadTimeout = (int)(networkTimeoutSeconds * 1000);
                    networkStream.WriteTimeout = Math.Max(1, (int)(networkTimeoutSeconds * 1000));
                    lock (m_ConnectionLifecycleLock)
                    {
                        if (!IsCurrentLocked(attempt)) throw new OperationCanceledException(token);
                        attempt.Stream = networkStream;
                        attempt.State = AttemptState.Establishing;
                    }

                    SendKeepalive(networkStream);
                    token.ThrowIfCancellationRequested();
                    m_BeforeStartupRegistrationsTestHook?.Invoke();
                    OnConnectionStartedCallback(networkStream, attempt);
                    RefreshTopicsList();

                    // Only the worker cancels/disposes the reader CTS. Session stop closes
                    // its socket; no linked callback can race reader CTS disposal.
                    readerCancellation = new CancellationTokenSource();
                    lock (m_ConnectionLifecycleLock)
                    {
                        if (!IsCurrentLocked(attempt)) throw new OperationCanceledException(token);
                        attempt.ReaderCancellation = readerCancellation;
                    }
                    readerTask = Task.Run(async () =>
                    {
                        try { await ReaderThread(nextReaderIdx, attempt, networkStream, incomingQueue, sleepMilliseconds, readerCancellation.Token); }
                        finally
                        {
                            lock (m_ConnectionLifecycleLock) RevokeAttemptLocked(attempt);
                            m_ReaderTerminatingTestHook?.Invoke();
                        }
                    });
                    nextReaderIdx++;
                    lock (m_ConnectionLifecycleLock)
                    {
                        attempt.Reader = readerTask;
                        if (IsCurrentLocked(attempt)) attempt.State = AttemptState.Ready;
                    }

                    if (m_HasOutputConnectionError)
                    {
                        Debug.Log($"ROS Connection to {rosIPAddress}:{rosPort} succeeded!");
                        m_HasOutputConnectionError = false;
                    }

                    // connected, now just watch our queue for outgoing messages to send (or else send a keepalive message occasionally)
                    float waitingSinceRealTime = s_RealTimeSinceStartup;
                    while (true)
                    {
                        if (!IsSessionRunning(session))
                            throw new OperationCanceledException(token);
                        if (readerTask.IsCompleted)
                            await readerTask;
                        if (!IsAttemptAccepting(attempt))
                            throw new IOException("The ROS reader closed this connection attempt.");
                        if (outgoingQueue.Count == 0)
                        {
                            bool wakeWasSignaled = session.WakeSignal.Wait(sleepMilliseconds);
                            token.ThrowIfCancellationRequested();
                            if (!wakeWasSignaled && s_RealTimeSinceStartup > waitingSinceRealTime + keepaliveTime)
                            {
                                SendKeepalive(networkStream);
                                waitingSinceRealTime = s_RealTimeSinceStartup;
                            }
                        }

                        int messagesInBatch = 0;
                        while (messagesInBatch++ < 64 && outgoingQueue.TryDequeue(out OutgoingMessageQueue.Entry outgoingMessage))
                        {
                            if (outgoingMessage.Kind == OutgoingMessageQueue.EntryKind.Raw)
                                ReleaseRawUnit(outgoingMessage);
                            if (!ReferenceEquals(outgoingMessage.Attempt, attempt))
                            {
                                ClearOutgoingEntry(outgoingMessage);
                                continue;
                            }
                            if (!IsAttemptAccepting(attempt))
                            {
                                ClearOutgoingEntry(outgoingMessage);
                                throw new IOException("The ROS connection attempt was revoked before queued work could be claimed.");
                            }
                            OutgoingMessageSender.SendToState sendToState;
                            switch (outgoingMessage.Kind)
                            {
                                case OutgoingMessageQueue.EntryKind.Publication:
                                    sendToState = ((TopicMessageSender)outgoingMessage.Sender).SendPublication(
                                        outgoingMessage.Publication, networkStream, attempt);
                                    break;
                                case OutgoingMessageQueue.EntryKind.Exact:
                                    sendToState = outgoingMessage.Exact.SendInternal(messageSerializer, networkStream);
                                    break;
                                case OutgoingMessageQueue.EntryKind.Raw:
                                    sendToState = outgoingMessage.Sender.SendInternal(messageSerializer, networkStream);
                                    break;
                                case OutgoingMessageQueue.EntryKind.Registration:
                                    if (!TryBeginRegistrationWrite(outgoingMessage.Registration, attempt))
                                    {
                                        ClearOutgoingEntry(outgoingMessage);
                                        continue;
                                    }
                                    bool registrationSucceeded = false;
                                    try
                                    {
                                        // The registration writer is the only path allowed to
                                        // perform this socket write. The hook remains a lifecycle
                                        // test seam, but no registration is sent directly by a
                                        // public/configuration call.
                                        m_BeforeImmediateCommandWriteTestHook?.Invoke();
                                        if (!IsAttemptAccepting(attempt))
                                            throw new IOException("The registration attempt is no longer active.");
                                        sendToState = outgoingMessage.Registration.Sender.SendInternal(messageSerializer, networkStream);
                                        registrationSucceeded = sendToState == OutgoingMessageSender.SendToState.Normal;
                                        m_AfterImmediateCommandWriteTestHook?.Invoke();
                                    }
                                    finally
                                    {
                                        CompleteRegistrationWrite(outgoingMessage.Registration, attempt, registrationSucceeded);
                                    }
                                    break;
                                default:
                                    // Compatibility-only lifecycle seam; connector
                                    // publication paths never enqueue this kind.
                                    sendToState = outgoingMessage.Sender.SendInternal(messageSerializer, networkStream);
                                    break;
                            }
                            switch (sendToState)
                            {
                                case OutgoingMessageSender.SendToState.Normal:
                                    // This is normal operation.
                                    break;
                                case OutgoingMessageSender.SendToState.QueueFullWarning:
                                    // Legacy senders may still report this state.
                                    Debug.LogWarning($"Queue full! Messages are getting dropped! " +
                                                     "Try check your connection speed is fast enough to handle the traffic.");
                                    break;
                                case OutgoingMessageSender.SendToState.NoMessageToSendError:
                                    Debug.LogError(
                                        "Logic Error! An outbound unit was claimed but had no bytes to send.");
                                    break;
                            }

                            if (!IsAttemptAccepting(attempt))
                                throw new IOException("The ROS connection attempt was revoked after the queued work was claimed.");
                            token.ThrowIfCancellationRequested();
                            waitingSinceRealTime = s_RealTimeSinceStartup;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception) when (token.IsCancellationRequested || !IsSessionRunning(session))
                {
                }
                catch (Exception e)
                {
                    lock (m_ConnectionLifecycleLock) RevokeAttemptLocked(attempt);
                    m_HasConnectionError = true;
                    if (!m_HasOutputConnectionError)
                    {
                        Debug.LogError($"ROS Connection to {rosIPAddress}:{rosPort} failed - " + e);
                        m_HasOutputConnectionError = true;
                    }
                    delayBeforeReconnect = true;
                }
                finally
                {
                    // Failure loses admission before cancellation, close, reader join or logging.
                    lock (m_ConnectionLifecycleLock) RevokeAttemptLocked(attempt);
                    Exception teardownFailure = null;
                    m_WorkerBeforeCloseTestHook?.Invoke();
                    try { transport?.CloseOnce(m_SocketCloseTestHook); }
                    catch (Exception exception) { teardownFailure = exception; }
                    m_WorkerSocketCloseReturnedTestHook?.Invoke();
                    try { readerCancellation?.Cancel(); }
                    catch (Exception exception) { teardownFailure = teardownFailure ?? exception; }

                    if (readerTask != null)
                    {
                        try
                        {
                            await readerTask;
                        }
                        catch (OperationCanceledException)
                        {
                        }
                        catch (Exception e)
                        {
                            if (!token.IsCancellationRequested) teardownFailure = teardownFailure ?? e;
                        }
                    }
                    try { readerCancellation?.Dispose(); }
                    finally
                    {
                        try { RetireAttempt(attempt); }
                        finally
                        {
                            // A losing CloseOnce caller must wait for the actual close owner,
                            // outside G, before the worker can release transport ownership.
                            if (transport != null) await transport.Closed.Task;
                            lock (m_ConnectionLifecycleLock)
                            {
                                attempt.Transport = null;
                                attempt.Stream = null;
                                attempt.ReaderCancellation = null;
                                attempt.State = AttemptState.Closed;
                            }
                        }
                    }
                    if (teardownFailure != null) Debug.Log("Reader/resource exception while stopping connection: " + teardownFailure);
                }
                if (delayBeforeReconnect && IsSessionRunning(session))
                    await Task.Delay(nextReconnectionDelay, token);
                await Task.Yield();
            }
        }

        async Task ReaderThread(int readerIdx, AttemptContext attempt, NetworkStream networkStream, ConcurrentQueue<IncomingMessage> queue, int sleepMilliseconds, CancellationToken token)
        {
            // First message should be the handshake
            Tuple<string, byte[]> handshakeContent = await ReadMessageContents(networkStream, sleepMilliseconds, token);
            if (handshakeContent.Item1 == SysCommand.k_SysCommand_Handshake)
            {
                m_HasConnectionError = false;
                queue.Enqueue(new IncomingMessage(attempt, handshakeContent.Item1, handshakeContent.Item2));
            }
            else
            {
                Debug.LogError($"Invalid ROS-TCP-Endpoint version detected: 0.6.0 or older. Expected: {k_Version}.");
            }

            while (!token.IsCancellationRequested)
            {
                try
                {
                    Tuple<string, byte[]> content = await ReadMessageContents(networkStream, sleepMilliseconds, token);
                    m_HasConnectionError = false;

                    if (content.Item1 != "") // ignore keepalive messages
                        queue.Enqueue(new IncomingMessage(attempt, content.Item1, content.Item2));
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception) when (token.IsCancellationRequested || !IsAttemptAccepting(attempt))
                {
                }
                catch (Exception e)
                {
                    m_HasConnectionError = true;
                    Debug.Log("Reader " + readerIdx + " exception! " + e);
                    throw;
                }
            }
        }

        // Topic names are small protocol identifiers. Messages allow large image frames while
        // retaining a hard bound against corrupt or hostile length prefixes.
        internal const int k_MaxTopicNameBytes = 4096;
        internal const int k_MaxMessageBytes = 64 * 1024 * 1024;

        static async Task ReadToByteArray(Stream stream, byte[] array, int length, int sleepMilliseconds, CancellationToken token)
        {
            int read = 0;
            while (read < length && stream.CanRead)
            {
                token.ThrowIfCancellationRequested();
                int bytesRead = await stream.ReadAsync(array, read, length - read, token);
                if (bytesRead == 0)
                    throw new EndOfStreamException("The ROS connection closed before the complete frame was received.");
                read += bytesRead;
            }

            if (read < length)
                throw new EndOfStreamException("The ROS connection closed before the complete frame was received.");
        }

        static async Task<Tuple<string, byte[]>> ReadMessageContents(Stream stream, int sleepMilliseconds, CancellationToken token)
        {
            byte[] fourBytes = new byte[4];
            // Get first bytes to determine length of topic name
            await ReadToByteArray(stream, fourBytes, 4, sleepMilliseconds, token);
            int topicLength = BitConverter.ToInt32(fourBytes, 0);
            if (topicLength < 0 || topicLength > k_MaxTopicNameBytes)
                throw new InvalidDataException($"ROS topic length {topicLength} is outside the allowed range 0-{k_MaxTopicNameBytes} bytes.");

            byte[] topicBytes = new byte[topicLength];
            await ReadToByteArray(stream, topicBytes, topicLength, sleepMilliseconds, token);
            string topicName = Encoding.ASCII.GetString(topicBytes, 0, topicLength);

            await ReadToByteArray(stream, fourBytes, 4, sleepMilliseconds, token);
            int fullMessageSize = BitConverter.ToInt32(fourBytes, 0);
            if (fullMessageSize < 0 || fullMessageSize > k_MaxMessageBytes)
                throw new InvalidDataException($"ROS message length {fullMessageSize} is outside the allowed range 0-{k_MaxMessageBytes} bytes.");

            byte[] readBuffer = new byte[fullMessageSize];
            await ReadToByteArray(stream, readBuffer, fullMessageSize, sleepMilliseconds, token);

            return Tuple.Create(topicName, readBuffer);
        }

        void OnApplicationQuit()
        {
            Disconnect();
        }

        void OnDestroy()
        {
            try { Disconnect(); }
            finally
            {
                ConnectorDomainLifetime.Unregister(this);
                // Disconnect preserves compatible latch handles for reconnect. Destruction
                // is terminal: release sender/cache ownership, never a claimed writer's use.
                // Snapshot accessors release their gates before registry/pool retirement.
                foreach (RosTopicState topic in AllTopics)
                {
                    topic.MessageSender?.Dispose();
                    topic.ServiceResponseTopic?.MessageSender?.Dispose();
                }
                HudPanel.UnregisterHeader(DrawHeaderGUI);
                if (ReferenceEquals(_instance, this))
                    _instance = null;
            }
        }

        static void PopulateSysCommand(MessageSerializer messageSerializer, string command, object param)
        {
            messageSerializer.Clear();
            // syscommands are sent as:
            // 4 byte command length, followed by that many bytes of the command
            // (all command names start with __ to distinguish them from ros topics)
            messageSerializer.Write(command);
            // 4-byte json length, followed by a json string of that length
            string json = JsonUtility.ToJson(param);
            messageSerializer.WriteUnaligned(json);
        }

        public void QueueSysCommand(string command, object param)
        {
            // Capture the epoch and a bounded preparation unit before invoking JSON
            // serialization. A top-level Message is borrowed only for this snapshot;
            // arbitrary object graphs remain the caller's responsibility.
            if (!TryBeginRawCommand(out AttemptContext attempt))
                throw new IOException("An outbound ROS command requires an authorized connection epoch with available capacity.");
            MessageUse borrow = param is Message message ? MessageUseRegistry.Acquire(message, null) : null;
            bool admitted = false;
            try
            {
                var messageSerializer = new MessageSerializer();
                PopulateSysCommand(messageSerializer, command, param);
                var sender = new SysCommandSender(messageSerializer.GetBytesSequence());
                lock (m_ConnectionLifecycleLock)
                {
                    if (!IsCurrentLocked(attempt))
                        throw new IOException("The outbound ROS command epoch is no longer active.");
                    attempt.Outgoing.EnqueueRaw(attempt, sender, sender.FrozenBytes);
                    admitted = true;
                }
                SignalAttempt(attempt);
            }
            finally
            {
                if (!admitted)
                    AbortRawCommand(attempt);
                borrow?.Release(false);
            }
        }

        [Obsolete("Use Publish instead of Send", false)]
        public void Send(string rosTopicName, Message message)
        {
            Publish(rosTopicName, message);
        }

        public void Publish(string rosTopicName, Message message)
        {
            if (rosTopicName.StartsWith("__"))
            {
                QueueSysCommand(rosTopicName, message);
            }
            else
            {
                RosTopicState rosTopic = GetTopic(rosTopicName);
                if (rosTopic == null || !rosTopic.IsPublisher)
                {
                    MessageUse borrow = MessageUseRegistry.Acquire(message, null);
                    try { throw new Exception($"No registered publisher on topic {rosTopicName} for type {message.RosMessageName}!"); }
                    finally { borrow.Release(false); }
                }

                rosTopic.Publish(message);
            }
        }

        void InitializeHUD()
        {
            if (!Application.isPlaying || (!m_ShowHUD && m_HudPanel == null))
                return;

            if (m_HudPanel == null)
            {
                m_HudPanel = gameObject.AddComponent<HudPanel>();
            }

            m_HudPanel.isEnabled = m_ShowHUD;
        }

        void DrawHeaderGUI()
        {
            GUIStyle labelStyle = new GUIStyle
            {
                alignment = TextAnchor.MiddleLeft,
                normal = { textColor = Color.white },
                fontStyle = FontStyle.Bold,
                fixedWidth = 250
            };

            GUIStyle contentStyle = new GUIStyle
            {
                alignment = TextAnchor.MiddleLeft,
                padding = new RectOffset(10, 0, 0, 5),
                normal = { textColor = Color.white },
            };


            // ROS IP Setup
            GUILayout.BeginHorizontal(GUILayout.Width(300));
            DrawConnectionArrows(
                true,
                0,
                0,
                Time.realtimeSinceStartup - LastMessageReceivedRealtime,
                Time.realtimeSinceStartup - LastMessageSentRealtime,
                HasConnectionThread,
                HasConnectionThread,
                HasConnectionError
            );

#if ROS2
            string protocolName = "ROS2";
#else
            string protocolName = "ROS";
#endif

            GUILayout.Space(30);
            GUILayout.Label($"{protocolName} IP: ", labelStyle, GUILayout.Width(100));

            if (!HasConnectionThread)
            {
                GUILayout.Label(m_ConnectionConfig != null
                    ? $"{RosIPAddress}:{RosPort}"
                    : "Missing ROS Connection Config", contentStyle);

                GUILayout.EndHorizontal();
                GUILayout.Label("(Not connected)");
                if (m_ConnectionConfig != null && m_ConnectionConfig.IsValid && GUILayout.Button("Connect"))
                    Connect();
            }
            else
            {
                GUILayout.Label($"{RosIPAddress}:{RosPort}", contentStyle);

                if (HasConnectionError)
                {
                    if (GUI.Button(new Rect(250, 2, 50, 22), "Set IP"))
                        Disconnect();
                }

                GUILayout.EndHorizontal();
            }
        }

        static GUIStyle s_ConnectionArrowStyle;

        public static void DrawConnectionArrows(bool withBar, float x, float y, float receivedTime, float sentTime, bool isPublisher, bool isSubscriber, bool hasError)
        {
            if (s_ConnectionArrowStyle == null)
            {
                s_ConnectionArrowStyle = new GUIStyle
                {
                    alignment = TextAnchor.MiddleLeft,
                    normal = { textColor = Color.white },
                    fontSize = 22,
                    fontStyle = FontStyle.Bold,
                    fixedWidth = 250
                };
            }

            var baseColor = GUI.color;
            GUI.color = Color.white;
            if (withBar)
                GUI.Label(new Rect(x + 4, y + 5, 25, 15), "I", s_ConnectionArrowStyle);
            GUI.color = GetConnectionColor(receivedTime, isSubscriber, hasError);
            GUI.Label(new Rect(x + 8, y + 6, 25, 15), "\u2190", s_ConnectionArrowStyle);
            GUI.color = GetConnectionColor(sentTime, isPublisher, hasError);
            GUI.Label(new Rect(x + 8, y + 0, 25, 15), "\u2192", s_ConnectionArrowStyle);
            GUI.color = baseColor;
        }

        public static Color GetConnectionColor(float elapsedTime, bool hasConnection, bool hasError)
        {
            var bright = new Color(1, 1, 0.5f);
            var mid = new Color(0, 1, 1);
            var dark = new Color(0, 0.5f, 1);
            const float brightDuration = 0.03f;
            const float fadeToDarkDuration = 1.0f;

            if (!hasConnection)
                return Color.gray;
            if (hasError)
                return Color.red;

            if (elapsedTime <= brightDuration)
                return bright;
            return Color.Lerp(mid, dark, elapsedTime / fadeToDarkDuration);
        }

        public static bool IPFormatIsCorrect(string ipAddress)
        {
            if (String.IsNullOrWhiteSpace(ipAddress))
                return false;

            foreach (char character in ipAddress)
                if (Char.IsWhiteSpace(character) || Char.IsControl(character))
                    return false;

            IPAddress parsedAddress;
            if (IPAddress.TryParse(ipAddress, out parsedAddress))
                return true;

            return Uri.CheckHostName(ipAddress) == UriHostNameType.Dns;
        }
    }
}
