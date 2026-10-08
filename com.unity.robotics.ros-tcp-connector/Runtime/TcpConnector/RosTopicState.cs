using System;
using System.Collections;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Linq;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector
{
    public class RosTopicState
    {
        readonly object m_StateLock = new object();
        string m_Topic;
        public string Topic => m_Topic;
        bool m_IsDiscoveryPlaceholder;
        internal bool IsDiscoveryPlaceholder
        {
            get
            {
                lock (m_StateLock)
                    return m_IsDiscoveryPlaceholder;
            }
        }
        internal bool HasActiveConfiguration
        {
            get
            {
                lock (m_StateLock)
                    return m_IsPublisher || m_IsRosService || m_ServiceImplementation != null
                        || m_ServiceImplementationAsync != null || m_SubscriberCallbacks.Count > 0;
            }
        }

        MessageSubtopic m_Subtopic;
        public MessageSubtopic Subtopic => m_Subtopic;

        string m_RosMessageName;
        long m_SchemaRevision;
        public string RosMessageName { get { lock (m_StateLock) return m_RosMessageName; } }

        TopicMessageSender m_MessageSender;
        public TopicMessageSender MessageSender { get { lock (m_StateLock) return m_MessageSender; } }
        public bool IsPublisher { get { lock (m_StateLock) return m_IsPublisher; } }
        bool m_IsPublisher;
        public bool IsPublisherLatched { get { lock (m_StateLock) return m_IsPublisherLatched; } }
        bool m_IsPublisherLatched;

        internal bool TryGetLatchedPublisher(out TopicMessageSender sender)
        {
            lock (m_StateLock)
            {
                sender = m_IsPublisherLatched ? m_MessageSender : null;
                return sender != null;
            }
        }

        ROSConnection m_Connection;
        public ROSConnection Connection => m_Connection;
        ROSConnection.InternalAPI m_ConnectionInternal;
        Func<MessageDeserializer, Message> m_Deserializer;

        Func<Message, Message> m_ServiceImplementation;
        Func<Message, Task<Message>> m_ServiceImplementationAsync;
        // Deterministic authority seams for the request path. They are invoked only
        // after user-code boundaries and outside the topic/lifecycle ownership gates.
        Action m_AfterUnityServiceRequestDeserializationTestHook;
        Action m_AfterUnityServiceRequestImplementationTestHook;
        int m_PublisherQueueSize;
        string m_UnityServiceResponseMessageName;
        string m_RosServiceResponseMessageName;

        RosTopicState m_ServiceResponseTopic;
        public RosTopicState ServiceResponseTopic { get { lock (m_StateLock) return m_ServiceResponseTopic; } }

        internal ServiceResponseSnapshot CaptureServiceResponseSnapshot()
        {
            RosTopicState responseTopic;
            lock (m_StateLock)
                responseTopic = m_ServiceResponseTopic;
            return responseTopic?.CaptureResponseSnapshot();
        }

        ServiceResponseSnapshot CaptureResponseSnapshot()
        {
            string messageName;
            long schemaRevision;
            Func<MessageDeserializer, Message> deserializer;
            ROSConnection.CallbackRegistration<Message>[] callbacks;
            lock (m_StateLock)
            {
                messageName = m_RosMessageName;
                schemaRevision = m_SchemaRevision;
                deserializer = m_Deserializer;
                callbacks = m_SubscriberCallbacks.ToArray();
            }
            if (deserializer == null)
                deserializer = MessageRegistry.GetDeserializeFunction(messageName, m_Subtopic);
            return new ServiceResponseSnapshot(this, messageName, schemaRevision, deserializer, callbacks);
        }

        bool m_IsRosService;
        int m_PendingServiceCalls;
        int m_ServiceDecoderTickets;
        public bool IsRosService { get { lock (m_StateLock) return m_IsRosService; } }

        internal enum ExistingConfigurationKind { Subscriber, Publisher, UnityService, RosService }

        // A configuration plan contains every object that must be visible together at
        // the connection commit gate. It is prepared without either ownership gate;
        // only the field transfer is performed while the connection catalog is locked.
        internal sealed class ExistingTopicConfiguration
        {
            internal readonly ExistingConfigurationKind Kind;
            internal readonly string ExpectedMessageName;
            internal readonly string MessageName;
            internal readonly int QueueSize;
            internal readonly bool Latch;
            internal readonly string ResponseMessageName;
            internal readonly ROSConnection.CallbackRegistration<Message> Subscriber;
            internal readonly Func<Message, Message> Implementation;
            internal readonly Func<Message, Task<Message>> ImplementationAsync;
            internal readonly TopicMessageSender Sender;
            internal readonly RosTopicState PreparedResponseTopic;

            internal ExistingTopicConfiguration(ExistingConfigurationKind kind, string expectedMessageName,
                string messageName, int queueSize, bool latch, string responseMessageName,
                ROSConnection.CallbackRegistration<Message> subscriber,
                Func<Message, Message> implementation,
                Func<Message, Task<Message>> implementationAsync, TopicMessageSender sender,
                RosTopicState preparedResponseTopic)
            {
                Kind = kind;
                ExpectedMessageName = expectedMessageName;
                MessageName = messageName;
                QueueSize = queueSize;
                Latch = latch;
                ResponseMessageName = responseMessageName;
                Subscriber = subscriber;
                Implementation = implementation;
                ImplementationAsync = implementationAsync;
                Sender = sender;
                PreparedResponseTopic = preparedResponseTopic;
            }
        }
        internal sealed class ServiceResponseSnapshot
        {
            internal readonly RosTopicState Topic;
            internal readonly string MessageName;
            internal readonly long SchemaRevision;
            internal readonly Func<MessageDeserializer, Message> Deserializer;
            internal readonly ROSConnection.CallbackRegistration<Message>[] Callbacks;

            internal ServiceResponseSnapshot(RosTopicState topic, string messageName, long schemaRevision,
                Func<MessageDeserializer, Message> deserializer,
                ROSConnection.CallbackRegistration<Message>[] callbacks)
            {
                Topic = topic;
                MessageName = messageName;
                SchemaRevision = schemaRevision;
                Deserializer = deserializer;
                Callbacks = callbacks;
            }
        }

        public bool IsUnityService { get { lock (m_StateLock) return m_ServiceImplementation != null || m_ServiceImplementationAsync != null; } }
        public bool IsService { get { lock (m_StateLock) return m_ServiceResponseTopic != null || m_Subtopic == MessageSubtopic.Response; } }

        List<ROSConnection.CallbackRegistration<Message>> m_SubscriberCallbacks =
            new List<ROSConnection.CallbackRegistration<Message>>();
        public bool HasSubscriberCallback
        {
            get
            {
                lock (m_StateLock)
                    return m_SubscriberCallbacks.Count > 0;
            }
        }
        public bool SentSubscriberRegistration => m_Connection.RegistrationSent(this, false);
        public bool SentPublisherRegistration => m_Connection.RegistrationSent(this, true);

        float m_LastMessageReceivedRealtime;
        float m_LastMessageSentRealtime;
        public float LastMessageReceivedRealtime => m_LastMessageReceivedRealtime;
        public float LastMessageSentRealtime => m_LastMessageSentRealtime;

        internal RosTopicState(string topic, string rosMessageName, ROSConnection connection, ROSConnection.InternalAPI connectionInternal, bool isService, MessageSubtopic subtopic = MessageSubtopic.Default, bool discoveryPlaceholder = false)
        {
            m_Topic = topic;
            m_IsDiscoveryPlaceholder = discoveryPlaceholder;
            m_Subtopic = subtopic;
            m_RosMessageName = rosMessageName;
            m_SchemaRevision = rosMessageName == null ? 0 : 1;
            m_Connection = connection;
            m_ConnectionInternal = connectionInternal;
            if (isService && subtopic == MessageSubtopic.Default)
            {
                m_ServiceResponseTopic = new RosTopicState(topic, rosMessageName, m_Connection, m_ConnectionInternal, isService, MessageSubtopic.Response);
            }
        }

        internal void ChangeRosMessageName(string rosMessageName)
        {
            if (!TryChangeRosMessageNameWithoutWarning(rosMessageName,
                out string previous, out MessageUse retiredCache))
                return;
            retiredCache?.Release(true);
            if (previous != null)
                Debug.LogWarning($"Inconsistent declaration of topic '{Topic}': was '{previous}', switching to '{rosMessageName}'.");
        }

        internal bool TryChangeRosMessageNameWithoutWarning(string rosMessageName,
            out string previous, out MessageUse retiredCache)
        {
            lock (m_StateLock)
            {
                previous = m_RosMessageName;
                if (string.Equals(previous, rosMessageName, StringComparison.Ordinal))
                {
                    retiredCache = null;
                    return false;
                }
                m_RosMessageName = rosMessageName;
                m_SchemaRevision++;
                m_Deserializer = null;
                retiredCache = m_MessageSender?.InvalidateCache();
                return true;
            }
        }

        internal bool TryUpdateDiscoveryPlaceholder(string rosMessageName,
            out string previous, out MessageUse retiredCache)
        {
            lock (m_StateLock)
            {
                previous = m_RosMessageName;
                if (!m_IsDiscoveryPlaceholder || m_IsPublisher || m_IsRosService
                    || m_ServiceImplementation != null || m_ServiceImplementationAsync != null
                    || m_SubscriberCallbacks.Count > 0)
                {
                    retiredCache = null;
                    return false;
                }
                if (string.Equals(previous, rosMessageName, StringComparison.Ordinal))
                {
                    retiredCache = null;
                    return false;
                }
                m_RosMessageName = rosMessageName;
                m_SchemaRevision++;
                m_Deserializer = null;
                retiredCache = m_MessageSender?.InvalidateCache();
                return true;
            }
        }

        void EnsureMessageName(string expectedMessageName)
        {
            if (string.IsNullOrEmpty(expectedMessageName))
                throw new ArgumentException("Message name cannot be null or empty.", nameof(expectedMessageName));
            MessageUse retiredCache = null;
            lock (m_StateLock)
            {
                if (m_RosMessageName != null
                    && !string.Equals(m_RosMessageName, expectedMessageName, StringComparison.Ordinal))
                    throw new ArgumentException("The configured topic message type conflicts with the requested role.",
                        nameof(expectedMessageName));
                if (m_RosMessageName == null)
                {
                    m_RosMessageName = expectedMessageName;
                    m_SchemaRevision++;
                    m_Deserializer = null;
                    retiredCache = m_MessageSender?.InvalidateCache();
                }
            }
            retiredCache?.Release(true);
        }

        internal void OnMessageReceived(byte[] data)
        {
            OnMessageReceivedForAttempt(data, null);
        }

        internal void OnMessageReceivedForAttempt(byte[] data, ROSConnection.AttemptContext attempt)
        {
            if (attempt != null && !m_Connection.IsAttemptAccepting(attempt))
                return;
            m_LastMessageReceivedRealtime = Time.realtimeSinceStartup;
            if (m_IsRosService && m_ServiceResponseTopic != null)
            {
                //  For a service, incoming messages are a different type from outgoing messages.
                //  We process them using a separate RosTopicState.
                m_ServiceResponseTopic.OnMessageReceivedForAttempt(data, attempt);
                return;
            }

            // don't bother deserializing this message if nobody cares
            ROSConnection.CallbackRegistration<Message>[] callbacks;
            lock (m_StateLock)
                callbacks = m_SubscriberCallbacks.ToArray();
            if (callbacks.Length == 0)
                return;

            Message message = Deserialize(data);
            MessageUse inboundUse = MessageUseRegistry.Acquire(message, null);
            try
            {
                foreach (ROSConnection.CallbackRegistration<Message> registration in callbacks)
                {
                    if (!m_Connection.TryBeginCallback(attempt, registration))
                        continue;
                    registration.Callback(message);
                }
            }
            finally
            {
                // The decoded root is borrowed for the complete synchronous batch.
                // An earlier observer may republish it through a pooled sender while
                // later observers still need the same reference.
                inboundUse.Release(false);
            }
        }

        internal void OnServiceResponseForAttempt(byte[] data, ROSConnection.AttemptContext attempt,
            ServiceResponseSnapshot snapshot)
        {
            if (snapshot == null || (attempt != null && !m_Connection.IsAttemptAccepting(attempt)))
                return;
            m_LastMessageReceivedRealtime = Time.realtimeSinceStartup;
            if (snapshot.Callbacks.Length == 0)
                return;

            Message message = Deserialize(data, snapshot.Deserializer);
            if (attempt != null && !m_Connection.IsAttemptAccepting(attempt))
                return;
            MessageUse inboundUse = MessageUseRegistry.Acquire(message, null);
            try
            {
                foreach (ROSConnection.CallbackRegistration<Message> registration in snapshot.Callbacks)
                {
                    if (!m_Connection.TryBeginCallback(attempt, registration))
                        continue;
                    registration.Callback(message);
                }
            }
            finally
            {
                // The decoded root is borrowed for the complete synchronous response batch.
                inboundUse.Release(false);
            }
        }

        bool OnMessageSentForAttempt(Message message, ROSConnection.AttemptContext attempt)
        {
            if (attempt != null && !m_Connection.IsAttemptAccepting(attempt))
                return false;
            m_LastMessageSentRealtime = ROSConnection.s_RealTimeSinceStartup;
            if (m_RosMessageName == null)
            {
                ChangeRosMessageName(message.RosMessageName);
            }

            ROSConnection.CallbackRegistration<Message>[] callbacks;
            lock (m_StateLock)
                callbacks = m_SubscriberCallbacks.ToArray();
            foreach (ROSConnection.CallbackRegistration<Message> registration in callbacks)
            {
                if (!m_Connection.TryBeginCallback(attempt, registration))
                    continue;
                registration.Callback(message);
            }
            return attempt == null || m_Connection.IsAttemptAccepting(attempt);
        }

        internal async void HandleUnityServiceRequest(byte[] data, int serviceId, ROSConnection.AttemptContext attempt)
        {
            try
            {
                Func<Message, Message> implementation;
                Func<Message, Task<Message>> implementationAsync;
                RosTopicState responseTopic;
                TopicMessageSender sender;
                lock (m_StateLock)
                {
                    implementation = m_ServiceImplementation;
                    implementationAsync = m_ServiceImplementationAsync;
                    responseTopic = m_ServiceResponseTopic;
                    sender = m_MessageSender;
                }
                if (implementation == null && implementationAsync == null)
                {
                    Debug.LogError($"Unity service '{m_Topic}' has not been implemented!");
                    return;
                }

                OnMessageReceivedForAttempt(data, attempt);
                if (!m_Connection.IsAttemptAccepting(attempt))
                    return;

                // deserialize the request message
                Message requestMessage = Deserialize(data);
                m_AfterUnityServiceRequestDeserializationTestHook?.Invoke();
                if (!m_Connection.IsAttemptAccepting(attempt))
                    return;

                MessageUse requestUse = MessageUseRegistry.Acquire(requestMessage, null);
                try
                {
                    // run the actual service
                    Message response;

                    try
                    {
                        if (implementationAsync != null)
                            response = await implementationAsync(requestMessage);
                        else
                            response = implementation(requestMessage);
                    }
                    catch (Exception exception)
                    {
                        Debug.LogError($"Unity service '{m_Topic}' implementation failed: {exception}");
                        return;
                    }

                    m_AfterUnityServiceRequestImplementationTestHook?.Invoke();
                    if (!m_Connection.IsAttemptAccepting(attempt))
                        return;

                    MessageUse use = null;
                    try
                    {
                        use = sender.AcquireMessageUse(response);
                        if (!responseTopic.OnMessageSentForAttempt(response, attempt))
                            return;
                        if (!m_ConnectionInternal.QueueServicePairForUse(SysCommand.k_SysCommand_ServiceResponse,
                            new SysCommand_Service { srv_id = serviceId }, sender, use, attempt))
                            return;
                        use = null; // Exact capsule owns the admitted source use.
                    }
                    catch (Exception exception)
                    {
                        Debug.LogError($"Unity service '{m_Topic}' response could not be queued: {exception}");
                    }
                    finally { use?.Release(false); }
                }
                finally { requestUse.Release(false); }
            }
            catch (Exception exception)
            {
                Debug.LogError($"Unity service '{m_Topic}' request handling failed: {exception}");
            }
        }

        Message Deserialize(byte[] data)
        {
            Func<MessageDeserializer, Message> deserializer;
            lock (m_StateLock)
            {
                if (m_Deserializer == null)
                    m_Deserializer = MessageRegistry.GetDeserializeFunction(m_RosMessageName, m_Subtopic);
                deserializer = m_Deserializer;
            }
            var messageDeserializer = new MessageDeserializer();
            messageDeserializer.InitWithBuffer(data);
            return deserializer(messageDeserializer);
        }

        Message Deserialize(byte[] data, Func<MessageDeserializer, Message> deserializer)
        {
            var messageDeserializer = new MessageDeserializer();
            messageDeserializer.InitWithBuffer(data);
            return deserializer(messageDeserializer);
        }

        bool ShouldRegisterSubscriberLocked()
        {
            return m_SubscriberCallbacks.Count > 0 && m_ServiceResponseTopic == null
                && m_Subtopic != MessageSubtopic.Response && !m_IsRosService
                && m_ServiceImplementation == null && m_ServiceImplementationAsync == null;
        }

        internal ExistingTopicConfiguration PrepareSubscriberConfiguration(Action<Message> callback,
            string expectedMessageName)
        {
            string messageName;
            lock (m_StateLock)
            {
                if (expectedMessageName != null)
                {
                    if (string.IsNullOrEmpty(expectedMessageName))
                        throw new ArgumentException("Message name cannot be null or empty.", nameof(expectedMessageName));
                    bool hasActiveConfiguration = m_IsPublisher || m_IsRosService
                        || m_ServiceImplementation != null || m_ServiceImplementationAsync != null
                        || m_SubscriberCallbacks.Count > 0 || m_Subtopic == MessageSubtopic.Response;
                    if (m_RosMessageName != null
                        && !string.Equals(m_RosMessageName, expectedMessageName, StringComparison.Ordinal)
                        && hasActiveConfiguration)
                    {
                        throw new ArgumentException(
                            "The configured topic message type conflicts with the requested subscriber.",
                            nameof(expectedMessageName));
                    }
                    messageName = m_RosMessageName == null || !string.Equals(m_RosMessageName,
                        expectedMessageName, StringComparison.Ordinal) ? expectedMessageName : m_RosMessageName;
                }
                else
                {
                    messageName = m_RosMessageName;
                }
            }
            var registration = new ROSConnection.CallbackRegistration<Message>(null, callback);
            return new ExistingTopicConfiguration(ExistingConfigurationKind.Subscriber, expectedMessageName,
                messageName, 0, false, null, registration, null, null, null, null);
        }

        internal ExistingTopicConfiguration PreparePublisherConfiguration(int queueSize, bool latch,
            string expectedMessageName)
        {
            if (queueSize < 1)
                throw new Exception("Queue size must be greater than or equal to 1.");
            string messageName;
            bool alreadyPublisher;
            lock (m_StateLock)
            {
                if (expectedMessageName != null)
                {
                    if (string.IsNullOrEmpty(expectedMessageName))
                        throw new ArgumentException("Message name cannot be null or empty.", nameof(expectedMessageName));
                    if (m_RosMessageName != null
                        && !string.Equals(m_RosMessageName, expectedMessageName, StringComparison.Ordinal))
                        throw new ArgumentException("The configured topic message type conflicts with the requested role.",
                            nameof(expectedMessageName));
                }
                if (m_IsRosService || m_ServiceImplementation != null || m_ServiceImplementationAsync != null)
                    throw new InvalidOperationException("A configured service cannot become a publisher.");
                messageName = m_RosMessageName ?? expectedMessageName;
                alreadyPublisher = m_IsPublisher;
            }
            TopicMessageSender sender = alreadyPublisher ? null
                : new TopicMessageSender(Topic, messageName, queueSize, m_Connection, this);
            return new ExistingTopicConfiguration(ExistingConfigurationKind.Publisher, expectedMessageName,
                messageName, queueSize, latch, null, null, null, null, sender, null);
        }

        internal ExistingTopicConfiguration PrepareUnityServiceConfiguration(
            Func<Message, Message> implementation, Func<Message, Task<Message>> implementationAsync,
            string responseMessageName, int queueSize, string expectedRequestName)
        {
            if (queueSize < 1)
                throw new Exception("Queue size must be greater than or equal to 1.");
            if (string.IsNullOrEmpty(expectedRequestName))
                throw new ArgumentException("Message name cannot be null or empty.", nameof(expectedRequestName));
            string messageName;
            RosTopicState responseTopic;
            lock (m_StateLock)
            {
                if (m_RosMessageName != null
                    && !string.Equals(m_RosMessageName, expectedRequestName, StringComparison.Ordinal))
                    throw new ArgumentException("The configured topic message type conflicts with the requested role.",
                        nameof(expectedRequestName));
                if (m_IsPublisher || m_IsRosService)
                    throw new InvalidOperationException("A configured publisher or ROS service cannot become a Unity service.");
                messageName = m_RosMessageName ?? expectedRequestName;
                responseTopic = m_ServiceResponseTopic;
            }
            if (responseTopic == null && m_Subtopic == MessageSubtopic.Default)
                responseTopic = new RosTopicState(Topic, responseMessageName, m_Connection, m_ConnectionInternal,
                    true, MessageSubtopic.Response);
            TopicMessageSender sender = new TopicMessageSender(Topic, messageName, queueSize, m_Connection, this);
            return new ExistingTopicConfiguration(ExistingConfigurationKind.UnityService, expectedRequestName,
                messageName, queueSize, false, responseMessageName, null, implementation, implementationAsync,
                sender, responseTopic);
        }

        internal ExistingTopicConfiguration PrepareRosServiceConfiguration(string responseMessageName,
            int queueSize, string expectedRequestName)
        {
            if (string.IsNullOrEmpty(responseMessageName))
                throw new ArgumentException("responseMessageName cannot be null or empty.", nameof(responseMessageName));
            if (queueSize < 1)
                throw new Exception("Queue size must be greater than or equal to 1.");
            string messageName;
            RosTopicState responseTopic;
            lock (m_StateLock)
            {
                if (expectedRequestName != null && m_RosMessageName != null
                    && !string.Equals(m_RosMessageName, expectedRequestName, StringComparison.Ordinal))
                    throw new ArgumentException("The configured topic message type conflicts with the requested role.",
                        nameof(expectedRequestName));
                if (m_IsPublisher && !m_IsRosService || m_ServiceImplementation != null
                    || m_ServiceImplementationAsync != null)
                    throw new InvalidOperationException("A configured publisher or Unity service cannot become a ROS service.");
                messageName = m_RosMessageName ?? expectedRequestName;
                responseTopic = m_ServiceResponseTopic;
            }
            if (responseTopic == null && m_Subtopic == MessageSubtopic.Default)
                responseTopic = new RosTopicState(Topic, responseMessageName, m_Connection, m_ConnectionInternal,
                    true, MessageSubtopic.Response);
            TopicMessageSender sender = new TopicMessageSender(Topic, messageName, queueSize, m_Connection, this);
            return new ExistingTopicConfiguration(ExistingConfigurationKind.RosService, expectedRequestName,
                messageName, queueSize, false, responseMessageName, null, null, null, sender, responseTopic);
        }

        internal bool ApplySubscriberConfiguration(ExistingTopicConfiguration configuration,
            out bool shouldRegister, out string previousMessageName)
        {
            shouldRegister = false;
            previousMessageName = null;
            lock (m_StateLock)
            {
                if (configuration.ExpectedMessageName != null)
                {
                    if (m_RosMessageName != null
                        && !string.Equals(m_RosMessageName, configuration.ExpectedMessageName, StringComparison.Ordinal))
                    {
                        bool hasActiveConfiguration = m_IsPublisher || m_IsRosService
                            || m_ServiceImplementation != null || m_ServiceImplementationAsync != null
                            || m_SubscriberCallbacks.Count > 0 || m_Subtopic == MessageSubtopic.Response;
                        if (hasActiveConfiguration)
                            throw new ArgumentException(
                                "The configured topic message type conflicts with the requested subscriber.",
                                nameof(configuration.ExpectedMessageName));
                        previousMessageName = m_RosMessageName;
                        m_RosMessageName = configuration.ExpectedMessageName;
                        m_SchemaRevision++;
                        m_Deserializer = null;
                    }
                    else if (m_RosMessageName == null)
                    {
                        m_RosMessageName = configuration.ExpectedMessageName;
                        m_SchemaRevision++;
                        m_Deserializer = null;
                    }
                    if (!string.Equals(m_RosMessageName, configuration.MessageName, StringComparison.Ordinal)
                        && !string.Equals(configuration.MessageName, configuration.ExpectedMessageName, StringComparison.Ordinal))
                        throw new InvalidOperationException("The topic configuration changed while it was being prepared.");
                }
                else if (!string.Equals(m_RosMessageName, configuration.MessageName, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException("The topic configuration changed while it was being prepared.");
                }

                bool hadSubscriber = m_SubscriberCallbacks.Count > 0;
                m_SubscriberCallbacks.Add(configuration.Subscriber);
                m_IsDiscoveryPlaceholder = false;
                shouldRegister = !hadSubscriber && ShouldRegisterSubscriberLocked();
            }
            return true;
        }

        internal bool ApplyPublisherConfiguration(ExistingTopicConfiguration configuration,
            out TopicMessageSender displaced)
        {
            displaced = null;
            lock (m_StateLock)
            {
                if (configuration.ExpectedMessageName != null
                    && m_RosMessageName != null
                    && !string.Equals(m_RosMessageName, configuration.ExpectedMessageName, StringComparison.Ordinal))
                    throw new ArgumentException("The configured topic message type conflicts with the requested role.",
                        nameof(configuration.ExpectedMessageName));
                if (m_IsRosService || m_ServiceImplementation != null || m_ServiceImplementationAsync != null)
                    throw new InvalidOperationException("A configured service cannot become a publisher.");
                if (m_IsPublisher)
                    return false;
                if (m_RosMessageName == null && configuration.ExpectedMessageName != null)
                {
                    m_RosMessageName = configuration.ExpectedMessageName;
                    m_SchemaRevision++;
                    m_Deserializer = null;
                }
                if (!string.Equals(m_RosMessageName, configuration.MessageName, StringComparison.Ordinal))
                    throw new InvalidOperationException("The topic configuration changed while it was being prepared.");
                if (configuration.Sender == null)
                    throw new InvalidOperationException("The publisher configuration has no prepared sender.");
                m_IsPublisher = true;
                m_IsDiscoveryPlaceholder = false;
                m_IsPublisherLatched = configuration.Latch;
                m_PublisherQueueSize = configuration.QueueSize;
                displaced = m_MessageSender;
                m_MessageSender = configuration.Sender;
                return true;
            }
        }

        internal bool ApplyUnityServiceConfiguration(ExistingTopicConfiguration configuration,
            out TopicMessageSender displaced, out RosTopicState responseTopic)
        {
            displaced = null;
            responseTopic = null;
            lock (m_StateLock)
            {
                if (m_RosMessageName != null
                    && !string.Equals(m_RosMessageName, configuration.ExpectedMessageName, StringComparison.Ordinal))
                    throw new ArgumentException("The configured topic message type conflicts with the requested role.",
                        nameof(configuration.ExpectedMessageName));
                if (m_IsPublisher || m_IsRosService)
                    throw new InvalidOperationException("A configured publisher or ROS service cannot become a Unity service.");
                if (m_RosMessageName == null)
                {
                    m_RosMessageName = configuration.ExpectedMessageName;
                    m_SchemaRevision++;
                    m_Deserializer = null;
                }
                if (!string.Equals(m_RosMessageName, configuration.MessageName, StringComparison.Ordinal))
                    throw new InvalidOperationException("The topic configuration changed while it was being prepared.");
                if (configuration.Sender == null)
                    throw new InvalidOperationException("The Unity service configuration has no prepared sender.");
                if (m_ServiceResponseTopic == null && m_Subtopic == MessageSubtopic.Default)
                    m_ServiceResponseTopic = configuration.PreparedResponseTopic;
                m_ServiceImplementation = configuration.Implementation;
                m_ServiceImplementationAsync = configuration.ImplementationAsync;
                m_IsDiscoveryPlaceholder = false;
                m_UnityServiceResponseMessageName = configuration.ResponseMessageName;
                responseTopic = m_ServiceResponseTopic;
                displaced = m_MessageSender;
                m_MessageSender = configuration.Sender;
                return true;
            }
        }

        internal bool ApplyRosServiceConfiguration(ExistingTopicConfiguration configuration,
            out TopicMessageSender displaced, out RosTopicState responseTopic)
        {
            displaced = null;
            responseTopic = null;
            lock (m_StateLock)
            {
                if (configuration.ExpectedMessageName != null && m_RosMessageName != null
                    && !string.Equals(m_RosMessageName, configuration.ExpectedMessageName, StringComparison.Ordinal))
                    throw new ArgumentException("The configured topic message type conflicts with the requested role.",
                        nameof(configuration.ExpectedMessageName));
                if (m_IsPublisher && !m_IsRosService || m_ServiceImplementation != null
                    || m_ServiceImplementationAsync != null)
                    throw new InvalidOperationException("A configured publisher or Unity service cannot become a ROS service.");
                if (m_RosMessageName == null && configuration.ExpectedMessageName != null)
                {
                    m_RosMessageName = configuration.ExpectedMessageName;
                    m_SchemaRevision++;
                    m_Deserializer = null;
                }
                if (!string.Equals(m_RosMessageName, configuration.MessageName, StringComparison.Ordinal))
                    throw new InvalidOperationException("The topic configuration changed while it was being prepared.");
                if (configuration.Sender == null)
                    throw new InvalidOperationException("The ROS service configuration has no prepared sender.");
                if (m_ServiceResponseTopic == null && m_Subtopic == MessageSubtopic.Default)
                    m_ServiceResponseTopic = configuration.PreparedResponseTopic;
                m_IsRosService = true;
                m_IsDiscoveryPlaceholder = false;
                m_RosServiceResponseMessageName = configuration.ResponseMessageName;
                responseTopic = m_ServiceResponseTopic;
                displaced = m_MessageSender;
                m_MessageSender = configuration.Sender;
                return true;
            }
        }

        // Transaction commits update the visible name while the catalog gate is held,
        // but cache invalidation and any source release are intentionally deferred.
        internal bool ApplyMessageNameForTransaction(string rosMessageName,
            out string previousMessageName, out TopicMessageSender cacheOwner)
        {
            lock (m_StateLock)
            {
                previousMessageName = m_RosMessageName;
                if (string.Equals(previousMessageName, rosMessageName, StringComparison.Ordinal))
                {
                    cacheOwner = null;
                    return false;
                }
                m_RosMessageName = rosMessageName;
                m_SchemaRevision++;
                m_Deserializer = null;
                cacheOwner = m_MessageSender;
                return true;
            }
        }

        internal bool AddSubscriberLocal(Action<Message> callback)
        {
            var registration = new ROSConnection.CallbackRegistration<Message>(null, callback);
            lock (m_StateLock)
            {
                m_SubscriberCallbacks.Add(registration);
                return ShouldRegisterSubscriberLocked();
            }
        }

        public void AddSubscriber(Action<Message> callback)
        {
            AddSubscriber(callback, null);
        }

        internal void AddSubscriber(Action<Message> callback, string expectedMessageName)
        {
            m_Connection.ConfigureExistingSubscriber(this, callback, expectedMessageName);
        }

        public void UnsubscribeAll()
        {
            ROSConnection.CallbackRegistration<Message>[] registrations;
            lock (m_StateLock)
            {
                registrations = m_SubscriberCallbacks.ToArray();
                m_SubscriberCallbacks.Clear();
            }
            m_Connection.RevokeCallbacks(registrations);
            m_Connection.RemoveDesiredRegistration(this, ROSConnection.RegistrationRole.Subscriber);
        }

        internal RosTopicState ConfigureUnityServiceLocal(Func<Message, Message> implementation,
            Func<Message, Task<Message>> implementationAsync, string responseName, int queueSize,
            out TopicMessageSender displaced)
        {
            if (queueSize < 1)
                throw new Exception("Queue size must be greater than or equal to 1.");
            RosTopicState responseTopic;
            lock (m_StateLock)
            {
                if (m_IsPublisher || m_IsRosService)
                    throw new InvalidOperationException("A configured publisher or ROS service cannot become a Unity service.");
                if (m_ServiceResponseTopic == null && m_Subtopic == MessageSubtopic.Default)
                    m_ServiceResponseTopic = new RosTopicState(m_Topic, m_RosMessageName, m_Connection,
                        m_ConnectionInternal, true, MessageSubtopic.Response);
                m_ServiceImplementation = implementation;
                m_ServiceImplementationAsync = implementationAsync;
                m_IsDiscoveryPlaceholder = false;
                m_UnityServiceResponseMessageName = responseName;
                responseTopic = m_ServiceResponseTopic;
                displaced = CreateMessageSenderLocked(queueSize);
            }
            responseTopic.ChangeRosMessageName(responseName);
            return responseTopic;
        }

        public void ImplementService<TRequest, TResponse>(Func<TRequest, TResponse> implementation, int queueSize)
            where TRequest : Message
            where TResponse : Message
        {
            ImplementService(implementation, queueSize, MessageRegistry.GetRosMessageName<TRequest>());
        }

        internal void ImplementService<TRequest, TResponse>(Func<TRequest, TResponse> implementation,
            int queueSize, string expectedRequestName)
            where TRequest : Message
            where TResponse : Message
        {
            string responseName = MessageRegistry.GetRosMessageName<TResponse>();
            m_Connection.ConfigureExistingUnityService(this,
                (Message msg) => implementation((TRequest)msg), null, responseName, queueSize, expectedRequestName);
        }

        public void ImplementService<TRequest, TResponse>(Func<TRequest, Task<TResponse>> implementation, int queueSize)
            where TRequest : Message
            where TResponse : Message
        {
            ImplementService(implementation, queueSize, MessageRegistry.GetRosMessageName<TRequest>());
        }

        internal void ImplementService<TRequest, TResponse>(Func<TRequest, Task<TResponse>> implementation,
            int queueSize, string expectedRequestName)
            where TRequest : Message
            where TResponse : Message
        {
            string responseName = MessageRegistry.GetRosMessageName<TResponse>();
            m_Connection.ConfigureExistingUnityService(this, null,
                async (Message msg) => await implementation((TRequest)msg), responseName, queueSize, expectedRequestName);
        }

        internal bool ConfigurePublisherLocal(int queueSize, bool latch, out TopicMessageSender displaced)
        {
            if (queueSize < 1)
                throw new Exception("Queue size must be greater than or equal to 1.");
            lock (m_StateLock)
            {
                if (m_IsRosService || m_ServiceImplementation != null || m_ServiceImplementationAsync != null)
                    throw new InvalidOperationException("A configured service cannot become a publisher.");
                if (m_IsPublisher)
                {
                    displaced = null;
                    return false;
                }
                m_IsPublisher = true;
                m_IsDiscoveryPlaceholder = false;
                m_IsPublisherLatched = latch;
                m_PublisherQueueSize = queueSize;
                displaced = CreateMessageSenderLocked(queueSize);
                return true;
            }
        }

        public void RegisterPublisher(int queueSize, bool latch)
        {
            RegisterPublisher(queueSize, latch, null);
        }

        public void RegisterPublisher(int queueSize, bool latch, string expectedMessageName)
        {
            m_Connection.ConfigureExistingPublisher(this, queueSize, latch, expectedMessageName);
        }

        public void Publish(Message message)
        {
            TopicMessageSender sender;
            lock (m_StateLock)
                sender = m_MessageSender;
            if (!m_ConnectionInternal.TryBeginPublication(sender, out ROSConnection.AttemptContext attempt,
                out TopicMessageSender.PendingQueue queue))
                throw new System.IO.IOException("Publishing requires an authorized connection epoch.");

            MessageUse use = null;
            TopicMessageSender.OwnedPublication publication = null;
            bool preparationActive = true;
            try
            {
                use = sender.AcquireMessageUse(message);
                if (!NotifyMessageForAttempt(message, attempt))
                    throw new System.IO.IOException("The publishing connection epoch is no longer active.");
                publication = sender.PreparePublication(use);
                use = null; // The prepared publication now owns the source lease.
                bool admitted = m_ConnectionInternal.CommitPublication(sender, attempt, queue, publication);
                preparationActive = false;
                if (!admitted)
                    throw new System.IO.IOException("The publishing connection epoch is no longer active.");
                publication = null; // The admitted outbox unit owns the publication.
                m_LastMessageSentRealtime = ROSConnection.s_RealTimeSinceStartup;
            }
            finally
            {
                if (preparationActive)
                    m_ConnectionInternal.AbortPublicationPreparation(queue);
                sender.AbortPublication(publication, false);
                use?.Release(false);
            }
        }

        TopicMessageSender CreateMessageSenderLocked(int queueSize)
        {
            TopicMessageSender displaced = m_MessageSender;
            m_MessageSender = new TopicMessageSender(Topic, m_RosMessageName, queueSize, m_Connection, this);
            return displaced; // Its queue/cache uses must be released AFTER the topic gate.
        }

        public void SetMessagePool(IMessagePool messagePool)
        {
            TopicMessageSender sender;
            lock (m_StateLock)
                sender = m_MessageSender;
            sender.SetMessagePool(messagePool);
        }

        internal RosTopicState ConfigureRosServiceLocal(string responseMessageName, int queueSize,
            out TopicMessageSender displaced)
        {
            if (queueSize < 1)
                throw new Exception("Queue size must be greater than or equal to 1.");
            RosTopicState responseTopic;
            lock (m_StateLock)
            {
                if (m_IsPublisher && !m_IsRosService || m_ServiceImplementation != null || m_ServiceImplementationAsync != null)
                    throw new InvalidOperationException("A configured publisher or Unity service cannot become a ROS service.");
                if (m_ServiceResponseTopic == null && m_Subtopic == MessageSubtopic.Default)
                    m_ServiceResponseTopic = new RosTopicState(m_Topic, m_RosMessageName, m_Connection,
                        m_ConnectionInternal, true, MessageSubtopic.Response);
                m_IsRosService = true;
                m_IsDiscoveryPlaceholder = false;
                m_RosServiceResponseMessageName = responseMessageName;
                responseTopic = m_ServiceResponseTopic;
                displaced = CreateMessageSenderLocked(queueSize);
            }
            responseTopic.ChangeRosMessageName(responseMessageName);
            return responseTopic;
        }

        public void RegisterRosService(string responseMessageName, int queueSize)
        {
            RegisterRosService(null, responseMessageName, queueSize);
        }

        internal void RegisterRosService(string requestMessageName, string responseMessageName, int queueSize)
        {
            m_Connection.ConfigureExistingRosService(this, requestMessageName, responseMessageName, queueSize);
        }

        internal bool TryReservePendingServiceCall()
        {
            lock (m_StateLock)
            {
                if (!m_IsRosService || m_MessageSender == null || m_PendingServiceCalls >= m_MessageSender.QueueSize)
                    return false;
                m_PendingServiceCalls++;
                return true;
            }
        }

        internal void ReleasePendingServiceCall()
        {
            lock (m_StateLock)
            {
                if (m_PendingServiceCalls <= 0)
                    throw new InvalidOperationException("Pending ROS service call accounting underflow.");
                m_PendingServiceCalls--;
            }
        }

        internal bool TryReserveServiceDecoder()
        {
            lock (m_StateLock)
            {
                if (!m_IsRosService || m_MessageSender == null
                    || m_ServiceDecoderTickets >= m_MessageSender.QueueSize)
                    return false;
                m_ServiceDecoderTickets++;
                return true;
            }
        }

        internal void ReleaseServiceDecoder()
        {
            lock (m_StateLock)
            {
                if (m_ServiceDecoderTickets <= 0)
                    throw new InvalidOperationException("Service decoder ticket accounting underflow.");
                m_ServiceDecoderTickets--;
            }
        }

        internal TopicMessageSender ServiceRequestSenderSnapshot()
        {
            lock (m_StateLock)
                return m_MessageSender;
        }

        internal ExactMessageSender CreateServiceRequest(MessageUse use, int serviceId, TopicMessageSender sender)
        {
            return m_ConnectionInternal.CreateServicePair(SysCommand.k_SysCommand_ServiceRequest,
                new SysCommand_Service { srv_id = serviceId }, sender, use);
        }

        internal void NotifyServiceRequest(Message requestMessage, ROSConnection.AttemptContext attempt)
        {
            if (!NotifyMessageForAttempt(requestMessage, attempt))
                throw new System.IO.IOException("The ROS service request connection attempt is no longer active.");
        }

        bool NotifyMessageForAttempt(Message message, ROSConnection.AttemptContext attempt)
        {
            if (attempt != null && !m_Connection.IsAttemptAccepting(attempt))
                return false;
            if (m_RosMessageName == null)
                ChangeRosMessageName(message.RosMessageName);
            ROSConnection.CallbackRegistration<Message>[] callbacks;
            lock (m_StateLock)
                callbacks = m_SubscriberCallbacks.ToArray();
            foreach (ROSConnection.CallbackRegistration<Message> registration in callbacks)
            {
                if (!m_Connection.TryBeginCallback(attempt, registration))
                    continue;
                registration.Callback(message);
            }
            return attempt == null || m_Connection.IsAttemptAccepting(attempt);
        }

        internal void OnConnectionEstablished(NetworkStream stream, ROSConnection.AttemptContext attempt)
        {
            // Kept as an internal compatibility seam. Registration admission and
            // wire completion are owned by ROSConnection's single reconciler; this
            // method never writes to the supplied stream.
            m_Connection.ReconcileRegistrations(attempt);
        }


    }
}
