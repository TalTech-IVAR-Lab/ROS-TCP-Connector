using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using RosMessageTypes.Std;
using Unity.Robotics.ROSTCPConnector;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using UnityEngine;
using UnityEngine.TestTools;

namespace UnitTests
{
    public class ConnectorOwnershipTests
    {
        sealed class RecordingPool : IMessagePool
        {
            public readonly ManualResetEventSlim Offered = new ManualResetEventSlim(false);
            public Message Message;
            public int Offers;

            public void AddMessage(Message message)
            {
                Interlocked.Increment(ref Offers);
                Volatile.Write(ref Message, message);
                Offered.Set();
            }
        }

        sealed class Slice7ServiceResponse : Message
        {
            public const string k_RosMessageName = "slice7/ServiceResponse";
            public override string RosMessageName => k_RosMessageName;
            public override void SerializeTo(MessageSerializer serializer) { }
            public static Slice7ServiceResponse Deserialize(MessageDeserializer deserializer)
            {
                return new Slice7ServiceResponse();
            }
        }

        sealed class Slice7CallbackGateMessage : Message
        {
            public const string k_RosMessageName = "slice7/CallbackGate";
            internal static Action DeserializeEntered;
            public override string RosMessageName => k_RosMessageName;
            public override void SerializeTo(MessageSerializer serializer) { }
            public static Slice7CallbackGateMessage Deserialize(MessageDeserializer deserializer)
            {
                DeserializeEntered?.Invoke();
                return new Slice7CallbackGateMessage();
            }
        }

        // Exercise the standalone channel's real queue/eviction path. The connector
        // performs this same deferred release after its admission gate has unwound.
        static void Queue(TopicMessageSender sender, Message message)
        {
            object use = typeof(TopicMessageSender).GetMethod("AcquireMessageUse",
                BindingFlags.Instance | BindingFlags.NonPublic).Invoke(sender, new object[] { message });
            object displaced = typeof(TopicMessageSender).GetMethod("Queue",
                BindingFlags.Instance | BindingFlags.NonPublic).Invoke(sender, new[] { use });
            if (displaced != null)
                displaced.GetType().GetMethod("Release", BindingFlags.Instance | BindingFlags.NonPublic)
                    .Invoke(displaced, new object[] { true });
        }

        static void RecyclerFence()
        {
            var channel = new TopicMessageSender("/fence", UInt8MultiArrayMsg.k_RosMessageName, 1);
            var pool = new RecordingPool();
            channel.SetMessagePool(pool);
            Prepare(channel, new UInt8MultiArrayMsg(), Control(SysCommand.k_SysCommand_ServiceRequest, 997)).ClearAllQueuedData();
            Assert.IsTrue(pool.Offered.Wait(TimeSpan.FromSeconds(3)), "FIFO recycler fence must finish.");
        }

        [Test]
        public void QueueSizeOneRepeatedReferenceDoesNotRecycleQueuedUse()
        {
            var source = new UInt8MultiArrayMsg();
            var channel = new TopicMessageSender("/duplicate", source.RosMessageName, 1);
            var pool = new RecordingPool();
            channel.SetMessagePool(pool);
            try
            {
                Queue(channel, source);
                Queue(channel, source);
                RecyclerFence();
                Assert.AreEqual(0, Volatile.Read(ref pool.Offers), "The replacement queue entry still owns this exact root.");
                Assert.IsTrue(channel.PeekNextMessageToSend(out Message peek));
                Assert.AreSame(source, peek);
                channel.ClearAllQueuedData();
                RecyclerFence();
                Assert.AreEqual(1, Volatile.Read(ref pool.Offers), "One completed ownership generation gets at most one offer.");
            }
            finally { channel.ClearAllQueuedData(); }
        }

        [TestCase(false)]
        [TestCase(true)]
        public void QueuedDuplicateCannotReleaseClaimedSource(bool exact)
        {
            var source = new UInt8MultiArrayMsg();
            var channel = new TopicMessageSender("/claimed-duplicate", source.RosMessageName, 1);
            var pool = new RecordingPool();
            channel.SetMessagePool(pool);
            OutgoingMessageSender first;
            if (exact)
                first = Prepare(channel, source, Control(SysCommand.k_SysCommand_ServiceRequest, 998));
            else
            {
                Queue(channel, source);
                first = channel;
            }
            using (var wire = new HeldWriteStream(true))
            {
                Task<Exception> writer = Task.Run(() =>
                {
                    try { first.SendInternal(new MessageSerializer(), wire); return (Exception)null; }
                    catch (Exception exception) { return exception; }
                });
                try
                {
                    Assert.IsTrue(wire.Entered.Wait(TimeSpan.FromSeconds(3)));
                    Queue(channel, source);
                    channel.ClearAllQueuedData();
                    RecyclerFence();
                    Assert.AreEqual(0, Volatile.Read(ref pool.Offers), "Clearing a duplicate cannot release the claimed root.");
                    if (exact) Assert.AreEqual(1, Capacity(channel));
                }
                finally
                {
                    wire.Release.Set();
                    Assert.IsTrue(writer.Wait(TimeSpan.FromSeconds(3)));
                    channel.ClearAllQueuedData();
                }
                Assert.AreSame(wire.Failure, writer.Result);
                RecyclerFence();
                Assert.AreEqual(1, Volatile.Read(ref pool.Offers));
                Assert.AreEqual(0, Capacity(channel));
            }
        }

        sealed class LiveConnection : IDisposable
        {
            internal readonly GameObject Object = new GameObject("ownership live connection");
            internal readonly ROSConnection Connection;
            readonly TcpListener m_Listener = new TcpListener(IPAddress.Loopback, 0);
            readonly ROSConnectionConfig m_Config = ScriptableObject.CreateInstance<ROSConnectionConfig>();
            TcpClient m_Peer;
            internal LiveConnection()
            {
                Connection = Object.AddComponent<ROSConnection>();
                Connection.ConnectOnStart = false;
                Connection.listenForTFMessages = false;
                m_Listener.Start();
                typeof(ROSConnectionConfig).GetField("m_RosIPAddress", BindingFlags.Instance | BindingFlags.NonPublic)
                    .SetValue(m_Config, "127.0.0.1");
                typeof(ROSConnectionConfig).GetField("m_RosPort", BindingFlags.Instance | BindingFlags.NonPublic)
                    .SetValue(m_Config, ((IPEndPoint)m_Listener.LocalEndpoint).Port);
                Connection.ConnectionConfig = m_Config;
            }
            internal void Connect()
            {
                m_Peer?.Close();
                m_Peer = null;
                Task<TcpClient> accept = m_Listener.AcceptTcpClientAsync();
                Connection.Connect();
                Assert.IsTrue(accept.Wait(TimeSpan.FromSeconds(3)));
                m_Peer = accept.Result;
            }
            internal object Attempt => AttemptTestAccess.Current(Connection);
            internal void SendFrame(string topic, byte[] payload)
            {
                var writer = new BinaryWriter(m_Peer.GetStream(), Encoding.UTF8, true);
                byte[] name = Encoding.UTF8.GetBytes(topic);
                writer.Write(name.Length);
                writer.Write(name);
                writer.Write(payload.Length);
                writer.Write(payload);
                writer.Flush();
            }
            internal List<Tuple<string, byte[]>> ReadThrough(string fence, int maxFrames = 64)
            {
                m_Peer.GetStream().ReadTimeout = 5000;
                var result = new List<Tuple<string, byte[]>>();
                for (int index = 0; index < maxFrames; index++)
                {
                    Tuple<string, byte[]> frame = ReadFrame(m_Peer.GetStream());
                    result.Add(frame);
                    if (frame.Item1 == fence) return result;
                }
                Assert.Fail("Wire fence missing after bounded startup/data frames.");
                return result;
            }
            public void Dispose()
            {
                Connection.Disconnect();
                bool stopped = SpinWait.SpinUntil(() => !Connection.HasConnectionThread, TimeSpan.FromSeconds(5));
                m_Peer?.Close();
                m_Listener.Stop();
                UnityEngine.Object.DestroyImmediate(Object);
                UnityEngine.Object.DestroyImmediate(m_Config);
                Assert.IsTrue(stopped);
            }
        }

        [TestCase("publication")]
        [TestCase("request")]
        [TestCase("response")]
        public void OlderSendCannotRecycleDuringNewNotification(string kind)
        {
            using (var live = new LiveConnection())
            using (var entered = new ManualResetEventSlim(false))
            using (var release = new ManualResetEventSlim(false))
            using (var wire = new HeldWriteStream(true))
            {
                var source = new UInt8MultiArrayMsg();
                var pool = new RecordingPool();
                ROSConnection connection = live.Connection;
                if (kind == "publication") connection.RegisterPublisher<UInt8MultiArrayMsg>("/overlap");
                else if (kind == "request") connection.RegisterRosService<UInt8MultiArrayMsg, UInt8MultiArrayMsg>("/overlap");
                else connection.ImplementService<UInt8MultiArrayMsg, UInt8MultiArrayMsg>("/overlap", _ => source);
                RosTopicState topic = connection.GetTopic("/overlap");
                topic.SetMessagePool(pool);
                (kind == "response" ? topic.ServiceResponseTopic : topic).AddSubscriber(_ =>
                {
                    entered.Set();
                    Assert.IsTrue(release.Wait(TimeSpan.FromSeconds(5)));
                });
                live.Connect();
                object attempt = live.Attempt;
                var olderChannel = new TopicMessageSender("/older", source.RosMessageName, 1);
                olderChannel.SetMessagePool(pool);
                OutgoingMessageSender older = Prepare(olderChannel, source, Control(SysCommand.k_SysCommand_ServiceRequest, 990));
                Task writer = Task.Run(() => Assert.Throws<IOException>(() => older.SendInternal(new MessageSerializer(), wire)));
                Assert.IsTrue(wire.Entered.Wait(TimeSpan.FromSeconds(3)));
                int offersWhileNotifying = -1;
                Task retire = Task.Run(() =>
                {
                    try
                    {
                        Assert.IsTrue(entered.Wait(TimeSpan.FromSeconds(3)));
                        wire.Release.Set();
                        Assert.IsTrue(writer.Wait(TimeSpan.FromSeconds(3)));
                        RecyclerFence();
                        offersWhileNotifying = Volatile.Read(ref pool.Offers);
                        connection.Disconnect();
                    }
                    finally { release.Set(); }
                });
                try
                {
                    if (kind == "publication") Assert.Throws<IOException>(() => connection.Publish("/overlap", source));
                    else if (kind == "request")
                    {
                        Task<UInt8MultiArrayMsg> call = connection.SendServiceMessage<UInt8MultiArrayMsg>("/overlap", source);
                        Assert.IsTrue(call.IsFaulted);
                        Assert.IsInstanceOf<IOException>(call.Exception.InnerException);
                    }
                    else typeof(RosTopicState).GetMethod("HandleUnityServiceRequest", BindingFlags.Instance | BindingFlags.NonPublic)
                        .Invoke(topic, new object[] { Serialize(new UInt8MultiArrayMsg()), 992, attempt });
                    Assert.IsTrue(retire.Wait(TimeSpan.FromSeconds(3)));
                    Assert.AreEqual(0, offersWhileNotifying, "Notification is a live use before new queue admission, even across senders.");
                    RecyclerFence();
                    Assert.AreEqual(0, Volatile.Read(ref pool.Offers),
                        "An aborted notification borrow suppresses return for the overlapping generation, even after all accepted uses end.");
                }
                finally
                {
                    wire.Release.Set();
                    release.Set();
                    Assert.IsTrue(writer.Wait(TimeSpan.FromSeconds(3)));
                    Assert.IsTrue(retire.Wait(TimeSpan.FromSeconds(3)));
                    older.ClearAllQueuedData();
                }
            }
        }

        [Test]
        public void IncomingObserverBatchRetainsDecodedRootUntilFinalCallbackReturns()
        {
            var gameObject = new GameObject("incoming observer batch lifetime");
            var pool = new RecordingPool();
            using (var secondEntered = new ManualResetEventSlim(false))
            using (var releaseSecond = new ManualResetEventSlim(false))
            {
                ROSConnection connection = null;
                try
                {
                    connection = gameObject.AddComponent<ROSConnection>();
                    connection.ConnectOnStart = false;
                    connection.listenForTFMessages = false;
                    AttemptTestAccess.Replace(connection, 601L);

                    RosTopicState output = connection.RegisterPublisher<UInt8MultiArrayMsg>("/incoming-republish", 1);
                    output.SetMessagePool(pool);
                    RosTopicState input = connection.GetOrCreateTopic("/incoming-borrow",
                        UInt8MultiArrayMsg.k_RosMessageName);
                    input.AddSubscriber(message => connection.Publish("/incoming-republish", message));
                    input.AddSubscriber(_ =>
                    {
                        secondEntered.Set();
                        try
                        {
                            connection.Disconnect();
                            Assert.IsFalse(SpinWait.SpinUntil(() => pool.Offered.IsSet,
                                    TimeSpan.FromSeconds(1)),
                                "The decoded root was offered before the final incoming observer returned.");
                        }
                        finally { releaseSecond.Set(); }
                    });

                    object attempt = AttemptTestAccess.Current(connection);
                    Type frameType = typeof(ROSConnection).GetNestedType("IncomingMessage", BindingFlags.NonPublic);
                    object frame = Activator.CreateInstance(frameType,
                        BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null,
                        new object[] { attempt, "/incoming-borrow", Serialize(new UInt8MultiArrayMsg()) }, null);
                    typeof(ROSConnection).GetMethod("DispatchIncomingMessage",
                        BindingFlags.Instance | BindingFlags.NonPublic).Invoke(connection, new[] { frame });
                    Assert.IsTrue(secondEntered.IsSet, "The complete incoming observer batch must run.");
                    Assert.IsTrue(releaseSecond.IsSet);
                    Assert.AreEqual(0, Volatile.Read(ref pool.Offers));
                }
                finally
                {
                    releaseSecond.Set();
                    if (connection != null)
                    {
                        try { connection.Disconnect(); } catch (Exception) { }
                    }
                    UnityEngine.Object.DestroyImmediate(gameObject);
                }
            }
        }

        [Test]
        public void SubscriberCallbackPermitRejectsUnsubscribeAfterSnapshot()
        {
            var gameObject = new GameObject("subscriber callback permit authority");
            int deliveries = 0;
            ROSConnection connection = null;
            try
            {
                connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                connection.listenForTFMessages = false;
                AttemptTestAccess.Replace(connection, 701L);
                RosTopicState topic = connection.GetOrCreateTopic(
                    "/callback-permit-subscriber", Slice7CallbackGateMessage.k_RosMessageName);
                topic.AddSubscriber(_ => Interlocked.Increment(ref deliveries));
                Slice7CallbackGateMessage.DeserializeEntered = topic.UnsubscribeAll;
                MessageRegistry.Register<Slice7CallbackGateMessage>(
                    Slice7CallbackGateMessage.k_RosMessageName,
                    Slice7CallbackGateMessage.Deserialize);
                Assert.NotNull(MessageRegistry.GetDeserializeFunction(
                    Slice7CallbackGateMessage.k_RosMessageName),
                    "The callback-gate decoder must be registered before delivery.");

                object attempt = AttemptTestAccess.Current(connection);
                MethodInfo receive = typeof(RosTopicState).GetMethod(
                    "OnMessageReceivedForAttempt", BindingFlags.Instance | BindingFlags.NonPublic);
                receive.Invoke(topic, new object[] { new byte[0], attempt });

                Assert.AreEqual(0, Volatile.Read(ref deliveries),
                    "UnsubscribeAll must revoke a snapshotted callback before its permit is acquired.");
            }
            finally
            {
                Slice7CallbackGateMessage.DeserializeEntered = null;
                if (connection != null)
                {
                    try { connection.Disconnect(); } catch (Exception) { }
                }
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void ServiceResponseObserverBatchRetainsDecodedRootUntilFinalCallbackReturns()
        {
            var gameObject = new GameObject("service response observer batch lifetime");
            using (var pool = new HeldPool())
            {
                ROSConnection connection = null;
                bool offeredBeforeFinalCallback = false;
                try
                {
                    connection = gameObject.AddComponent<ROSConnection>();
                    connection.ConnectOnStart = false;
                    connection.listenForTFMessages = false;
                    AttemptTestAccess.Replace(connection, 603L);

                    RosTopicState output = connection.RegisterPublisher<UInt8MultiArrayMsg>(
                        "/service-response-republish", 1);
                    output.SetMessagePool(pool);
                    MessageRegistry.Register<Slice7ServiceResponse>(Slice7ServiceResponse.k_RosMessageName,
                        Slice7ServiceResponse.Deserialize, MessageSubtopic.Response);
                    connection.RegisterRosService<UInt8MultiArrayMsg, Slice7ServiceResponse>(
                        "/service-response-borrow");
                    RosTopicState service = connection.GetTopic("/service-response-borrow");
                    RosTopicState response = service.ServiceResponseTopic;
                    response.AddSubscriber(message => connection.Publish("/service-response-republish", message));
                    response.AddSubscriber(_ =>
                    {
                        connection.Disconnect();
                        offeredBeforeFinalCallback = pool.Entered.Wait(TimeSpan.FromSeconds(1));
                    });

                    object snapshot = typeof(RosTopicState).GetMethod("CaptureServiceResponseSnapshot",
                        BindingFlags.Instance | BindingFlags.NonPublic).Invoke(service, null);
                    MethodInfo deliver = typeof(RosTopicState).GetMethod("OnServiceResponseForAttempt",
                        BindingFlags.Instance | BindingFlags.NonPublic);
                    deliver.Invoke(response, new object[] { Serialize(new Slice7ServiceResponse()),
                        AttemptTestAccess.Current(connection), snapshot });

                    Assert.IsFalse(offeredBeforeFinalCallback,
                        "The decoded service response must remain borrowed through the complete observer batch.");
                    Assert.AreEqual(0, Volatile.Read(ref pool.Offers),
                        "An incoming decoded root must not be offered to an observer's pooled republisher.");
                }
                finally
                {
                    pool.Release.Set();
                    if (connection != null)
                    {
                        try { connection.Disconnect(); } catch (Exception) { }
                    }
                    UnityEngine.Object.DestroyImmediate(gameObject);
                }
            }
        }

        [UnityTest]
        public IEnumerator UnityServiceAsyncRequestRetainsDecodedRootUntilCompletion()
        {
            var gameObject = new GameObject("async service request lifetime");
            var pool = new RecordingPool();
            using (var implementationEntered = new ManualResetEventSlim(false))
            using (var implementationFinished = new ManualResetEventSlim(false))
            {
                var allowImplementation = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                ROSConnection connection = null;
                try
                {
                    connection = gameObject.AddComponent<ROSConnection>();
                    connection.ConnectOnStart = false;
                    connection.listenForTFMessages = false;
                    AttemptTestAccess.Replace(connection, 602L);

                    RosTopicState republish = connection.RegisterPublisher<UInt8MultiArrayMsg>("/async-republish", 1);
                    republish.SetMessagePool(pool);
                    connection.ImplementService<UInt8MultiArrayMsg, UInt8MultiArrayMsg>("/async-service", async request =>
                    {
                        connection.Publish("/async-republish", request);
                        implementationEntered.Set();
                        try
                        {
                            await allowImplementation.Task.ConfigureAwait(false);
                            return new UInt8MultiArrayMsg();
                        }
                        finally { implementationFinished.Set(); }
                    });

                    int openingRecords = LedgerCount("s_Records");
                    RosTopicState topic = connection.GetTopic("/async-service");
                    object attempt = AttemptTestAccess.Current(connection);
                    MethodInfo handle = typeof(RosTopicState).GetMethod(
                        "HandleUnityServiceRequest", BindingFlags.Instance | BindingFlags.NonPublic);
                    Assert.DoesNotThrow(() => handle.Invoke(topic,
                        new object[] { Serialize(new UInt8MultiArrayMsg()), 993, attempt }));
                    Assert.IsTrue(implementationEntered.Wait(TimeSpan.FromSeconds(3)),
                        "The Unity service implementation must enter before its awaited completion.");

                    republish.MessageSender.ClearAllQueuedData();
                    RecyclerFence();
                    Assert.AreEqual(0, Volatile.Read(ref pool.Offers),
                        "An awaited Unity service must keep its decoded request from returning while the implementation is pending.");
                    Assert.Greater(LedgerCount("s_Records"), openingRecords,
                        "The decoded request borrow must remain live until the returned Task completes.");

                    allowImplementation.TrySetResult(true);
                    for (int frame = 0; frame < 20 && !implementationFinished.IsSet; ++frame)
                        yield return null;
                    Assert.IsTrue(implementationFinished.IsSet,
                        "The awaited Unity service implementation must complete.");

                    connection.Disconnect();
                    RecyclerFence();
                    for (int frame = 0; frame < 60 && LedgerCount("s_Records") != openingRecords; ++frame)
                        yield return null;
                    Assert.AreEqual(openingRecords, LedgerCount("s_Records"),
                        "The decoded request borrow must be released after service completion and teardown.");
                }
                finally
                {
                    allowImplementation.TrySetResult(true);
                    if (connection != null)
                    {
                        try { connection.Disconnect(); } catch (Exception) { }
                    }
                    UnityEngine.Object.DestroyImmediate(gameObject);
                }
            }
        }

        sealed class HeldPool : IMessagePool, IDisposable
        {
            internal readonly ManualResetEventSlim Entered = new ManualResetEventSlim(false);
            internal readonly ManualResetEventSlim Release = new ManualResetEventSlim(false);
            internal readonly ManualResetEventSlim Finished = new ManualResetEventSlim(false);
            internal int Offers;
            internal int ThreadId;
            internal bool IsThreadPoolThread;
            public void AddMessage(Message message)
            {
                Interlocked.Increment(ref Offers);
                ThreadId = Thread.CurrentThread.ManagedThreadId;
                IsThreadPoolThread = Thread.CurrentThread.IsThreadPoolThread;
                Entered.Set();
                try { Release.Wait(); }
                finally { Finished.Set(); }
            }
            public void Dispose()
            {
                Release.Set();
                if (Entered.IsSet) Assert.IsTrue(Finished.Wait(TimeSpan.FromSeconds(3)));
                RecyclerFence();
                Entered.Dispose();
                Release.Dispose();
                Finished.Dispose();
            }
        }

        static void OfferTo(IMessagePool pool, Message message)
        {
            var channel = new TopicMessageSender("/return", UInt8MultiArrayMsg.k_RosMessageName, 1);
            channel.SetMessagePool(pool);
            Prepare(channel, message, Control(SysCommand.k_SysCommand_ServiceRequest, 991)).ClearAllQueuedData();
        }

        [Test]
        public void RetirementOfferIsCancelledByNewUse()
        {
            using (var held = new HeldPool())
            {
                OfferTo(held, new UInt8MultiArrayMsg());
                Assert.IsTrue(held.Entered.Wait(TimeSpan.FromSeconds(3)));
                var source = new UInt8MultiArrayMsg();
                var pool = new RecordingPool();
                var channel = new TopicMessageSender("/retirement", source.RosMessageName, 1);
                channel.SetMessagePool(pool);
                try
                {
                    Queue(channel, source);
                    channel.ClearAllQueuedData(); // Offer is pending behind held, not Returning yet.
                    Queue(channel, source);       // Reacquisition must invalidate that queued offer.
                    held.Release.Set();
                    RecyclerFence();
                    Assert.AreEqual(0, Volatile.Read(ref pool.Offers), "An unclaimed old offer must not run through a new use.");
                    channel.ClearAllQueuedData();
                    RecyclerFence();
                    Assert.AreEqual(1, Volatile.Read(ref pool.Offers));
                }
                finally { channel.ClearAllQueuedData(); }
            }
        }

        [Serializable]
        sealed class AccessProbe : UInt8MultiArrayMsg, ISerializationCallbackReceiver
        {
            [NonSerialized] internal int NameReads;
            [NonSerialized] internal int Serializations;
            [NonSerialized] internal int JsonCallbacks;
            public override string RosMessageName { get { Interlocked.Increment(ref NameReads); return k_RosMessageName; } }
            public override void SerializeTo(MessageSerializer serializer)
            {
                Interlocked.Increment(ref Serializations);
                base.SerializeTo(serializer);
            }
            public void OnBeforeSerialize() { Interlocked.Increment(ref JsonCallbacks); }
            public void OnAfterDeserialize() { }
        }

        [TestCase("publication")]
        [TestCase("request")]
        [TestCase("response")]
        [TestCase("unregistered")]
        [TestCase("raw")]
        public void ReturningSourceIsRejectedBeforeVirtualAccess(string path)
        {
            using (var live = new LiveConnection())
            using (var held = new HeldPool())
            using (var finished = new ManualResetEventSlim(false))
            {
                var source = new AccessProbe();
                ROSConnection connection = live.Connection;
                if (path == "request") connection.RegisterRosService<UInt8MultiArrayMsg, UInt8MultiArrayMsg>("/handoff");
                else if (path == "response") connection.ImplementService<UInt8MultiArrayMsg, UInt8MultiArrayMsg>("/handoff", _ => source);
                else connection.RegisterPublisher<UInt8MultiArrayMsg>("/handoff");
                RosTopicState topic = connection.GetTopic("/handoff");
                topic.SetMessagePool(held);
                int notifications = 0;
                (path == "response" ? topic.ServiceResponseTopic : topic).AddSubscriber(_ => notifications++);
                live.Connect();
                OfferTo(held, source);
                Assert.IsTrue(held.Entered.Wait(TimeSpan.FromSeconds(3)));
                source.NameReads = source.Serializations = source.JsonCallbacks = 0;
                bool timely = false;
                Task watchdog = Task.Run(() =>
                {
                    timely = finished.Wait(TimeSpan.FromSeconds(3));
                    held.Release.Set();
                });
                Exception failure = null;
                try
                {
                    if (path == "request")
                        failure = connection.SendServiceMessage<UInt8MultiArrayMsg>("/handoff", source).Exception?.InnerException;
                    else if (path == "response")
                    {
                        LogAssert.Expect(LogType.Error, new Regex("Unity service '/handoff' response could not be queued: System.IO.IOException"));
                        typeof(RosTopicState).GetMethod("HandleUnityServiceRequest", BindingFlags.Instance | BindingFlags.NonPublic)
                            .Invoke(topic, new object[] { Serialize(new UInt8MultiArrayMsg()), 995, live.Attempt });
                    }
                    else
                    {
                        try { connection.Publish(path == "unregistered" ? "/absent" : path == "raw" ? "__handoff" : "/handoff", source); }
                        catch (Exception exception) { failure = exception; }
                    }
                }
                finally { finished.Set(); Assert.IsTrue(watchdog.Wait(TimeSpan.FromSeconds(5))); }
                Assert.IsTrue(timely, "A Returning rejection must not wait for the arbitrary pool callback.");
                Assert.AreEqual(0, source.NameReads + source.Serializations + source.JsonCallbacks + notifications,
                    "No virtual access, JSON callback or local observer can precede handoff rejection.");
                if (path != "response") Assert.IsInstanceOf<IOException>(failure);
                Assert.AreEqual(0, Capacity(topic.MessageSender));
            }
        }

        sealed class EnqueueThenHoldPool : IMessagePool, IDisposable
        {
            internal readonly MessagePool<UInt8MultiArrayMsg> BuiltIn = new MessagePool<UInt8MultiArrayMsg>();
            internal readonly HeldPool Barrier = new HeldPool();
            public void AddMessage(Message message)
            {
                // Real built-in enqueue has happened, but the connector's pool call
                // has not returned and therefore cannot finalize Returning yet.
                BuiltIn.AddMessage((UInt8MultiArrayMsg)message);
                Barrier.AddMessage(message);
            }
            public void Dispose() { Barrier.Dispose(); }
        }

        [Test]
        public void BuiltInCheckoutSkipsReturnStillInProgress()
        {
            using (var live = new LiveConnection())
            using (var pool = new EnqueueThenHoldPool())
            {
                live.Connection.RegisterPublisher<UInt8MultiArrayMsg>("/checkout");
                live.Connect();
                var source = new UInt8MultiArrayMsg();
                OfferTo(pool, source);
                Assert.IsTrue(pool.Barrier.Entered.Wait(TimeSpan.FromSeconds(3)));
                UInt8MultiArrayMsg checkedOut = pool.BuiltIn.GetOrCreateMessage();
                Assert.AreNotSame(source, checkedOut, "Dequeue must skip the root while registry handoff is still Returning.");
                Assert.DoesNotThrow(() => live.Connection.Publish("/checkout", checkedOut));
                pool.Barrier.Release.Set();
                RecyclerFence();
                pool.BuiltIn.AddMessage(source);
                Assert.AreSame(source, pool.BuiltIn.GetOrCreateMessage(), "Direct Add/Get after completed handoff is unchanged.");
            }
        }

        [TestCase("captured-ordinary")]
        [TestCase("captured-exact")]
        [TestCase("during-serialization")]
        [TestCase("across-senders")]
        [TestCase("across-connections")]
        [TestCase("no-pool")]
        [TestCase("queued-different-pool")]
        public void PoolBindingIsCapturedForEachOwnershipGeneration(string scenario)
        {
            using (var firstConnection = new LiveConnection())
            using (var secondConnection = new LiveConnection())
            using (var held = new HeldPool())
            {
                var poolA = new RecordingPool();
                var poolB = new RecordingPool();
                var source = new InvalidatingServiceMessage { DuringSerialization = () => { } };
                if (scenario == "across-connections")
                {
                    firstConnection.Connect();
                    secondConnection.Connect();
                }
                TopicMessageSender first = scenario == "across-connections"
                    ? firstConnection.Connection.RegisterPublisher<UInt8MultiArrayMsg>("/binding").MessageSender
                    : new TopicMessageSender("/binding", UInt8MultiArrayMsg.k_RosMessageName, 1);
                TopicMessageSender second = scenario == "across-connections"
                    ? secondConnection.Connection.RegisterPublisher<UInt8MultiArrayMsg>("/binding").MessageSender
                    : new TopicMessageSender("/other-binding", UInt8MultiArrayMsg.k_RosMessageName, 1);
                first.SetMessagePool(poolA);
                second.SetMessagePool(scenario == "no-pool" ? null : poolB);
                OutgoingMessageSender exact = null;
                try
                {
                    if (scenario == "captured-exact" || scenario == "during-serialization")
                    {
                        if (scenario == "during-serialization") source.DuringSerialization = () => first.SetMessagePool(poolB);
                        exact = Prepare(first, source, Control(SysCommand.k_SysCommand_ServiceRequest, 983));
                        first.SetMessagePool(poolB);
                        exact.ClearAllQueuedData();
                    }
                    else
                    {
                        if (scenario == "queued-different-pool")
                        {
                            OfferTo(held, new UInt8MultiArrayMsg());
                            Assert.IsTrue(held.Entered.Wait(TimeSpan.FromSeconds(3)));
                        }
                        Queue(first, source);
                        if (scenario == "captured-ordinary") first.SetMessagePool(poolB);
                        else
                        {
                            if (scenario == "queued-different-pool") first.ClearAllQueuedData();
                            Queue(second, source);
                        }
                        first.ClearAllQueuedData();
                        held.Release.Set();
                        RecyclerFence();
                        if (!scenario.StartsWith("captured"))
                            Assert.AreEqual(0, Volatile.Read(ref poolA.Offers), "Other sender/connection still retains the reference.");
                        second.ClearAllQueuedData();
                    }
                    RecyclerFence();
                    int expectedA = scenario.StartsWith("captured") || scenario == "during-serialization" ? 1 : 0;
                    Assert.AreEqual(expectedA, Volatile.Read(ref poolA.Offers));
                    Assert.AreEqual(0, Volatile.Read(ref poolB.Offers), "A later pool cannot redirect an earlier generation's return.");
                }
                finally
                {
                    exact?.ClearAllQueuedData();
                    first.ClearAllQueuedData();
                    second.ClearAllQueuedData();
                }
            }
        }

        sealed class PoisonPool : IMessagePool
        {
            internal int Offers;
            public void AddMessage(Message message)
            {
                var array = (UInt8MultiArrayMsg)message;
                for (int index = 0; index < array.data.Length; index++) array.data[index] = 222;
                Interlocked.Increment(ref Offers);
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public void LatchReplayFailurePreservesOriginalWireSnapshot(bool pooled)
        {
            var source = new AccessProbe { data = new byte[] { 17, 23, 31 } };
            var pool = new PoisonPool();
            var channel = new TopicMessageSender("/latched", UInt8MultiArrayMsg.k_RosMessageName, 1);
            if (pooled) channel.SetMessagePool(pool);
            try
            {
                Queue(channel, source);
                byte[] original;
                using (var initial = new MemoryStream())
                {
                    Assert.AreEqual(OutgoingMessageSender.SendToState.Normal, channel.SendInternal(new MessageSerializer(), initial));
                    original = initial.ToArray();
                }
                if (!pooled) source.data[0] = 199; // Legal caller mutation cannot change replay wire bytes.
                channel.PrepareLatchMessage();
                Assert.IsTrue(channel.PeekNextMessageToSend(out Message peek));
                Assert.AreSame(source, peek, "Peek retains the original borrowed source reference.");
                using (var failed = new PartialWriteStream(1))
                    Assert.Throws<IOException>(() => channel.SendInternal(new MessageSerializer(), failed));
                RecyclerFence();
                Assert.AreEqual(0, Volatile.Read(ref pool.Offers), "A failed replay releases its own handle, not the cache.");
                channel.PrepareLatchMessage();
                using (var replay = new MemoryStream())
                {
                    Assert.AreEqual(OutgoingMessageSender.SendToState.Normal, channel.SendInternal(new MessageSerializer(), replay));
                    CollectionAssert.AreEqual(original, replay.ToArray(), "Replay owns bytes, not another invocation of a mutable source.");
                }
                Assert.AreEqual(1, source.Serializations, "Replay must never execute the source serializer again.");
                channel.SetMessagePool(null);
                Queue(channel, new UInt8MultiArrayMsg());
                channel.SendInternal(new MessageSerializer(), Stream.Null);
                RecyclerFence();
                Assert.AreEqual(pooled ? 1 : 0, Volatile.Read(ref pool.Offers));
            }
            finally
            {
                channel.ClearAllQueuedData();
                (channel as IDisposable)?.Dispose();
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public void CachedAndReplayHandlesOutliveDisplacementIndependently(bool failReplay)
        {
            var source = new UInt8MultiArrayMsg(new MultiArrayLayoutMsg(), new byte[] { 7 });
            var replacement = new UInt8MultiArrayMsg(new MultiArrayLayoutMsg(), new byte[] { 9 });
            var pool = new RecordingPool();
            var channel = new TopicMessageSender("/cache-handles", source.RosMessageName, 2);
            channel.SetMessagePool(pool);
            Queue(channel, source);
            channel.SendInternal(new MessageSerializer(), Stream.Null);
            channel.PrepareLatchMessage();
            using (var wire = new HeldWriteStream(failReplay))
            {
                Task<Exception> writer = Task.Run(() =>
                {
                    try { channel.SendInternal(new MessageSerializer(), wire); return (Exception)null; }
                    catch (Exception exception) { return exception; }
                });
                try
                {
                    Assert.IsTrue(wire.Entered.Wait(TimeSpan.FromSeconds(3)));
                    Queue(channel, replacement);
                    channel.SendInternal(new MessageSerializer(), Stream.Null);
                    RecyclerFence();
                    Assert.AreEqual(0, Volatile.Read(ref pool.Offers), "Displacement releases cache ownership, not a held replay.");
                }
                finally { wire.Release.Set(); Assert.IsTrue(writer.Wait(TimeSpan.FromSeconds(3))); }
                Assert.AreSame(wire.Failure, writer.Result);
                RecyclerFence();
                Assert.AreEqual(1, Volatile.Read(ref pool.Offers));
                Assert.AreSame(source, Volatile.Read(ref pool.Message), "A late successful replay cannot displace newer cache data.");
                channel.PrepareLatchMessage();
                Assert.IsTrue(channel.PeekNextMessageToSend(out Message peek));
                Assert.AreSame(replacement, peek);
                channel.ClearAllQueuedData();
                RecyclerFence();
                Assert.AreEqual(1, Volatile.Read(ref pool.Offers), "Queue clear must retain the compatible cache.");
                channel.PrepareLatchMessage();
                Assert.IsInstanceOf<IDisposable>(channel, "Standalone senders need terminal cache/source ownership release.");
                ((IDisposable)channel).Dispose();
                ((IDisposable)channel).Dispose();
                channel.PrepareLatchMessage();
                Assert.IsFalse(channel.PeekNextMessageToSend(out _));
                RecyclerFence();
                Assert.AreEqual(2, Volatile.Read(ref pool.Offers), "Disposal releases queued/cache handles exactly once.");
                Assert.AreSame(replacement, Volatile.Read(ref pool.Message));
            }
            (channel as IDisposable)?.Dispose();
        }

        sealed class HostileIdentityMessage : UInt8MultiArrayMsg
        {
            internal int IdentityCalls;
            public override bool Equals(object obj) { IdentityCalls++; throw new InvalidOperationException("Message.Equals must not run"); }
            public override int GetHashCode() { IdentityCalls++; throw new InvalidOperationException("Message.GetHashCode must not run"); }
        }

        [Test]
        public void MessageIdentityNeverExecutesOverrides()
        {
            var source = new HostileIdentityMessage();
            var pool = new RecordingPool();
            using (var channel = new TopicMessageSender("/identity", UInt8MultiArrayMsg.k_RosMessageName, 1))
            {
                channel.SetMessagePool(pool);
                Queue(channel, source);
                Queue(channel, source);
                Assert.AreEqual(OutgoingMessageSender.SendToState.QueueFullWarning, channel.SendInternal(new MessageSerializer(), Stream.Null));
                channel.SendInternal(new MessageSerializer(), Stream.Null);
                channel.PrepareLatchMessage();
                channel.SendInternal(new MessageSerializer(), Stream.Null);
                Prepare(channel, source, Control(SysCommand.k_SysCommand_ServiceResponse, 981)).ClearAllQueuedData();
                channel.ClearAllQueuedData();
                RecyclerFence();
                Assert.AreEqual(0, Volatile.Read(ref pool.Offers));
            }
            RecyclerFence();
            Assert.AreEqual(1, Volatile.Read(ref pool.Offers));
            Assert.AreEqual(0, source.IdentityCalls);
        }

        [TestCase(false)]
        [TestCase(true)]
        public void ConnectionDestructionReleasesCacheWithoutStealingClaimedReplay(bool failReplay)
        {
            using (var live = new LiveConnection())
            using (var wire = new HeldWriteStream(failReplay))
            {
                var source = new UInt8MultiArrayMsg();
                var pool = new RecordingPool();
                TopicMessageSender channel = live.Connection.RegisterPublisher<UInt8MultiArrayMsg>("/destroy-cache").MessageSender;
                channel.SetMessagePool(pool);
                live.Connect();
                Queue(channel, source);
                channel.SendInternal(new MessageSerializer(), Stream.Null);
                channel.PrepareLatchMessage();
                Task<Exception> writer = Task.Run(() =>
                {
                    try { channel.SendInternal(new MessageSerializer(), wire); return (Exception)null; }
                    catch (Exception exception) { return exception; }
                });
                try
                {
                    Assert.IsTrue(wire.Entered.Wait(TimeSpan.FromSeconds(3)));
                    UnityEngine.Object.DestroyImmediate(live.Object);
                    RecyclerFence();
                    Assert.AreEqual(0, Volatile.Read(ref pool.Offers), "Destroy cannot steal a claimed replay handle.");
                    wire.Release.Set();
                    Assert.IsTrue(writer.Wait(TimeSpan.FromSeconds(3)));
                    Assert.AreSame(wire.Failure, writer.Result);
                    RecyclerFence();
                    Assert.AreEqual(1, Volatile.Read(ref pool.Offers), "Destroy must retire the cache, including after a late successful write.");
                    channel.PrepareLatchMessage();
                    Assert.IsFalse(channel.PeekNextMessageToSend(out _));
                }
                finally
                {
                    wire.Release.Set();
                    Assert.IsTrue(writer.Wait(TimeSpan.FromSeconds(3)));
                    channel.Dispose();
                    RecyclerFence();
                }
            }
        }

        [TestCase("ros-service")]
        [TestCase("sync-service")]
        [TestCase("async-service")]
        public void SenderReplacementRetiresDisplacedCache(string role)
        {
            using (var live = new LiveConnection())
            {
                var source = new UInt8MultiArrayMsg();
                Action configure = () =>
                {
                    if (role == "ros-service")
                        live.Connection.RegisterRosService<UInt8MultiArrayMsg, UInt8MultiArrayMsg>("/replace-cache");
                    else if (role == "sync-service")
                        live.Connection.ImplementService<UInt8MultiArrayMsg, UInt8MultiArrayMsg>("/replace-cache", _ => source);
                    else
                        live.Connection.ImplementService<UInt8MultiArrayMsg, UInt8MultiArrayMsg>("/replace-cache", _ => Task.FromResult(source));
                };
                configure();
                TopicMessageSender old = live.Connection.GetTopic("/replace-cache").MessageSender;
                var pool = new RecordingPool();
                old.SetMessagePool(pool);
                live.Connect();
                try
                {
                    Queue(old, source);
                    old.SendInternal(new MessageSerializer(), Stream.Null);
                    Queue(old, source);
                    configure();
                    Assert.AreNotSame(old, live.Connection.GetTopic("/replace-cache").MessageSender);
                    RecyclerFence();
                    Assert.AreEqual(1, Volatile.Read(ref pool.Offers), "Displaced queued/cache handles must retire once outside topic locks.");
                    old.PrepareLatchMessage();
                    Assert.IsFalse(old.PeekNextMessageToSend(out _));
                    Assert.Throws<ObjectDisposedException>(() => Prepare(old, new UInt8MultiArrayMsg(),
                        Control(SysCommand.k_SysCommand_ServiceRequest, 977)));
                }
                finally { old.Dispose(); RecyclerFence(); }
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public void RetiredAttemptWriteCannotCommitLatch(bool priorCache)
        {
            using (var live = new LiveConnection())
            using (var wire = new HeldWriteStream(false))
            {
                TopicMessageSender channel = live.Connection.RegisterPublisher<UInt8MultiArrayMsg>("/late-cache").MessageSender;
                var pool = new RecordingPool();
                channel.SetMessagePool(pool);
                live.Connect();
                object attempt = live.Attempt;
                var good = new UInt8MultiArrayMsg();
                if (priorCache)
                {
                    Queue(channel, good);
                    channel.SendInternal(new MessageSerializer(), Stream.Null);
                }
                var late = new UInt8MultiArrayMsg();
                Queue(channel, late);
                Task writer = Task.Run(() => channel.SendInternal(new MessageSerializer(), wire));
                try
                {
                    Assert.IsTrue(wire.Entered.Wait(TimeSpan.FromSeconds(3)));
                    live.Connection.Disconnect();
                    Assert.IsTrue(SpinWait.SpinUntil(() => !live.Connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                    live.Connect();
                    Assert.AreNotSame(attempt, live.Attempt);
                    wire.Release.Set();
                    Assert.IsTrue(writer.Wait(TimeSpan.FromSeconds(3)));
                    channel.PrepareLatchMessage();
                    Assert.AreEqual(priorCache, channel.PeekNextMessageToSend(out Message peek),
                        "An A writer completing after revocation cannot create or replace B's cache.");
                    if (priorCache) Assert.AreSame(good, peek);
                    RecyclerFence();
                    Assert.AreEqual(1, Volatile.Read(ref pool.Offers));
                    Assert.AreSame(late, Volatile.Read(ref pool.Message));
                }
                finally
                {
                    wire.Release.Set();
                    Assert.IsTrue(writer.Wait(TimeSpan.FromSeconds(3)));
                    channel.Dispose();
                    RecyclerFence();
                }
            }
        }

        [Test]
        public void RetiredOrdinaryPublicationCannotCommitCacheAfterDisconnectWins()
        {
            using (var live = new LiveConnection())
            using (var checkPassed = new ManualResetEventSlim(false))
            using (var release = new ManualResetEventSlim(false))
            {
                TopicMessageSender channel = live.Connection.RegisterPublisher<UInt8MultiArrayMsg>(
                    "/retired-cache-barrier", latch: true).MessageSender;
                var source = new UInt8MultiArrayMsg();
                var pool = new RecordingPool();
                channel.SetMessagePool(pool);
                live.Connect();
                Queue(channel, source);

                FieldInfo hook = typeof(ROSConnection).GetField("m_BeforeSenderCacheCommitTestHook",
                    BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.NotNull(hook, "The cache commit seam must expose a barrier after authority validation.");
                hook.SetValue(live.Connection, (Action)(() =>
                {
                    checkPassed.Set();
                    release.Wait(TimeSpan.FromSeconds(5));
                }));

                Task writer = null;
                try
                {
                    writer = Task.Run(() => channel.SendInternal(new MessageSerializer(), Stream.Null));
                    Assert.IsTrue(checkPassed.Wait(TimeSpan.FromSeconds(3)),
                        "The writer must pause after the current-attempt check and before cache mutation.");

                    Task disconnect = Task.Run(() => live.Connection.Disconnect());
                    Assert.IsTrue(disconnect.Wait(TimeSpan.FromSeconds(3)),
                        "Disconnect must win while the retired writer is paused at cache commit.");
                    Assert.IsFalse(disconnect.IsFaulted);
                    Assert.IsTrue(SpinWait.SpinUntil(() => !live.Connection.HasConnectionThread,
                        TimeSpan.FromSeconds(3)));

                    release.Set();
                    Assert.IsTrue(writer.Wait(TimeSpan.FromSeconds(3)));
                    Assert.IsFalse(writer.IsFaulted);

                    FieldInfo cache = typeof(TopicMessageSender).GetField("m_LastMessageSent",
                        BindingFlags.Instance | BindingFlags.NonPublic);
                    Assert.IsNull(cache.GetValue(channel),
                        "A cache commit that loses to Disconnect must not install retired-epoch latch bytes.");
                    RecyclerFence();
                    Assert.AreEqual(1, Volatile.Read(ref pool.Offers),
                        "The retired publication must release its source without a retained cache handle.");
                    Assert.AreSame(source, Volatile.Read(ref pool.Message));
                }
                finally
                {
                    release.Set();
                    if (writer != null) writer.Wait(TimeSpan.FromSeconds(3));
                    hook.SetValue(live.Connection, null);
                    channel.Dispose();
                    RecyclerFence();
                }
            }
        }

        [Test]
        public void LatchReplayScheduledAtMostOncePerAttempt()
        {
            using (var live = new LiveConnection())
            {
                live.Connect();
                live.ReadThrough(SysCommand.k_SysCommand_TopicList);
                RosTopicState topic = live.Connection.RegisterPublisher<UInt8MultiArrayMsg>("/once-latch", latch: true);
                var source = new UInt8MultiArrayMsg(new MultiArrayLayoutMsg(), new byte[] { 53 });
                live.Connection.Publish("/once-latch", source);
                live.Connection.QueueSysCommand("__first_latch", new SysCommand_TopicsRequest());
                Assert.AreEqual(1, live.ReadThrough("__first_latch").FindAll(frame => frame.Item1 == "/once-latch").Count);
                live.Connection.Disconnect();
                Assert.IsTrue(SpinWait.SpinUntil(() => !live.Connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                live.Connect();
                var firstReplay = live.ReadThrough("/once-latch").FindAll(frame => frame.Item1 == "/once-latch");
                Assert.AreEqual(1, firstReplay.Count);
                CollectionAssert.AreEqual(Serialize(source), firstReplay[0].Item2);
                var client = (TcpClient)AttemptTestAccess.Root(live.Connection, "Client");
                typeof(RosTopicState).GetMethod("OnConnectionEstablished", BindingFlags.Instance | BindingFlags.NonPublic)
                    .Invoke(topic, new object[] { client.GetStream(), live.Attempt });
                live.Connection.QueueSysCommand("__repeat_latch", new SysCommand_TopicsRequest());
                Assert.AreEqual(0, live.ReadThrough("__repeat_latch").FindAll(frame => frame.Item1 == "/once-latch").Count,
                    "Establishment/reconciliation cannot schedule the same latch twice on one attempt.");
            }
        }

        [Test]
        public void SchemaChangeRetiresCacheWithoutRevivingClaimedSource()
        {
            using (var live = new LiveConnection())
            using (var wire = new HeldWriteStream(false))
            {
                RosTopicState topic = live.Connection.RegisterPublisher<UInt8MultiArrayMsg>("/schema-cache");
                TopicMessageSender channel = topic.MessageSender;
                var source = new UInt8MultiArrayMsg();
                var pool = new RecordingPool();
                channel.SetMessagePool(pool);
                live.Connect();
                Queue(channel, source);
                channel.SendInternal(new MessageSerializer(), Stream.Null);
                channel.PrepareLatchMessage();
                Task writer = Task.Run(() => channel.SendInternal(new MessageSerializer(), wire));
                try
                {
                    Assert.IsTrue(wire.Entered.Wait(TimeSpan.FromSeconds(3)));
                    // Public discovery rejects a conflicting active schema. Exercise the
                    // internal cache-retirement seam directly after the public guard.
                    LogAssert.Expect(LogType.Warning, new Regex("Inconsistent declaration of topic '/schema-cache'"));
                    typeof(RosTopicState).GetMethod("ChangeRosMessageName",
                        BindingFlags.Instance | BindingFlags.NonPublic)
                        .Invoke(topic, new object[] { StringMsg.k_RosMessageName });
                    RecyclerFence();
                    Assert.AreEqual(0, Volatile.Read(ref pool.Offers), "The old replay remains a claimed source use.");
                    wire.Release.Set();
                    Assert.IsTrue(writer.Wait(TimeSpan.FromSeconds(3)));
                    channel.PrepareLatchMessage();
                    Assert.IsFalse(channel.PeekNextMessageToSend(out _), "An old-schema completion cannot resurrect incompatible cache bytes.");
                    RecyclerFence();
                    Assert.AreEqual(1, Volatile.Read(ref pool.Offers));
                }
                finally
                {
                    wire.Release.Set();
                    Assert.IsTrue(writer.Wait(TimeSpan.FromSeconds(3)));
                    channel.Dispose();
                    RecyclerFence();
                }
            }
        }

        [TestCase("empty")]
        [TestCase("fresh")]
        [TestCase("stale-attempt")]
        public void LatchAdmissionSkipsUnavailableOrIneligibleReplay(string scenario)
        {
            using (var live = new LiveConnection())
            {
                TopicMessageSender channel = live.Connection.RegisterPublisher<UInt8MultiArrayMsg>("/latch-admission").MessageSender;
                live.Connect();
                object attempt = live.Attempt;
                var cached = new UInt8MultiArrayMsg();
                var fresh = new UInt8MultiArrayMsg();
                if (scenario != "empty")
                {
                    Queue(channel, cached);
                    channel.SendInternal(new MessageSerializer(), Stream.Null);
                }
                if (scenario == "fresh") Queue(channel, fresh);
                if (scenario == "stale-attempt")
                {
                    live.Connection.Disconnect();
                    Assert.IsTrue(SpinWait.SpinUntil(() => !live.Connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                    live.Connect();
                    Assert.AreNotSame(attempt, live.Attempt);
                }
                MethodInfo queueLatch = typeof(ROSConnection).GetMethod("TryQueueLatch", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.IsFalse((bool)queueLatch.Invoke(live.Connection, new object[] { channel, attempt }));
                Assert.AreEqual(scenario == "fresh", channel.PeekNextMessageToSend(out Message peek));
                if (scenario == "fresh") Assert.AreSame(fresh, peek);
                if (scenario == "stale-attempt")
                {
                    Assert.IsTrue((bool)queueLatch.Invoke(live.Connection, new object[] { channel, live.Attempt }),
                        "Rejecting A must not consume B's one replay opportunity.");
                    var frames = live.ReadThrough("/latch-admission");
                    CollectionAssert.AreEqual(Serialize(cached), frames[frames.Count - 1].Item2);
                }
                else
                {
                    channel.ClearAllQueuedData();
                    Queue(channel, fresh);
                    channel.SendInternal(new MessageSerializer(), Stream.Null);
                    Assert.IsFalse((bool)queueLatch.Invoke(live.Connection, new object[] { channel, attempt }),
                        "A skipped replay is not retried later after fresh data has already been sent.");
                }
            }
        }

        [Test]
        public void RequestCapacitySurvivesMissingResponseWithoutUnboundedGrowth()
        {
            using (var live = new LiveConnection())
            {
                live.Connect();
                live.Connection.RegisterRosService("/pending-capacity", UInt8MultiArrayMsg.k_RosMessageName,
                    UInt8MultiArrayMsg.k_RosMessageName, 2);
                Task<UInt8MultiArrayMsg> first = live.Connection.SendServiceMessage<UInt8MultiArrayMsg>(
                    "/pending-capacity", new UInt8MultiArrayMsg());
                Task<UInt8MultiArrayMsg> second = live.Connection.SendServiceMessage<UInt8MultiArrayMsg>(
                    "/pending-capacity", new UInt8MultiArrayMsg());
                live.Connection.QueueSysCommand("__pending-capacity-fence", new SysCommand_TopicsRequest());
                List<Tuple<string, byte[]>> frames = live.ReadThrough("__pending-capacity-fence");
                Assert.AreEqual(2, frames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_ServiceRequest).Count,
                    "Both requests must complete their wire writes before the unanswered-call limit is tested.");
                Assert.IsFalse(first.IsCompleted);
                Assert.IsFalse(second.IsCompleted);

                Task<UInt8MultiArrayMsg> overflow = live.Connection.SendServiceMessage<UInt8MultiArrayMsg>(
                    "/pending-capacity", new UInt8MultiArrayMsg());
                Assert.IsTrue(overflow.IsFaulted, "Pending-call capacity is separate from freed wire capacity.");
                Assert.IsInstanceOf<IOException>(overflow.Exception.InnerException);

                live.Connection.Disconnect();
                Assert.IsTrue(SpinWait.SpinUntil(() => !live.Connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                Assert.IsTrue(first.IsFaulted);
                Assert.IsTrue(second.IsFaulted);
                Assert.IsInstanceOf<IOException>(first.Exception.InnerException);
                Assert.IsInstanceOf<IOException>(second.Exception.InnerException);
            }
        }

        [Test]
        public void PublishPreparationFailureRollsBackAndAllowsNextPublication()
        {
            using (var live = new LiveConnection())
            {
                RosTopicState topic = live.Connection.RegisterPublisher<UInt8MultiArrayMsg>("/publish-preparation", 1);
                live.Connect();
                var failure = new InvalidOperationException("controlled publish notification failure");
                topic.AddSubscriber(_ => throw failure);

                Assert.Throws<InvalidOperationException>(() => live.Connection.Publish(
                    "/publish-preparation", new UInt8MultiArrayMsg()));
                Assert.AreEqual(0, Capacity(topic.MessageSender),
                    "A notification exception must release the ordinary preparation slot.");

                topic.UnsubscribeAll();
                var valid = new UInt8MultiArrayMsg(new MultiArrayLayoutMsg(), new byte[] { 31, 47 });
                live.Connection.Publish("/publish-preparation", valid);
                live.Connection.QueueSysCommand("__publish-preparation-fence", new SysCommand_TopicsRequest());
                List<Tuple<string, byte[]>> frames = live.ReadThrough("__publish-preparation-fence");
                Tuple<string, byte[]> publication = frames.Find(frame => frame.Item1 == "/publish-preparation");
                Assert.IsNotNull(publication, "A later valid publication must not be blocked by the failed preparation.");
                CollectionAssert.AreEqual(Serialize(valid), publication.Item2);
            }
        }

        [Test]
        public void PublishCallbackDisconnectAbortsLaterCallbacksAndCommit()
        {
            using (var live = new LiveConnection())
            {
                RosTopicState topic = live.Connection.RegisterPublisher<UInt8MultiArrayMsg>("/reentrant-disconnect", 1);
                live.Connect();
                int firstCallbacks = 0;
                int laterCallbacks = 0;
                topic.AddSubscriber(_ =>
                {
                    firstCallbacks++;
                    live.Connection.Disconnect();
                });
                topic.AddSubscriber(_ => laterCallbacks++);

                Assert.Throws<IOException>(() => live.Connection.Publish(
                    "/reentrant-disconnect", new UInt8MultiArrayMsg()));
                Assert.AreEqual(1, firstCallbacks);
                Assert.AreEqual(0, laterCallbacks,
                    "A callback invalidated by Disconnect must not receive the copied callback batch.");
                Assert.AreEqual(0, Capacity(topic.MessageSender),
                    "Disconnect from a notification must not leak the outer preparation slot.");
            }
        }

        [Test]
        public void NestedPublishOfSameMessageRetainsBothOwnershipUses()
        {
            using (var live = new LiveConnection())
            {
                RosTopicState topic = live.Connection.RegisterPublisher<UInt8MultiArrayMsg>("/nested-publish", 4);
                live.Connect();
                var source = new UInt8MultiArrayMsg(new MultiArrayLayoutMsg(), new byte[] { 7, 13 });
                bool nested = false;
                topic.AddSubscriber(_ =>
                {
                    if (!nested)
                    {
                        nested = true;
                        live.Connection.Publish("/nested-publish", source);
                    }
                });

                live.Connection.Publish("/nested-publish", source);
                live.Connection.QueueSysCommand("__nested-publish-fence", new SysCommand_TopicsRequest());
                List<Tuple<string, byte[]>> frames = live.ReadThrough("__nested-publish-fence");
                Assert.AreEqual(2, frames.FindAll(frame => frame.Item1 == "/nested-publish").Count,
                    "Nested publication must retain one exact unit for each admitted ownership use.");
                Assert.AreEqual(0, Capacity(topic.MessageSender));
            }
        }

        [Test]
        public void BlockedPublisherSerializerDoesNotBlockDisconnectOrReplacement()
        {
            using (var live = new LiveConnection())
            using (var blocked = new BlockingOfflineMessage())
            {
                live.Connect();
                live.Connection.RegisterPublisher<UInt8MultiArrayMsg>("/blocked-publisher");
                Task<Exception> publish = Task.Run(() =>
                {
                    try
                    {
                        live.Connection.Publish("/blocked-publisher", blocked);
                        return (Exception)null;
                    }
                    catch (Exception exception) { return exception; }
                });
                Assert.IsTrue(blocked.Entered.Wait(TimeSpan.FromSeconds(3)),
                    "The publisher serializer must be the blocked caller-side operation.");

                Task disconnect = Task.Run(() => live.Connection.Disconnect());
                Assert.IsTrue(disconnect.Wait(TimeSpan.FromSeconds(3)),
                    "Disconnect must not wait for a publisher serializer owned by another caller.");
                Assert.IsFalse(disconnect.IsFaulted);
                Assert.IsTrue(SpinWait.SpinUntil(() => !live.Connection.HasConnectionThread, TimeSpan.FromSeconds(3)),
                    "Disconnect must complete transport cleanup before the replacement attempt starts.");

                live.Connect();
                live.Connection.QueueSysCommand("__blocked-publisher-fence", new SysCommand_TopicsRequest());
                blocked.Release.Set();
                Assert.IsTrue(publish.Wait(TimeSpan.FromSeconds(3)));
                Assert.IsInstanceOf<IOException>(publish.Result,
                    "A publication prepared in the closed offline epoch must not enter its replacement.");

                List<Tuple<string, byte[]>> frames = live.ReadThrough("__blocked-publisher-fence");
                Assert.IsFalse(frames.Exists(frame => frame.Item1 == "/blocked-publisher"),
                    "The replacement epoch must not inherit the blocked publication.");
            }
        }

        [Test]
        public void OutgoingWakeDrainsLargeBatchAndLateSignalsAreSafe()
        {
            using (var live = new LiveConnection())
            {
                live.Connect();
                object session = typeof(ROSConnection).GetField("m_WorkerSession",
                    BindingFlags.Instance | BindingFlags.NonPublic).GetValue(live.Connection);
                const int commandCount = 80;
                for (int index = 0; index < commandCount; index++)
                    live.Connection.QueueSysCommand("__l7-command-" + index, new SysCommand_TopicsRequest());
                live.Connection.QueueSysCommand("__l7-fence", new SysCommand_TopicsRequest());

                List<Tuple<string, byte[]>> frames = live.ReadThrough("__l7-fence", 256);
                for (int index = 0; index < commandCount; index++)
                    Assert.AreEqual(1, frames.FindAll(frame => frame.Item1 == "__l7-command-" + index).Count,
                        "Every command queued beyond one worker batch must be written exactly once.");

                live.Connection.Disconnect();
                Assert.IsTrue(SpinWait.SpinUntil(() => !live.Connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                object wakeSignal = session.GetType().GetField("WakeSignal",
                    BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public).GetValue(session);
                Assert.DoesNotThrow(() => wakeSignal.GetType().GetMethod("Signal",
                    BindingFlags.Instance | BindingFlags.NonPublic).Invoke(wakeSignal, null),
                    "A late enqueue signal after session disposal must be harmless.");
            }
        }

        [Test]
        public void PreConnectPublicationAndRawCommandAreRetainedUntilConnect()
        {
            using (var live = new LiveConnection())
            {
                RosTopicState topic = live.Connection.RegisterPublisher<UInt8MultiArrayMsg>("/offline-publication");
                var source = new UInt8MultiArrayMsg(new MultiArrayLayoutMsg(), new byte[] { 17, 23 });

                live.Connection.Publish("/offline-publication", source);
                live.Connection.QueueSysCommand("__offline-fence", new SysCommand_TopicsRequest());

                live.Connect();
                List<Tuple<string, byte[]>> frames = live.ReadThrough("__offline-fence");
                int registration = frames.FindIndex(frame => frame.Item1 == SysCommand.k_SysCommand_Publish);
                int publication = frames.FindIndex(frame => frame.Item1 == "/offline-publication");
                int fence = frames.FindIndex(frame => frame.Item1 == "__offline-fence");

                Assert.GreaterOrEqual(registration, 0, "Connect must register the offline publisher.");
                Assert.Greater(publication, registration, "The offline publication must follow its registration.");
                Assert.Greater(fence, publication, "The raw offline command must remain after the publication.");
                CollectionAssert.AreEqual(Serialize(source), frames[publication].Item2);
                Assert.AreSame(topic, live.Connection.GetTopic("/offline-publication"));
            }
        }

        sealed class BlockingOfflineMessage : UInt8MultiArrayMsg, IDisposable
        {
            internal readonly ManualResetEventSlim Entered = new ManualResetEventSlim(false);
            internal readonly ManualResetEventSlim Release = new ManualResetEventSlim(false);

            public override void SerializeTo(MessageSerializer serializer)
            {
                Entered.Set();
                Release.Wait();
                base.SerializeTo(serializer);
            }

            public void Dispose()
            {
                Release.Dispose();
                Entered.Dispose();
            }
        }

        [Test]
        public void OfflinePreparationCannotSurviveExplicitDisconnect()
        {
            var gameObject = new GameObject("offline preparation epoch");
            var listener = new TcpListener(IPAddress.Loopback, 0);
            ROSConnectionConfig firstConfig = null;
            ROSConnectionConfig replacementConfig = null;
            TcpClient peer = null;
            Task<Exception> blockedPublish = null;
            var connection = gameObject.AddComponent<ROSConnection>();
            connection.ConnectOnStart = false;
            connection.listenForTFMessages = false;
            var blocked = new BlockingOfflineMessage();
            try
                {
                    firstConfig = ScriptableObject.CreateInstance<ROSConnectionConfig>();
                    typeof(ROSConnectionConfig).GetField("m_RosIPAddress", BindingFlags.Instance | BindingFlags.NonPublic)
                        .SetValue(firstConfig, "127.0.0.1");
                    typeof(ROSConnectionConfig).GetField("m_RosPort", BindingFlags.Instance | BindingFlags.NonPublic)
                        .SetValue(firstConfig, 10000);
                    connection.ConnectionConfig = firstConfig;
                    connection.RegisterPublisher<UInt8MultiArrayMsg>("/offline-epoch");

                    blockedPublish = Task.Run(() =>
                    {
                        try
                        {
                            connection.Publish("/offline-epoch", blocked);
                            return (Exception)null;
                        }
                        catch (Exception exception) { return exception; }
                    });
                    Assert.IsTrue(blocked.Entered.Wait(TimeSpan.FromSeconds(3)),
                        "The old offline publication must be blocked in caller-side preparation.");

                    connection.Disconnect();
                    listener.Start();
                    replacementConfig = ScriptableObject.CreateInstance<ROSConnectionConfig>();
                    typeof(ROSConnectionConfig).GetField("m_RosIPAddress", BindingFlags.Instance | BindingFlags.NonPublic)
                        .SetValue(replacementConfig, "127.0.0.1");
                    typeof(ROSConnectionConfig).GetField("m_RosPort", BindingFlags.Instance | BindingFlags.NonPublic)
                        .SetValue(replacementConfig, ((IPEndPoint)listener.LocalEndpoint).Port);
                    connection.ConnectionConfig = replacementConfig;

                    var fresh = new UInt8MultiArrayMsg(new MultiArrayLayoutMsg(), new byte[] { 41, 59 });
                    Assert.DoesNotThrow(() => connection.Publish("/offline-epoch", fresh));
                    blocked.Release.Set();
                    Assert.IsTrue(blockedPublish.Wait(TimeSpan.FromSeconds(3)));
                    Assert.IsInstanceOf<IOException>(blockedPublish.Result,
                        "A preparation captured by the closed offline epoch must not commit to its replacement.");

                    Task<TcpClient> accept = listener.AcceptTcpClientAsync();
                    connection.QueueSysCommand("__offline-epoch-fence", new SysCommand_TopicsRequest());
                    connection.Connect();
                    Assert.IsTrue(accept.Wait(TimeSpan.FromSeconds(5)));
                    peer = accept.Result;
                    peer.GetStream().ReadTimeout = 5000;
                    var frames = new List<Tuple<string, byte[]>>();
                    for (int index = 0; index < 64; index++)
                    {
                        Tuple<string, byte[]> frame = ReadFrame(peer.GetStream());
                        frames.Add(frame);
                        if (frame.Item1 == "__offline-epoch-fence") break;
                    }
                    Assert.IsTrue(frames.Exists(frame => frame.Item1 == "/offline-epoch"));
                    Assert.AreEqual(1, frames.FindAll(frame => frame.Item1 == "/offline-epoch").Count,
                        "The closed epoch's blocked publication must not be retagged onto the replacement.");
                    Tuple<string, byte[]> publication = frames.Find(frame => frame.Item1 == "/offline-epoch");
                    CollectionAssert.AreEqual(Serialize(fresh), publication.Item2);
                }
            finally
            {
                blocked.Release.Set();
                if (blockedPublish != null) Assert.IsTrue(blockedPublish.Wait(TimeSpan.FromSeconds(5)));
                connection.Disconnect();
                Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                peer?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (firstConfig != null) UnityEngine.Object.DestroyImmediate(firstConfig);
                if (replacementConfig != null) UnityEngine.Object.DestroyImmediate(replacementConfig);
                blocked.Dispose();
            }
        }

        static int LedgerCount(string field)
        {
            Type registry = typeof(TopicMessageSender).Assembly.GetType("Unity.Robotics.ROSTCPConnector.MessageUseRegistry");
            object gate = registry.GetField("s_Gate", BindingFlags.Static | BindingFlags.NonPublic).GetValue(null);
            object collection = registry.GetField(field, BindingFlags.Static | BindingFlags.NonPublic).GetValue(null);
            lock (gate)
                return ((System.Collections.ICollection)collection).Count;
        }

        [Test]
        public void BlockedPoolCannotStarveTransportOrGrowReturnWork()
        {
            RecyclerFence();
            int openingRecords = LedgerCount("s_Records");
            using (var held = new HeldPool())
            using (var live = new LiveConnection())
            {
                OfferTo(held, new UInt8MultiArrayMsg());
                Assert.IsTrue(held.Entered.Wait(TimeSpan.FromSeconds(3)));
                var pool = new RecordingPool();
                for (int index = 0; index < 48; index++) OfferTo(pool, new UInt8MultiArrayMsg());
                Assert.AreEqual(32, LedgerCount("s_Pending"));
                Assert.AreEqual(openingRecords + 33, LedgerCount("s_Records"), "Abandoned excess returns must not leave ledger history.");
                Assert.AreEqual(0, Volatile.Read(ref pool.Offers));
                Assert.IsFalse(held.IsThreadPoolThread);
                Type recycler = typeof(TopicMessageSender).Assembly.GetType("Unity.Robotics.ROSTCPConnector.MessageRecycler");
                var thread = (Thread)recycler.GetField("s_Thread", BindingFlags.Static | BindingFlags.NonPublic).GetValue(null);
                Assert.AreEqual(held.ThreadId, thread.ManagedThreadId);

                bool received = false;
                live.Connection.Subscribe<UInt8MultiArrayMsg>("/incoming", _ => received = true);
                RosTopicState publisher = live.Connection.RegisterPublisher<UInt8MultiArrayMsg>("/outgoing");
                live.Connect();
#if ROS2
                string protocol = "ROS2";
#else
                string protocol = "ROS1";
#endif
                live.SendFrame(SysCommand.k_SysCommand_Handshake, Encoding.UTF8.GetBytes(JsonUtility.ToJson(new SysCommand_Handshake
                { version = ROSConnection.k_Version, metadata = JsonUtility.ToJson(new SysCommand_Handshake_Metadata { protocol = protocol }) })));
                var source = new UInt8MultiArrayMsg(new MultiArrayLayoutMsg(), new byte[] { 13, 19 });
                live.Connection.Publish("/outgoing", source);
                live.Connection.QueueSysCommand("__pool_fence", new SysCommand_TopicsRequest());
                List<Tuple<string, byte[]>> frames = live.ReadThrough("__pool_fence");
                int publications = 0;
                foreach (Tuple<string, byte[]> frame in frames)
                    if (frame.Item1 == "/outgoing") { publications++; CollectionAssert.AreEqual(Serialize(source), frame.Item2); }
                Assert.AreEqual(1, publications);
                live.SendFrame("/incoming", Serialize(new UInt8MultiArrayMsg()));
                MethodInfo update = typeof(ROSConnection).GetMethod("Update", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.IsTrue(SpinWait.SpinUntil(() => { update.Invoke(live.Connection, null); return received; }, TimeSpan.FromSeconds(5)));
                live.Connection.Disconnect();
                Assert.IsTrue(SpinWait.SpinUntil(() => !live.Connection.HasConnectionThread, TimeSpan.FromSeconds(5)),
                    "Transport completion must not belong to the blocked pool.");
                publisher.MessageSender.Dispose();
                Assert.AreEqual(32, LedgerCount("s_Pending"));
                Assert.AreEqual(1, Volatile.Read(ref held.Offers));
                held.Release.Set();
                Assert.IsTrue(SpinWait.SpinUntil(() => Volatile.Read(ref pool.Offers) == 32, TimeSpan.FromSeconds(3)));
                RecyclerFence();
                Assert.AreEqual(openingRecords, LedgerCount("s_Records"));
                Assert.AreEqual(0, LedgerCount("s_Pending"));
            }
        }

        sealed class HeldWriteStream : MemoryStream
        {
            public readonly ManualResetEventSlim Entered = new ManualResetEventSlim(false);
            public readonly ManualResetEventSlim Release = new ManualResetEventSlim(false);
            public readonly IOException Failure;
            bool m_Held;

            public HeldWriteStream(bool fail) { Failure = fail ? new IOException("controlled exact write failure") : null; }

            public override void Write(byte[] buffer, int offset, int count)
            {
                if (!m_Held)
                {
                    m_Held = true;
                    Entered.Set();
                    Release.Wait();
                    if (Failure != null) throw Failure;
                }
                base.Write(buffer, offset, count);
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing) { Entered.Dispose(); Release.Dispose(); }
                base.Dispose(disposing);
            }
        }

        static int Capacity(TopicMessageSender sender)
        {
            return (int)typeof(TopicMessageSender).GetField("m_ExactMessagesInFlight",
                BindingFlags.Instance | BindingFlags.NonPublic).GetValue(sender);
        }

        sealed class RepeatedSegmentMessage : Message
        {
            readonly byte[] m_Segment = new byte[1024 * 1024];
            readonly int m_Repetitions;
            public RepeatedSegmentMessage(int repetitions) { m_Repetitions = repetitions; }
            public override void SerializeTo(MessageSerializer serializer)
            {
                for (int index = 0; index < m_Repetitions; index++) serializer.Write(m_Segment);
            }
        }

        sealed class InvalidatingServiceMessage : UInt8MultiArrayMsg
        {
            public Action DuringSerialization;
            public override void SerializeTo(MessageSerializer serializer)
            {
                base.SerializeTo(serializer);
                DuringSerialization();
            }
        }

        static List<byte[]> Control(string command, int id)
        {
            var serializer = new MessageSerializer();
            typeof(ROSConnection).GetMethod("PopulateSysCommand", BindingFlags.Static | BindingFlags.NonPublic)
                .Invoke(null, new object[] { serializer, command, new SysCommand_Service { srv_id = id } });
            return serializer.GetBytesSequence();
        }

        sealed class PartialWriteStream : MemoryStream
        {
            readonly int m_BytesBeforeFailure;
            public PartialWriteStream(int bytesBeforeFailure) { m_BytesBeforeFailure = bytesBeforeFailure; }
            public override void Write(byte[] buffer, int offset, int count)
            {
                int accepted = Math.Min(count, m_BytesBeforeFailure - (int)Length);
                base.Write(buffer, offset, accepted);
                throw new IOException("controlled partial pair write");
            }
        }

        [TestCase("clear-before-claim")]
        [TestCase("serialization")]
        [TestCase("partial-control")]
        [TestCase("partial-payload")]
        public void ExactCapsuleTerminalMatrix(string terminal)
        {
            var channel = new TopicMessageSender("/terminal", UInt8MultiArrayMsg.k_RosMessageName, 1);
            var pool = new RecordingPool();
            channel.SetMessagePool(pool);
            List<byte[]> control = Control(SysCommand.k_SysCommand_ServiceResponse, 811);
            OutgoingMessageSender pair = null;
            try
            {
                if (terminal == "serialization")
                {
                    var source = new InvalidatingServiceMessage
                    { DuringSerialization = () => { throw new IOException("controlled preparation failure"); } };
                    Assert.Throws<IOException>(() => Prepare(channel, source, control));
                }
                else
                {
                    pair = Prepare(channel, new UInt8MultiArrayMsg(), control);
                    if (terminal == "clear-before-claim")
                        pair.ClearAllQueuedData();
                    else
                    {
                        int controlLength = 0;
                        foreach (byte[] segment in control) controlLength += segment.Length;
                        int failAfter = terminal == "partial-control" ? 1 : controlLength + 1;
                        using (var partial = new PartialWriteStream(failAfter))
                        {
                            Assert.Throws<IOException>(() => pair.SendInternal(new MessageSerializer(), partial));
                            Assert.AreEqual(failAfter, partial.Length);
                        }
                    }
                    using (var laterStream = new MemoryStream())
                    {
                        Assert.AreEqual(OutgoingMessageSender.SendToState.NoMessageToSendError,
                            pair.SendInternal(new MessageSerializer(), laterStream));
                        pair.ClearAllQueuedData();
                        pair.ClearAllQueuedData();
                        Assert.AreEqual(0, laterStream.Length, "Neither a cancelled pair nor a failed suffix may be retried.");
                    }
                }
                Assert.AreEqual(0, Capacity(channel));
                var fence = new UInt8MultiArrayMsg();
                OutgoingMessageSender next = Prepare(channel, fence, control);
                Assert.NotNull(next);
                next.ClearAllQueuedData();
                Assert.IsTrue(SpinWait.SpinUntil(() => ReferenceEquals(Volatile.Read(ref pool.Message), fence), TimeSpan.FromSeconds(2)));
                Assert.AreEqual(terminal == "serialization" ? 1 : 2, Volatile.Read(ref pool.Offers));
                Assert.AreEqual(0, Capacity(channel));
            }
            finally { pair?.ClearAllQueuedData(); }
        }

        static OutgoingMessageSender Prepare(TopicMessageSender sender, Message message, List<byte[]> control)
        {
            try
            {
                return (OutgoingMessageSender)typeof(TopicMessageSender)
                    .GetMethod("CreateExactSender", BindingFlags.Instance | BindingFlags.NonPublic)
                    .Invoke(sender, new object[] { message, control });
            }
            catch (TargetInvocationException exception)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(exception.InnerException).Throw();
                throw;
            }
        }

        static byte[] Serialize(Message message)
        {
            var serializer = new MessageSerializer();
            serializer.SerializeMessage(message);
            return serializer.GetBytes();
        }

        static Tuple<string, byte[]> ReadFrame(Stream stream)
        {
            var reader = new BinaryReader(stream, Encoding.UTF8, true);
            int topicLength = reader.ReadInt32();
            Assert.That(topicLength, Is.InRange(0, 4096));
            byte[] topic = reader.ReadBytes(topicLength);
            Assert.AreEqual(topicLength, topic.Length);
            int payloadLength = reader.ReadInt32();
            Assert.That(payloadLength, Is.InRange(0, 64 * 1024 * 1024));
            byte[] payload = reader.ReadBytes(payloadLength);
            Assert.AreEqual(payloadLength, payload.Length);
            return Tuple.Create(Encoding.UTF8.GetString(topic).TrimEnd('\0'), payload);
        }

        [TestCase(true)]
        [TestCase(false)]
        public void ExactSnapshotRejectsOversizeBeforeCopyAndReusesCapacity(bool oversizedTopic)
        {
            var channel = new TopicMessageSender(oversizedTopic ? new string('x', 4097) : "/bounded", "test/Bounded", 1);
            Message source = oversizedTopic ? (Message)new UInt8MultiArrayMsg() : new RepeatedSegmentMessage(65);
            OutgoingMessageSender unexpected = null;
            try
            {
                Assert.Catch<IOException>(() => unexpected = Prepare(channel, source, Control(SysCommand.k_SysCommand_ServiceRequest, 10)),
                    "New owned snapshots must enforce the existing topic/payload byte limits before copying.");
                Assert.AreEqual(0, Capacity(channel), "Rejected snapshot preparation must not consume a pair slot.");
                if (!oversizedTopic)
                {
                    OutgoingMessageSender next = Prepare(channel, new UInt8MultiArrayMsg(), Control(SysCommand.k_SysCommand_ServiceRequest, 11));
                    Assert.NotNull(next);
                    next.ClearAllQueuedData();
                    Assert.AreEqual(0, Capacity(channel));
                }
            }
            finally { unexpected?.ClearAllQueuedData(); }
        }

        [TestCase("maximum")]
        [TestCase("overflowed-length")]
        [TestCase("unfinished-prefix")]
        public void ExactSnapshotEnvelopeBoundaryMatrix(string boundary)
        {
            int topicLength = 4096;
#if ROS2
            topicLength--; // The existing ROS2 wire encoding includes a trailing NUL.
#endif
            var channel = new TopicMessageSender(new string('x', topicLength), UInt8MultiArrayMsg.k_RosMessageName, 1);
            List<byte[]> control = Control(SysCommand.k_SysCommand_ServiceRequest, 12);
            OutgoingMessageSender pair = null;
            try
            {
                if (boundary == "maximum")
                {
                    var source = new UInt8MultiArrayMsg();
                    source.data = new byte[64 * 1024 * 1024 - Serialize(source).Length];
                    pair = Prepare(channel, source, control);
                    Assert.NotNull(pair);
                    Assert.AreEqual(OutgoingMessageSender.SendToState.Normal, pair.SendInternal(new MessageSerializer(), Stream.Null));
                }
                else if (boundary == "overflowed-length")
                    Assert.Throws<IOException>(() => Prepare(channel, new RepeatedSegmentMessage(2048), control));
                else
                {
                    control.Add(null);
                    Assert.Throws<IOException>(() => Prepare(channel, new UInt8MultiArrayMsg(), control));
                }
                Assert.AreEqual(0, Capacity(channel));
            }
            finally { pair?.ClearAllQueuedData(); }
        }

        [TestCase(true)]
        [TestCase(false)]
        public void RejectedServicePairDoesNotReturnUntransferredSource(bool request)
        {
            var gameObject = new GameObject("service pair rollback");
            var listener = new TcpListener(IPAddress.Loopback, 0);
            ROSConnectionConfig config = null;
            TcpClient peer = null;
            var connection = gameObject.AddComponent<ROSConnection>();
            connection.ConnectOnStart = false;
            connection.listenForTFMessages = false;
            try
            {
                listener.Start();
                config = ScriptableObject.CreateInstance<ROSConnectionConfig>();
                typeof(ROSConnectionConfig).GetField("m_RosIPAddress", BindingFlags.Instance | BindingFlags.NonPublic)
                    .SetValue(config, "127.0.0.1");
                typeof(ROSConnectionConfig).GetField("m_RosPort", BindingFlags.Instance | BindingFlags.NonPublic)
                    .SetValue(config, ((IPEndPoint)listener.LocalEndpoint).Port);
                connection.ConnectionConfig = config;
                connection.RegisterRosService("/rollback", UInt8MultiArrayMsg.k_RosMessageName, UInt8MultiArrayMsg.k_RosMessageName, 1);
                var pool = new RecordingPool();
                TopicMessageSender channel = connection.GetTopic("/rollback").MessageSender;
                channel.SetMessagePool(pool);
                Task<TcpClient> accept = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(accept.Wait(TimeSpan.FromSeconds(5)));
                peer = accept.Result;
                object attempt = AttemptTestAccess.Current(connection);
                var source = new InvalidatingServiceMessage { DuringSerialization = connection.Disconnect };
                if (request)
                {
                    Task<UInt8MultiArrayMsg> call = connection.SendServiceMessage<UInt8MultiArrayMsg>("/rollback", source);
                    Assert.IsTrue(call.IsFaulted);
                    Assert.IsInstanceOf<IOException>(call.Exception.InnerException);
                }
                else
                {
                    Assert.IsFalse((bool)typeof(ROSConnection).GetMethod("TryQueueServicePair",
                        BindingFlags.Instance | BindingFlags.NonPublic).Invoke(connection, new object[]
                        { SysCommand.k_SysCommand_ServiceResponse, new SysCommand_Service { srv_id = 901 }, channel, source, attempt }));
                }
                Assert.AreEqual(0, Capacity(channel));
                Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));

                // A later standalone unit is a FIFO recycler fence. When its callback
                // arrives, every earlier offer from the aborted preparation has run.
                var fence = new UInt8MultiArrayMsg();
                OutgoingMessageSender accepted = Prepare(channel, fence, Control(SysCommand.k_SysCommand_ServiceRequest, 902));
                accepted.ClearAllQueuedData();
                Assert.IsTrue(SpinWait.SpinUntil(() => ReferenceEquals(Volatile.Read(ref pool.Message), fence), TimeSpan.FromSeconds(2)));
                Assert.AreEqual(1, Volatile.Read(ref pool.Offers),
                    "Failed admission borrowed the source; it must not transfer the caller's object to a pool.");
                Assert.AreEqual(0, Capacity(channel));
            }
            finally
            {
                connection.Disconnect();
                bool stopped = SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5));
                peer?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
                Assert.IsTrue(stopped);
            }
        }

        static int ReadServiceControl(Stream stream)
        {
            for (int count = 0; count < 8; count++)
            {
                Tuple<string, byte[]> frame = ReadFrame(stream);
                if (frame.Item1 == SysCommand.k_SysCommand_ServiceRequest)
                    return JsonUtility.FromJson<SysCommand_Service>(Encoding.UTF8.GetString(frame.Item2).TrimEnd('\0')).srv_id;
                CollectionAssert.Contains(new[] { "", SysCommand.k_SysCommand_RosService, SysCommand.k_SysCommand_TopicList }, frame.Item1,
                    "No old service payload or partial suffix may precede a new request.");
            }
            Assert.Fail("A service request must arrive after bounded startup traffic.");
            return 0;
        }

        [Test]
        public void ServicePairWriteFailureReleasesWholeAdmission()
        {
            var gameObject = new GameObject("partial service pair failure");
            var listener = new TcpListener(IPAddress.Loopback, 0);
            ROSConnectionConfig config = null;
            TcpClient firstPeer = null;
            TcpClient secondPeer = null;
            Task<UInt8MultiArrayMsg> firstCall = null;
            Task<UInt8MultiArrayMsg> secondCall = null;
            var connection = gameObject.AddComponent<ROSConnection>();
            connection.ConnectOnStart = false;
            connection.listenForTFMessages = false;
            connection.NetworkTimeoutSeconds = 10;
            try
            {
                listener.Start();
                config = ScriptableObject.CreateInstance<ROSConnectionConfig>();
                typeof(ROSConnectionConfig).GetField("m_RosIPAddress", BindingFlags.Instance | BindingFlags.NonPublic)
                    .SetValue(config, "127.0.0.1");
                typeof(ROSConnectionConfig).GetField("m_RosPort", BindingFlags.Instance | BindingFlags.NonPublic)
                    .SetValue(config, ((IPEndPoint)listener.LocalEndpoint).Port);
                connection.ConnectionConfig = config;
                connection.RegisterRosService("/partial", UInt8MultiArrayMsg.k_RosMessageName,
                    UInt8MultiArrayMsg.k_RosMessageName, 1);
                TopicMessageSender channel = connection.GetTopic("/partial").MessageSender;
                Task<TcpClient> accept = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(accept.Wait(TimeSpan.FromSeconds(5)));
                firstPeer = accept.Result;
                firstPeer.ReceiveBufferSize = 4096;
                firstPeer.GetStream().ReadTimeout = 5000;
                var client = (TcpClient)AttemptTestAccess.Root(connection, "Client");
                client.SendBufferSize = 4096;
                long firstAttempt = AttemptTestAccess.Id(connection);

                // The unread payload is much larger than either bounded socket buffer.
                // Reading its control/header/first bytes is the barrier, not a sleep or
                // peer.Available guess. Closing this peer must interrupt an unfinished write.
                var large = new UInt8MultiArrayMsg(new MultiArrayLayoutMsg(), new byte[8 * 1024 * 1024]);
                firstCall = connection.SendServiceMessage<UInt8MultiArrayMsg>("/partial", large);
                int firstId = ReadServiceControl(firstPeer.GetStream());
                var reader = new BinaryReader(firstPeer.GetStream(), Encoding.UTF8, true);
                Assert.AreEqual("/partial", Encoding.UTF8.GetString(reader.ReadBytes(reader.ReadInt32())).TrimEnd('\0'));
                Assert.Greater(reader.ReadInt32(), large.data.Length);
                Assert.AreEqual(64, reader.ReadBytes(64).Length, "The peer actually received a payload prefix.");
                Assert.AreEqual(1, Capacity(channel), "The unread payload must still own the sole pair slot.");
                Task<UInt8MultiArrayMsg> overflow = connection.SendServiceMessage<UInt8MultiArrayMsg>("/partial", new UInt8MultiArrayMsg());
                Assert.IsTrue(overflow.IsFaulted);
                Assert.IsInstanceOf<IOException>(overflow.Exception.InnerException);

                Task<TcpClient> retry = listener.AcceptTcpClientAsync();
                LogAssert.Expect(LogType.Error, new Regex("ROS Connection to .* failed -"));
                firstPeer.Client.LingerState = new LingerOption(true, 0);
                firstPeer.Close();
                firstPeer = null;
                Assert.IsTrue(SpinWait.SpinUntil(() => firstCall.IsCompleted, TimeSpan.FromSeconds(5)),
                    "The failed attempt must fault its outstanding typed service waiter.");
                Assert.IsInstanceOf<IOException>(firstCall.Exception.InnerException);
                Assert.AreEqual(0, Capacity(channel), "A partial-pair failure releases whole admission once.");
                Assert.IsTrue(retry.Wait(TimeSpan.FromSeconds(5)), "The worker may start a fresh attempt, not resume a suffix.");
                secondPeer = retry.Result;
                secondPeer.GetStream().ReadTimeout = 5000;
                Assert.AreNotEqual(firstAttempt, AttemptTestAccess.Id(connection));

                var fresh = new UInt8MultiArrayMsg(new MultiArrayLayoutMsg(), new byte[] { 31, 47, 61 });
                secondCall = connection.SendServiceMessage<UInt8MultiArrayMsg>("/partial", fresh);
                connection.QueueSysCommand("__slice2_end", new SysCommand_TopicsRequest());
                int secondId = ReadServiceControl(secondPeer.GetStream());
                Assert.AreNotEqual(firstId, secondId, "No automatic retry of the possibly executed old request.");
                Tuple<string, byte[]> payload = ReadFrame(secondPeer.GetStream());
                Assert.AreEqual("/partial", payload.Item1);
                CollectionAssert.AreEqual(Serialize(fresh), payload.Item2);
                Tuple<string, byte[]> end = ReadFrame(secondPeer.GetStream());
                if (end.Item1 == SysCommand.k_SysCommand_TopicList) end = ReadFrame(secondPeer.GetStream());
                Assert.AreEqual("__slice2_end", end.Item1, "No second service half/pair trails the fresh request.");
                Assert.AreEqual(0, Capacity(channel));
                Assert.IsFalse(secondCall.IsCompleted, "A full write is not remote service completion.");
            }
            finally
            {
                connection.Disconnect();
                bool stopped = SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5));
                if (firstCall != null) { Exception observed = firstCall.Exception; }
                if (secondCall != null) { Exception observed = secondCall.Exception; }
                firstPeer?.Close();
                secondPeer?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
                Assert.IsTrue(stopped, "The failure fixture must leave no transport owner.");
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public void ClearCannotStealInFlightExactCapsule(bool failWrite)
        {
            var source = new UInt8MultiArrayMsg(new MultiArrayLayoutMsg(), new byte[] { 5, 9 });
            byte[] expected = Serialize(source);
            var channel = new TopicMessageSender("/claimed", source.RosMessageName, 1);
            var pool = new RecordingPool();
            channel.SetMessagePool(pool);
            OutgoingMessageSender pair = Prepare(channel, source, Control(SysCommand.k_SysCommand_ServiceRequest, 401));
            using (var wire = new HeldWriteStream(failWrite))
            using (var competingWire = new MemoryStream())
            {
                Task<Exception> writer = Task.Run(() =>
                {
                    try { pair.SendInternal(new MessageSerializer(), wire); return (Exception)null; }
                    catch (Exception exception) { return exception; }
                });
                try
                {
                    Assert.IsTrue(wire.Entered.Wait(TimeSpan.FromSeconds(2)), "The writer must claim before Clear.");
                    Task clear = Task.Run(() =>
                    {
                        pair.ClearAllQueuedData();
                        Assert.AreEqual(OutgoingMessageSender.SendToState.NoMessageToSendError,
                            pair.SendInternal(new MessageSerializer(), competingWire));
                        pair.ClearAllQueuedData();
                    });
                    Assert.IsTrue(clear.Wait(TimeSpan.FromSeconds(2)), "Clear must not wait for the stream.");
                    Assert.AreEqual(1, Capacity(channel), "Only the claimed writer can release whole-pair capacity.");
                    Assert.AreEqual(0, Volatile.Read(ref pool.Offers), "Clear cannot return the writer's source.");
                    OutgoingMessageSender overflow = Prepare(channel, new UInt8MultiArrayMsg(),
                        Control(SysCommand.k_SysCommand_ServiceRequest, 402));
                    try { Assert.IsNull(overflow, "QueueSize=1 still includes the held write."); }
                    finally { overflow?.ClearAllQueuedData(); }
                    Assert.AreEqual(0, competingWire.Length);
                }
                finally
                {
                    wire.Release.Set();
                    Assert.IsTrue(writer.Wait(TimeSpan.FromSeconds(2)), "Release the writer even on an assertion failure.");
                }
                Assert.AreSame(wire.Failure, writer.Result);
                Assert.AreEqual(0, Capacity(channel));
                Assert.IsTrue(pool.Offered.Wait(TimeSpan.FromSeconds(2)));
                Assert.AreSame(source, pool.Message);
                pair.ClearAllQueuedData();
                Assert.AreEqual(OutgoingMessageSender.SendToState.NoMessageToSendError,
                    pair.SendInternal(new MessageSerializer(), competingWire));
                Assert.AreEqual(1, Volatile.Read(ref pool.Offers));
                Assert.AreEqual(0, Capacity(channel));
                if (!failWrite)
                {
                    wire.Position = 0;
                    Assert.AreEqual(SysCommand.k_SysCommand_ServiceRequest, ReadFrame(wire).Item1);
                    CollectionAssert.AreEqual(expected, ReadFrame(wire).Item2);
                    Assert.AreEqual(wire.Length, wire.Position);
                }
                channel.SetMessagePool(null);
                OutgoingMessageSender next = Prepare(channel, new UInt8MultiArrayMsg(),
                    Control(SysCommand.k_SysCommand_ServiceRequest, 403));
                Assert.NotNull(next, "Terminal completion makes one slot available again.");
                next.ClearAllQueuedData();
                next.ClearAllQueuedData();
                Assert.AreEqual(0, Capacity(channel), "A cancelled capsule releases once too.");
            }
        }

        [Test]
        public void ExactControlBytesRemainFrozenWhenCallerClearsAndMutatesPrefix()
        {
            var source = new UInt8MultiArrayMsg();
            var channel = new TopicMessageSender("/control", source.RosMessageName, 1);
            List<byte[]> prefix = Control(SysCommand.k_SysCommand_ServiceResponse, 671);
            OutgoingMessageSender pair = Prepare(channel, source, prefix);
            try
            {
                foreach (byte[] segment in prefix) Array.Clear(segment, 0, segment.Length);
                prefix.Clear();
                using (var wire = new MemoryStream())
                {
                    Assert.AreEqual(OutgoingMessageSender.SendToState.Normal, pair.SendInternal(new MessageSerializer(), wire));
                    wire.Position = 0;
                    Tuple<string, byte[]> control = ReadFrame(wire);
                    Assert.AreEqual(SysCommand.k_SysCommand_ServiceResponse, control.Item1);
                    Assert.AreEqual(671, JsonUtility.FromJson<SysCommand_Service>(Encoding.UTF8.GetString(control.Item2).TrimEnd('\0')).srv_id);
                    Tuple<string, byte[]> payload = ReadFrame(wire);
                    Assert.AreEqual("/control", payload.Item1);
                    CollectionAssert.AreEqual(Serialize(source), payload.Item2);
                    Assert.AreEqual(wire.Length, wire.Position);
                }
            }
            finally { pair.ClearAllQueuedData(); }
        }

        [TestCase(SysCommand.k_SysCommand_ServiceRequest)]
        [TestCase(SysCommand.k_SysCommand_ServiceResponse)]
        public void FrozenServicePayloadDoesNotAliasByteArrayFields(string command)
        {
            // A real generated byte-array-bearing Message used as the service payload, not
            // a test serializer which conveniently copies arrays itself.
            var request = new UInt8MultiArrayMsg(new MultiArrayLayoutMsg(), new byte[] { 17, 29, 43 });
            var channel = new TopicMessageSender("/frozen", request.RosMessageName, 1);
            byte[] expected = Serialize(request);
            OutgoingMessageSender pair = Prepare(channel, request, Control(command, 731));
            Assert.NotNull(pair);
            try
            {
                request.data[0] = 201;
                request.layout.data_offset = 777;
                using (var wire = new MemoryStream())
                {
                    Assert.AreEqual(OutgoingMessageSender.SendToState.Normal,
                        pair.SendInternal(new MessageSerializer(), wire));
                    wire.Position = 0;
                    Tuple<string, byte[]> control = ReadFrame(wire);
                    Tuple<string, byte[]> payload = ReadFrame(wire);
                    Assert.AreEqual(command, control.Item1);
                    Assert.AreEqual(731, JsonUtility.FromJson<SysCommand_Service>(
                        Encoding.UTF8.GetString(control.Item2).TrimEnd('\0')).srv_id);
                    Assert.AreEqual("/frozen", payload.Item1);
                    CollectionAssert.AreEqual(expected, payload.Item2,
                        "Queued service bytes must be the submission snapshot, not a later byte[] alias.");
                    Assert.AreEqual(wire.Length, wire.Position, "The unit contains exactly one control/payload pair.");
                }
            }
            finally { pair.ClearAllQueuedData(); }
        }
        [Test]
        public void ConnectionOwnedClearAllQueuedDataRemovesGlobalPublication()
        {
            using (var live = new LiveConnection())
            {
                RosTopicState topic = live.Connection.RegisterPublisher<UInt8MultiArrayMsg>("/clear-global-publication");
                live.Connection.Publish("/clear-global-publication", new UInt8MultiArrayMsg());

                object outgoing = AttemptTestAccess.Root(live.Connection, "Outgoing");
                PropertyInfo count = outgoing.GetType().GetProperty("Count",
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                Assert.AreEqual(1, (int)count.GetValue(outgoing),
                    "The public publication must be present in the attempt-global outbox before clearing.");

                topic.MessageSender.ClearAllQueuedData();

                Assert.AreEqual(0, (int)count.GetValue(outgoing),
                    "Clearing a connection-owned sender must remove its global outbox node.");
            }
        }

        [Test]
        public void ConnectionOwnedDisposeRemovesGlobalPublication()
        {
            using (var live = new LiveConnection())
            {
                RosTopicState topic = live.Connection.RegisterPublisher<UInt8MultiArrayMsg>("/dispose-global-publication");
                live.Connection.Publish("/dispose-global-publication", new UInt8MultiArrayMsg());

                object outgoing = AttemptTestAccess.Root(live.Connection, "Outgoing");
                PropertyInfo count = outgoing.GetType().GetProperty("Count",
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                Assert.AreEqual(1, (int)count.GetValue(outgoing),
                    "The public publication must be present in the attempt-global outbox before disposal.");

                topic.MessageSender.Dispose();

                Assert.AreEqual(0, (int)count.GetValue(outgoing),
                    "Disposing a connection-owned sender must remove its global outbox node.");
            }
        }

        [Test]
        public void DisposingConnectionOwnedSenderAfterAttemptRetiresIsSafe()
        {
            using (var live = new LiveConnection())
            {
                RosTopicState topic = live.Connection.RegisterPublisher<UInt8MultiArrayMsg>("/dispose-retired-sender");
                live.Connection.Publish("/dispose-retired-sender", new UInt8MultiArrayMsg());
                live.Connection.Disconnect();

                Assert.DoesNotThrow(() => topic.MessageSender.Dispose(),
                    "Disposing a sender after its owning attempt is retired must be idempotent.");
            }
        }

    }
}
