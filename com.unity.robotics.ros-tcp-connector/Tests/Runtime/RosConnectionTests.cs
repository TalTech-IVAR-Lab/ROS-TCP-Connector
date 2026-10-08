using System.Collections;
using System.Collections.Generic;
using NUnit.Framework;
using RosMessageTypes.BuiltinInterfaces;
using RosMessageTypes.Std;
using RosMessageTypes.Geometry;
using RosMessageTypes.Tf2;
using Unity.Robotics.ROSTCPConnector;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using Unity.Robotics.ROSTCPConnector.ROSGeometry;
using UnityEngine;
using UnityEngine.TestTools;
using UnityEngine.Serialization;
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Text.RegularExpressions;

namespace UnitTests
{
    public class RosConnectionTests
    {
        static void AssertThrowsAsync<TException>(Func<Task> action) where TException : Exception
        {
            Exception caught = null;
            try
            {
                action().GetAwaiter().GetResult();
            }
            catch (Exception exception)
            {
                caught = exception;
            }

            Assert.NotNull(caught, $"Expected {typeof(TException).Name}, but no exception was thrown.");
            Assert.AreEqual(typeof(TException), caught.GetType());
        }

        class ThrowingCleanupSender : OutgoingMessageSender
        {
            public override SendToState SendInternal(MessageSerializer messageSerializer, Stream stream) => SendToState.NoMessageToSendError;
            public override void ClearAllQueuedData() => throw new InvalidOperationException("cleanup failure");
        }
        class BlockingPool : IMessagePool
        {
            public readonly ManualResetEventSlim Entered = new ManualResetEventSlim(false);
            public readonly ManualResetEventSlim Release = new ManualResetEventSlim(false);
            public void AddMessage(Message messageToRecycle)
            {
                Entered.Set();
                Release.Wait();
            }
        }

        class ReentrantThrowingPool : IMessagePool
        {
            readonly ROSConnection m_Connection;
            public readonly ManualResetEventSlim Entered = new ManualResetEventSlim(false);
            public ReentrantThrowingPool(ROSConnection connection) { m_Connection = connection; }
            public void AddMessage(Message messageToRecycle)
            {
                m_Connection.Disconnect();
                Entered.Set();
                throw new InvalidOperationException("pool failure");
            }
        }
        static ROSConnectionConfig Config(string address, int port)
        {
            var config = ScriptableObject.CreateInstance<ROSConnectionConfig>();
            typeof(ROSConnectionConfig).GetField("m_RosIPAddress", BindingFlags.Instance | BindingFlags.NonPublic).SetValue(config, address);
            typeof(ROSConnectionConfig).GetField("m_RosPort", BindingFlags.Instance | BindingFlags.NonPublic).SetValue(config, port);
            return config;
        }

        static void SetAttempt(ROSConnection connection, long attemptId)
        {
            AttemptTestAccess.Replace(connection, attemptId);
        }

        static void SetLiveAttempt(ROSConnection connection, long attemptId)
        {
            SetAttempt(connection, attemptId);
            object attempt = AttemptTestAccess.Current(connection);
            FieldInfo state = attempt.GetType().GetField("State", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            state.SetValue(attempt, Enum.Parse(state.FieldType, "Connecting"));
        }

        static void ClearQueue(ROSConnection connection, object queue)
        {
            MethodInfo clear = queue.GetType().GetMethod("ClearQueuedData", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            MethodInfo clearEntry = typeof(ROSConnection).GetMethod("ClearOutgoingEntry", BindingFlags.Instance | BindingFlags.NonPublic);
            Delegate callback = Delegate.CreateDelegate(clear.GetParameters()[0].ParameterType, connection, clearEntry);
            clear.Invoke(queue, new object[] { callback });
        }

        static int ExactEntryCount(ROSConnection connection)
        {
            object outgoing = AttemptTestAccess.Root(connection, "Outgoing");
            FieldInfo entriesField = outgoing.GetType().GetField("m_Entries", BindingFlags.Instance | BindingFlags.NonPublic);
            int count = 0;
            foreach (object entry in (IEnumerable)entriesField.GetValue(outgoing))
            {
                FieldInfo kindField = entry.GetType().GetField("Kind", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                if (string.Equals(kindField.GetValue(entry).ToString(), "Exact", StringComparison.Ordinal))
                    count++;
            }
            return count;
        }

        static object Session(ROSConnection connection)
        {
            return typeof(ROSConnection).GetField("m_WorkerSession", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(connection);
        }

        static CancellationTokenSource SessionCancellation(ROSConnection connection)
        {
            object session = Session(connection);
            return (CancellationTokenSource)session.GetType().GetField("Cancellation", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(session);
        }

        static byte[] Serialize(Message message)
        {
            var serializer = new MessageSerializer();
            serializer.SerializeMessage(message);
            return serializer.GetBytes();
        }

        class Slice1Response : Message
        {
            public Slice1Response() { }
            public int Value;
        }

        class Slice1ObserverResponse : Message { }

        class FailingServiceRequest : TimeMsg
        {
            readonly Exception m_Failure;
            public FailingServiceRequest(Exception failure) { m_Failure = failure; }
            public override void SerializeTo(MessageSerializer serializer) { throw m_Failure; }
        }

        sealed class BlockingPublishMessage : TimeMsg, IDisposable
        {
            public readonly ManualResetEventSlim Entered = new ManualResetEventSlim(false);
            public readonly ManualResetEventSlim Release = new ManualResetEventSlim(false);

            public override void SerializeTo(MessageSerializer serializer)
            {
                Entered.Set();
                Release.Wait();
                base.SerializeTo(serializer);
            }

            public void Dispose()
            {
                Release.Set();
                Release.Dispose();
                Entered.Dispose();
            }
        }

        // Real Connect/admission/wire request; only incoming Update dispatch is driven explicitly.
        sealed class ServicePublicationFixture : IDisposable
        {
            readonly GameObject m_GameObject = new GameObject("service publication");
            readonly TcpListener m_Listener = new TcpListener(IPAddress.Loopback, 0);
            readonly ROSConnectionConfig m_Config;
            TcpClient m_Peer;
            public readonly ROSConnection Connection;
            public readonly long AttemptId;
            public object Attempt;

            public ServicePublicationFixture(string responseName = TimeMsg.k_RosMessageName, int queueSize = 10)
            {
                Connection = m_GameObject.AddComponent<ROSConnection>();
                Connection.ConnectOnStart = false;
                Connection.listenForTFMessages = false;
                m_Listener.Start();
                m_Config = Config("127.0.0.1", ((IPEndPoint)m_Listener.LocalEndpoint).Port);
                Connection.ConnectionConfig = m_Config;
                Connection.RegisterRosService("/ros", TimeMsg.k_RosMessageName, responseName, queueSize);
                Task<TcpClient> accept = m_Listener.AcceptTcpClientAsync();
                Connection.Connect();
                Assert.IsTrue(accept.Wait(TimeSpan.FromSeconds(5)));
                m_Peer = accept.Result;
                m_Peer.GetStream().ReadTimeout = 5000;
                AttemptId = AttemptTestAccess.Id(Connection);
                Attempt = AttemptTestAccess.Current(Connection);
            }

            public int ReadRequestId()
            {
                Tuple<string, byte[]> frame;
                do { frame = ReadFrame(m_Peer.GetStream()); }
                while (frame.Item1 != SysCommand.k_SysCommand_ServiceRequest);
                int id = JsonUtility.FromJson<SysCommand_Service>(System.Text.Encoding.UTF8.GetString(frame.Item2)).srv_id;
                Assert.AreEqual("/ros", ReadFrame(m_Peer.GetStream()).Item1);
                return id;
            }

            public void Respond(int id, string topic = "/ros", object attempt = null, TimeMsg response = null)
            {
                DispatchFrame(Connection, attempt ?? Attempt, SysCommand.k_SysCommand_ServiceResponse,
                    System.Text.Encoding.UTF8.GetBytes(JsonUtility.ToJson(new SysCommand_Service { srv_id = id })));
                DispatchFrame(Connection, attempt ?? Attempt, topic, Serialize(response ?? new TimeMsg(8, 8)));
            }

            public void Reconnect()
            {
                Connection.Disconnect();
                Assert.IsTrue(SpinWait.SpinUntil(() => !Connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                m_Peer.Close();
                Task<TcpClient> accept = m_Listener.AcceptTcpClientAsync();
                Connection.Connect();
                Assert.IsTrue(accept.Wait(TimeSpan.FromSeconds(5)));
                m_Peer = accept.Result;
                m_Peer.GetStream().ReadTimeout = 5000;
                Attempt = AttemptTestAccess.Current(Connection);
                Assert.AreNotEqual(AttemptId, AttemptTestAccess.Id(Connection));
            }

            public void Dispose()
            {
                Connection.Disconnect();
                bool stopped = SpinWait.SpinUntil(() => !Connection.HasConnectionThread, TimeSpan.FromSeconds(5));
                m_Peer.Close();
                m_Listener.Stop();
                UnityEngine.Object.DestroyImmediate(m_GameObject);
                UnityEngine.Object.DestroyImmediate(m_Config);
                Assert.IsTrue(stopped, "The service fixture must leave no live connection worker.");
            }
        }

        static void DispatchFrame(ROSConnection connection, object attempt, string topic, byte[] data)
        {
            Type frameType = typeof(ROSConnection).GetNestedType("IncomingMessage", BindingFlags.NonPublic);
            object frame = Activator.CreateInstance(frameType, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                null, new object[] { attempt, topic, data }, null);
            typeof(ROSConnection).GetMethod("DispatchIncomingMessage", BindingFlags.Instance | BindingFlags.NonPublic)
                .Invoke(connection, new[] { frame });
        }

        sealed class ServiceDispatchContext : SynchronizationContext
        {
            readonly Queue<Action> m_Work = new Queue<Action>();

            public override void Post(SendOrPostCallback callback, object state)
            {
                lock (m_Work) m_Work.Enqueue(() => callback(state));
            }

            public T Run<T>(Func<T> action)
            {
                SynchronizationContext previous = Current;
                try
                {
                    SetSynchronizationContext(this);
                    return action();
                }
                finally { SetSynchronizationContext(previous); }
            }

            public void RunOne()
            {
                Assert.IsTrue(SpinWait.SpinUntil(() => { lock (m_Work) return m_Work.Count != 0; },
                    TimeSpan.FromSeconds(2)), "A captured-context continuation must be queued.");
                Action action;
                lock (m_Work) action = m_Work.Dequeue();
                Run(() => { action(); return true; });
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public void ResponseDeserializerCrossingDisconnectCannotPublishResult(bool throwAfterRelease)
        {
            using (var fixture = new ServicePublicationFixture())
            using (var entered = new ManualResetEventSlim(false))
            using (var release = new ManualResetEventSlim(false))
            using (var exited = new ManualResetEventSlim(false))
            {
                MessageRegistry.Register<Slice1Response>("slice1/BlockingResponse/" + throwAfterRelease, decoder =>
                {
                    entered.Set();
                    try
                    {
                        release.Wait();
                        if (throwAfterRelease) throw new InvalidOperationException("late registered decoder fault");
                        return new Slice1Response { Value = 8 };
                    }
                    finally { exited.Set(); }
                });
                var context = new ServiceDispatchContext();
                Task<Slice1Response> request = context.Run(() =>
                    fixture.Connection.SendServiceMessage<Slice1Response>("/ros", new TimeMsg()));
                try
                {
                    fixture.Respond(fixture.ReadRequestId());
                    Task<bool> invalidation = Task.Run(() =>
                    {
                        try
                        {
                            Assert.IsTrue(entered.Wait(TimeSpan.FromSeconds(2)), "The registered RESPONSE decoder must actually start.");
                            Task disconnect = Task.Run(() => fixture.Connection.Disconnect());
                            Assert.IsTrue(disconnect.Wait(TimeSpan.FromSeconds(2)), "Disconnect must not wait for the registered decoder.");
                            return SpinWait.SpinUntil(() => request.IsCompleted, TimeSpan.FromSeconds(1));
                        }
                        finally { release.Set(); }
                    });
                    context.RunOne(); // Decode on the captured/main context, while another thread disconnects.
                    bool terminalWhileBlocked = invalidation.GetAwaiter().GetResult();
                    Assert.IsTrue(exited.Wait(TimeSpan.FromSeconds(2)));
                    Assert.IsTrue(SpinWait.SpinUntil(() => request.IsCompleted, TimeSpan.FromSeconds(2)));
                    Exception observed = request.Exception;
                    Assert.IsTrue(terminalWhileBlocked && (request.IsFaulted || request.IsCanceled),
                        $"Public Task must fail while decoder is blocked; terminalWhileBlocked={terminalWhileBlocked}, afterRelease={request.Status}.");
                    Assert.IsInstanceOf<IOException>(request.Exception.InnerException,
                        "A late decoder fault must be observed without replacing the committed invalidation outcome.");
                }
                finally
                {
                    release.Set();
                    if (entered.IsSet) Assert.IsTrue(exited.Wait(TimeSpan.FromSeconds(2)));
                }
            }
        }

        [Test]
        public void InvalidatedDecoderRetainsExecutionTicketUntilFinally()
        {
            const string responseName = "slice7/BlockingDecoder";
            using (var fixture = new ServicePublicationFixture(responseName, 1))
            using (var entered = new ManualResetEventSlim(false))
            using (var release = new ManualResetEventSlim(false))
            using (var exited = new ManualResetEventSlim(false))
            {
                MessageRegistry.Register<Slice1Response>(responseName, decoder =>
                {
                    entered.Set();
                    try
                    {
                        release.Wait();
                        return new Slice1Response { Value = 17 };
                    }
                    finally { exited.Set(); }
                });
                Assert.NotNull(MessageRegistry.GetDeserializeFunction<Slice1Response>(),
                    "The registered generic response decoder must be available before the service call.");

                var context = new ServiceDispatchContext();
                Task<Slice1Response> first = context.Run(() =>
                    fixture.Connection.SendServiceMessage<Slice1Response>("/ros", new TimeMsg()));
                int firstId = fixture.ReadRequestId();
                fixture.Respond(firstId);
                Task<Slice1Response> second = null;
                Task lifecycle = Task.Run(() =>
                {
                    Assert.IsTrue(entered.Wait(TimeSpan.FromSeconds(2)),
                        "The response decoder must claim its execution ticket.");
                    fixture.Connection.Disconnect();
                    Assert.IsTrue(first.IsFaulted, "Disconnect must fail the public task before decoder return.");
                    fixture.Reconnect();
                    second = fixture.Connection.SendServiceMessage<Slice1Response>("/ros", new TimeMsg());
                    Assert.IsTrue(second.IsFaulted,
                        "A replacement attempt must not start a decoder while the old decoder ticket is held.");
                    Assert.IsInstanceOf<IOException>(second.Exception.InnerException);
                    release.Set();
                });
                context.RunOne();
                Assert.IsTrue(lifecycle.Wait(TimeSpan.FromSeconds(5)), "The lifecycle coordinator must finish.");
                lifecycle.GetAwaiter().GetResult();
                Assert.IsTrue(exited.Wait(TimeSpan.FromSeconds(2)), "The blocked decoder must eventually return.");
                FieldInfo decoderTickets = typeof(RosTopicState).GetField(
                    "m_ServiceDecoderTickets", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.NotNull(decoderTickets, "The service must expose an execution-ticket counter to its decoder owner.");
                Assert.IsTrue(SpinWait.SpinUntil(() => (int)decoderTickets.GetValue(fixture.Connection.GetTopic("/ros")) == 0,
                    TimeSpan.FromSeconds(2)), "The decoder ticket must be released after decoder return.");

                Task<Slice1Response> third = fixture.Connection.SendServiceMessage<Slice1Response>("/ros", new TimeMsg());
                Assert.IsFalse(third.IsFaulted, "A later call may reserve the ticket after the decoder finally returns.");
            }
        }

        [Test]
        public void InvalidatedDecoderBeforeClaimReleasesExecutionTicket()
        {
            const string responseName = "slice7/BeforeClaimDecoder";
            using (var fixture = new ServicePublicationFixture(responseName, 1))
            using (var continuationEntered = new ManualResetEventSlim(false))
            {
                int decoderInvocations = 0;
                MessageRegistry.Register<Slice1Response>(responseName, decoder =>
                {
                    Interlocked.Increment(ref decoderInvocations);
                    return new Slice1Response { Value = 23 };
                });
                Assert.NotNull(MessageRegistry.GetDeserializeFunction<Slice1Response>(),
                    "The registered generic response decoder must be available before the service call.");

                FieldInfo continuationHook = typeof(ROSConnection).GetField(
                    "m_ServiceResponseContinuationTestHook", BindingFlags.Instance | BindingFlags.NonPublic);
                FieldInfo decoderTickets = typeof(RosTopicState).GetField(
                    "m_ServiceDecoderTickets", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.NotNull(continuationHook,
                    "Response decoding needs a pre-claim authority barrier.");
                Assert.NotNull(decoderTickets,
                    "The service must expose an execution-ticket counter to its decoder owner.");

                RosTopicState topic = fixture.Connection.GetTopic("/ros");
                continuationHook.SetValue(fixture.Connection, new Action(() =>
                {
                    continuationEntered.Set();
                    fixture.Connection.Disconnect();
                }));
                try
                {
                    var context = new ServiceDispatchContext();
                    Task<Slice1Response> request = context.Run(() =>
                        fixture.Connection.SendServiceMessage<Slice1Response>("/ros", new TimeMsg()));
                    fixture.Respond(fixture.ReadRequestId());
                    context.RunOne();

                    Assert.IsTrue(continuationEntered.IsSet,
                        "The response continuation must reach the pre-claim authority barrier.");
                    Assert.IsTrue(request.IsFaulted,
                        "Disconnect must invalidate the public result before decoder claim.");
                    Assert.IsInstanceOf<IOException>(request.Exception.InnerException);
                    Assert.AreEqual(0, decoderInvocations,
                        "An invalidated pre-claim response must not invoke the registered decoder.");
                    Assert.AreEqual(0, (int)decoderTickets.GetValue(topic),
                        "An unclaimed decoder ticket must be released during invalidation.");
                }
                finally
                {
                    continuationHook.SetValue(fixture.Connection, null);
                }
            }
        }

        [UnityTest]
        public IEnumerator UnityServiceRequestDeserializerFailureIsObservedWithoutResponse()
        {
            using (var fixture = new ServicePublicationFixture())
            {
                RosTopicState topic = fixture.Connection.GetTopic("/ros");
                FieldInfo implementation = typeof(RosTopicState).GetField(
                    "m_ServiceImplementation", BindingFlags.Instance | BindingFlags.NonPublic);
                FieldInfo deserializer = typeof(RosTopicState).GetField(
                    "m_Deserializer", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.NotNull(implementation);
                Assert.NotNull(deserializer);
                implementation.SetValue(topic, new Func<Message, Message>(_ => new TimeMsg(1, 1)));

                int controlledErrors = 0;
                Application.LogCallback handler = (condition, stackTrace, type) =>
                {
                    if (condition.IndexOf("Unity service '/ros' request handling failed", StringComparison.Ordinal) >= 0)
                        Interlocked.Increment(ref controlledErrors);
                };
                bool previousIgnore = LogAssert.ignoreFailingMessages;
                Application.logMessageReceivedThreaded += handler;
                LogAssert.ignoreFailingMessages = true;
                try
                {
                    deserializer.SetValue(topic, new Func<MessageDeserializer, Message>(_ =>
                        throw new InvalidOperationException("controlled request decoder failure")));
                    MethodInfo handle = typeof(RosTopicState).GetMethod(
                        "HandleUnityServiceRequest", BindingFlags.Instance | BindingFlags.NonPublic);
                    Assert.DoesNotThrow(() => handle.Invoke(topic,
                        new object[] { Serialize(new TimeMsg()), 901, fixture.Attempt }));
                    for (int frame = 0; frame < 10 && Volatile.Read(ref controlledErrors) == 0; frame++)
                        yield return null;

                    Assert.AreEqual(1, Volatile.Read(ref controlledErrors),
                        "Request deserializer failures must terminate at the service boundary.");
                    Assert.AreEqual(0, ExactEntryCount(fixture.Connection),
                        "A failed request decode must not admit a response pair.");
                }
                finally
                {
                    Application.logMessageReceivedThreaded -= handler;
                    LogAssert.ignoreFailingMessages = previousIgnore;
                }
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public void CallbackWrapperRevalidatesAfterDecodedContinuation(bool reconnect)
        {
            using (var fixture = new ServicePublicationFixture())
            {
                var context = new ServiceDispatchContext();
                int callbacks = 0;
                context.Run(() =>
                {
                    fixture.Connection.SendServiceMessage<TimeMsg>("/ros", new TimeMsg(), _ => callbacks++);
                    return true;
                });
                fixture.Respond(fixture.ReadRequestId());
                context.RunOne(); // Typed success has committed; the wrapper is still queued.
                Assert.AreEqual(0, callbacks, "The fixture must hold dispatch before the callback permit.");
                fixture.Connection.Disconnect();
                if (reconnect) fixture.Reconnect();
                LogAssert.Expect(LogType.Error, new Regex("Exception sending service request or invoking service callback"));
                context.RunOne();
                Assert.AreEqual(0, callbacks, "A decoded result must not grant a callback permit after revocation.");
            }
        }

        [Test]
        public void CallbackPermittedBeforeDisconnectCanFinishAfterDisconnect()
        {
            using (var fixture = new ServicePublicationFixture())
            using (var entered = new ManualResetEventSlim(false))
            using (var release = new ManualResetEventSlim(false))
            {
                var context = new ServiceDispatchContext();
                int finished = 0;
                context.Run(() =>
                {
                    fixture.Connection.SendServiceMessage<TimeMsg>("/ros", new TimeMsg(), response =>
                    {
                        Assert.AreSame(context, SynchronizationContext.Current);
                        Assert.AreEqual(8, response.sec);
                        entered.Set();
                        release.Wait();
                        fixture.Connection.Disconnect(); // Reentrant stop is legal too.
                        finished++;
                    });
                    return true;
                });
                fixture.Respond(fixture.ReadRequestId());
                context.RunOne();
                Task stop = Task.Run(() =>
                {
                    try
                    {
                        Assert.IsTrue(entered.Wait(TimeSpan.FromSeconds(2)), "The callback must first acquire its permit.");
                        Task disconnect = Task.Run(() => fixture.Connection.Disconnect());
                        Assert.IsTrue(disconnect.Wait(TimeSpan.FromSeconds(2)), "Disconnect must finish while the callback is blocked.");
                        Assert.AreEqual(0, finished);
                    }
                    finally { release.Set(); }
                });
                try
                {
                    context.RunOne();
                    stop.GetAwaiter().GetResult();
                    Assert.AreEqual(1, finished, "A pre-revocation permit is not retroactively revoked.");
                }
                finally { release.Set(); }
            }
        }

        [Test]
        public void CommittedServiceSuccessCanBeObservedAfterDisconnect()
        {
            using (var fixture = new ServicePublicationFixture())
            {
                var context = new ServiceDispatchContext();
                Task<TimeMsg> request = context.Run(() =>
                    fixture.Connection.SendServiceMessage<TimeMsg>("/ros", new TimeMsg()));
                Task<TimeMsg> consumer = context.Run(async () => await request);
                fixture.Respond(fixture.ReadRequestId());
                context.RunOne();
                Assert.AreEqual(TaskStatus.RanToCompletion, request.Status);
                Assert.IsFalse(consumer.IsCompleted, "Consumer observation must still be queued.");
                fixture.Connection.Disconnect();
                context.RunOne();
                Assert.AreEqual(8, consumer.GetAwaiter().GetResult().sec);
                Assert.AreSame(request.Result, consumer.Result);
            }
        }

        [Test]
        public void ServiceResponseRequiresExactTopicBeforeConsumingCall()
        {
            using (var fixture = new ServicePublicationFixture())
            {
                var context = new ServiceDispatchContext();
                Task<TimeMsg> request = context.Run(() =>
                    fixture.Connection.SendServiceMessage<TimeMsg>("/ros", new TimeMsg()));
                int id = fixture.ReadRequestId();
                LogAssert.Expect(LogType.Error, new Regex("Unable to route service response on \"/ROS\""));
                fixture.Respond(id, "/ROS", response: new TimeMsg(3, 3));
                fixture.Respond(id);
                context.RunOne();
                Assert.AreEqual(8, request.GetAwaiter().GetResult().sec,
                    "A matching ID on a different topic must not consume the pending call.");
            }
        }

        [Test]
        public void SelectedServiceInvalidationCompletesEvenWhenCancellationThrows()
        {
            using (var fixture = new ServicePublicationFixture())
            {
                var context = new ServiceDispatchContext();
                Task<TimeMsg> request = context.Run(() =>
                    fixture.Connection.SendServiceMessage<TimeMsg>("/ros", new TimeMsg()));
                fixture.ReadRequestId();
                CancellationTokenSource cancellation = SessionCancellation(fixture.Connection);
                using (cancellation.Token.Register(() => { throw new InvalidOperationException("slice1 cancellation probe"); }))
                {
                    Assert.Throws<AggregateException>(() => fixture.Connection.Disconnect());
                    Assert.IsTrue(SpinWait.SpinUntil(() => request.IsCompleted, TimeSpan.FromSeconds(1)),
                        "Once invalidation owns the outcome, another stop phase must not strand the public Task.");
                    Assert.IsInstanceOf<IOException>(request.Exception.InnerException);
                    context.RunOne(); // The private raw await was also released, without a response.
                }
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public void ServiceResponseRequiresExactAttemptAndId(bool wrongAttempt)
        {
            using (var fixture = new ServicePublicationFixture())
            {
                var context = new ServiceDispatchContext();
                Task<TimeMsg> request = context.Run(() =>
                    fixture.Connection.SendServiceMessage<TimeMsg>("/ros", new TimeMsg()));
                int id = fixture.ReadRequestId();
                if (!wrongAttempt)
                    LogAssert.Expect(LogType.Error, new Regex("Unable to route service response"));
                fixture.Respond(wrongAttempt ? id : id + 1, attempt: wrongAttempt ? AttemptTestAccess.New(fixture.AttemptId + 1) : fixture.Attempt,
                    response: new TimeMsg(3, 3));
                fixture.Respond(id);
                context.RunOne();
                Assert.AreEqual(8, request.GetAwaiter().GetResult().sec);
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public void ServiceResponsePreservesObserverThenRequestedDecoderOrder(bool disconnectInObserver)
        {
            var order = new List<string>();
            string observerName = "slice1/Observed/" + disconnectInObserver;
            MessageRegistry.Register<Slice1ObserverResponse>(observerName, decoder =>
            {
                order.Add("observer decode");
                return new Slice1ObserverResponse();
            }, MessageSubtopic.Response);
            MessageRegistry.Register<Slice1Response>("slice1/Requested/" + disconnectInObserver, decoder =>
            {
                order.Add("requested decode");
                return new Slice1Response { Value = 8 };
            });
            using (var fixture = new ServicePublicationFixture(observerName))
            {
                var context = new ServiceDispatchContext();
                RosTopicState topic = fixture.Connection.GetTopic("/ros");
                topic.ServiceResponseTopic.AddSubscriber(message =>
                {
                    Assert.IsInstanceOf<Slice1ObserverResponse>(message);
                    Assert.AreSame(context, SynchronizationContext.Current);
                    order.Add("observer callback");
                    if (disconnectInObserver) fixture.Connection.Disconnect();
                });
                topic.ServiceResponseTopic.AddSubscriber(_ => order.Add("second observer"));
                Task<Slice1Response> request = context.Run(() =>
                    fixture.Connection.SendServiceMessage<Slice1Response>("/ros", new TimeMsg()));
                fixture.Respond(fixture.ReadRequestId());
                context.RunOne();
                if (disconnectInObserver)
                {
                    CollectionAssert.AreEqual(new[] { "observer decode", "observer callback" }, order);
                    Assert.IsInstanceOf<IOException>(request.Exception.InnerException);
                }
                else
                {
                    CollectionAssert.AreEqual(new[] { "observer decode", "observer callback", "second observer", "requested decode" }, order);
                    Assert.AreEqual(8, request.GetAwaiter().GetResult().Value);
                }
            }
        }

        [TestCase("validation")]
        [TestCase("notification")]
        [TestCase("serialization")]
        [TestCase("cancellation")]
        public void ServicePreparationFailuresRemainOnReturnedTask(string phase)
        {
            using (var fixture = new ServicePublicationFixture())
            using (var cancellation = new CancellationTokenSource())
            {
                cancellation.Cancel();
                Exception failure = phase == "cancellation" ? (Exception)new OperationCanceledException(cancellation.Token)
                    : new InvalidOperationException("service preparation probe");
                bool notified = false;
                fixture.Connection.GetTopic("/ros").AddSubscriber(_ =>
                {
                    notified = true;
                    if (phase == "notification" || phase == "cancellation") throw failure;
                });
                Message value = new TimeMsg();
                if (phase == "validation") value = null;
                if (phase == "serialization") value = new FailingServiceRequest(failure);
                Task<TimeMsg> request = null;
                Assert.DoesNotThrow(() => request = fixture.Connection.SendServiceMessage<TimeMsg>("/ros", value),
                    "Synchronous preparation failures must not newly escape the Task-returning entry point.");
                Assert.IsTrue(request.IsCompleted, "Preparation still finishes synchronously.");
                Assert.AreEqual(phase != "validation", notified, "Request notifications keep their synchronous timing.");
                if (phase == "cancellation")
                {
                    Assert.IsTrue(request.IsCanceled);
                    OperationCanceledException outcome = Assert.Catch<OperationCanceledException>(() => request.GetAwaiter().GetResult());
                    Assert.AreEqual(cancellation.Token, outcome.CancellationToken);
                }
                else if (phase == "validation")
                    Assert.IsInstanceOf<NullReferenceException>(request.Exception.InnerException);
                else
                    Assert.AreSame(failure, request.Exception.InnerException);
                var pending = (IDictionary)AttemptTestAccess.Root(fixture.Connection, "Calls");
                Assert.AreEqual(0, pending.Count);
            }
        }

        [Test]
        public void BlockingPoolOverflowCannotDelayDisconnectAdmissionCleanup()
        {
            var gameObject = new GameObject("blocking pool");
            var pool = new BlockingPool();
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                SetAttempt(connection, 10);
                RosTopicState topic = connection.RegisterPublisher<TimeMsg>("/pool", 1);
                topic.SetMessagePool(pool);
                topic.Publish(new TimeMsg(1, 1));
                topic.Publish(new TimeMsg(2, 2));
                Assert.IsTrue(pool.Entered.Wait(TimeSpan.FromSeconds(2)));
                Task disconnect = Task.Run(() => connection.Disconnect());
                Assert.IsTrue(disconnect.Wait(TimeSpan.FromSeconds(1)), "Pool recycling must not own Disconnect.");
                Assert.IsFalse(connection.HasConnectionThread);
            }
            finally
            {
                pool.Release.Set();
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void ThrowingReentrantPoolCannotDeadlockOrCarryPayloadIntoNextAttempt()
        {
            var gameObject = new GameObject("reentrant pool");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                SetAttempt(connection, 20);
                RosTopicState topic = connection.RegisterPublisher<TimeMsg>("/pool", 1);
                var pool = new ReentrantThrowingPool(connection);
                topic.SetMessagePool(pool);
                var logged = new ManualResetEventSlim(false);
                Application.LogCallback handler = (condition, stackTrace, type) =>
                {
                    if (condition.Contains("pool failure")) logged.Set();
                };
                Application.logMessageReceivedThreaded += handler;
                topic.Publish(new TimeMsg(1, 1));
                topic.Publish(new TimeMsg(2, 2));
                Assert.IsTrue(pool.Entered.Wait(TimeSpan.FromSeconds(2)));
                Assert.IsTrue(logged.Wait(TimeSpan.FromSeconds(2)));
                Application.logMessageReceivedThreaded -= handler;
                SetAttempt(connection, 21);
                Assert.AreEqual(OutgoingMessageSender.SendToState.NoMessageToSendError,
                    topic.MessageSender.SendInternal(new MessageSerializer(), new MemoryStream()));
            }
            finally { UnityEngine.Object.DestroyImmediate(gameObject); }
        }

        [Test]
        public void GetOrCreateInstance_CallOnce_ReturnsValidInstance()
        {
            ROSConnection ros = ROSConnection.GetOrCreateInstance();
            ros.ConnectOnStart = false;
            Assert.NotNull(ros);
        }

        [Test]
        public void GetOrCreateInstance_CallTwice_ReturnsSameInstance()
        {
            ROSConnection ros = ROSConnection.GetOrCreateInstance();
            Assert.NotNull(ros);
            ros.ConnectOnStart = false;
            ROSConnection ros2 = ROSConnection.GetOrCreateInstance();
            Assert.AreEqual(ros, ros2);
        }

        [Test]
        public void DuplicateConnectKeepsSingleWorkerAndStoppingStateBlocksConfigReplacement()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            var gameObject = new GameObject("lifecycle connection");
            ROSConnectionConfig first = null;
            ROSConnectionConfig second = null;
            TcpClient accepted = null;
            try
            {
                listener.Start();
                int port = ((IPEndPoint)listener.LocalEndpoint).Port;
                first = Config("127.0.0.1", port);
                second = Config("localhost", port);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectionConfig = first;
                Task<TcpClient> acceptTask = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(acceptTask.Wait(TimeSpan.FromSeconds(5)));
                accepted = acceptTask.Result;
                object worker = Session(connection);

                connection.Connect();
                Assert.AreSame(worker, Session(connection));

                connection.Disconnect();
                Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                connection.ConnectionConfig = second;
                Assert.AreSame(second, connection.ConnectionConfig);
            }
            finally
            {
                accepted?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (first != null) UnityEngine.Object.DestroyImmediate(first);
                if (second != null) UnityEngine.Object.DestroyImmediate(second);
            }
        }

        [Test]
        public void UnexpectedWorkerCompletionDoesNotAuthorizeConfigReplacement()
        {
            using (var fixture = new ServicePublicationFixture())
            {
                ROSConnectionConfig second = Config("127.0.0.1", 2);
                try
                {
                    // Terminate the actual worker without publishing explicit-stop intent.
                    SessionCancellation(fixture.Connection).Cancel();
                    Assert.IsTrue(SpinWait.SpinUntil(() => !fixture.Connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                    Assert.Throws<InvalidOperationException>(() => fixture.Connection.ConnectionConfig = second);
                    fixture.Connection.Disconnect();
                    fixture.Connection.ConnectionConfig = second;
                    Assert.AreSame(second, fixture.Connection.ConnectionConfig);
                }
                finally { UnityEngine.Object.DestroyImmediate(second); }
            }
        }

        [Test]
        public void ConnectionLossClearsPairedFrameHandlerAndFailsPendingServices()
        {
            var gameObject = new GameObject("connection loss");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                const long attemptId = 77;
                SetLiveAttempt(connection, attemptId);
                typeof(ROSConnection).GetMethod("ReceiveSysCommand", BindingFlags.Instance | BindingFlags.NonPublic)
                    .Invoke(connection, new object[] { AttemptTestAccess.Current(connection), SysCommand.k_SysCommand_ServiceResponse, "{\"srv_id\":1}" });
                connection.RegisterRosService<TimeMsg, TimeMsg>("/pending");
                Task<TimeMsg> completion = connection.SendServiceMessage<TimeMsg>("/pending", new TimeMsg());
                connection.GetTopicList(_ => Assert.Fail("A stale one-shot callback must not run."));

                AttemptTestAccess.Retire(connection);

                Assert.IsNull(AttemptTestAccess.Root(connection, "PendingPair"));
                Assert.IsTrue(completion.IsFaulted);
                Assert.IsInstanceOf<IOException>(completion.Exception.InnerException);
                var pending = (IDictionary)AttemptTestAccess.Root(connection, "Calls");
                Assert.AreEqual(0, pending.Count);
                var topicCallbacks = (System.Collections.ICollection)AttemptTestAccess.Root(connection, "TopicCallbacks");
                Assert.AreEqual(0, topicCallbacks.Count);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void ServiceRequestWithoutLiveAttemptIsRejectedAndDoesNotLeakWaiter()
        {
            var gameObject = new GameObject("failed service request");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                Task<TimeMsg> request = connection.SendServiceMessage<TimeMsg>("/unregistered", new TimeMsg());

                AssertThrowsAsync<IOException>(() => request);

                Assert.IsNull(AttemptTestAccess.Current(connection), "No attempt or pending-call owner was created.");
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [UnityTest]
        public IEnumerator CallbackServiceFailureIsCaughtAndLogged()
        {
            var gameObject = new GameObject("callback service failure");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                LogAssert.Expect(LogType.Error, new Regex("Exception sending service request or invoking service callback"));
                connection.SendServiceMessage<TimeMsg>("/unavailable", new TimeMsg(), _ => { });
                yield return null;
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void StaleIncomingFrameIsRejectedAtDispatchBoundary()
        {
            var gameObject = new GameObject("stale frame connection");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                Type frameType = typeof(ROSConnection).GetNestedType("IncomingMessage", BindingFlags.NonPublic);
                Assert.NotNull(frameType, "Incoming frames must carry private attempt identity.");
                MethodInfo dispatch = typeof(ROSConnection).GetMethod("DispatchIncomingMessage", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.NotNull(dispatch);


                bool invoked = false;
                connection.Subscribe<TimeMsg>("/stale", _ => invoked = true);
                var serializer = new MessageSerializer();
                serializer.SerializeMessage(new TimeMsg());
                object staleFrame = Activator.CreateInstance(frameType, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                    null, new object[] { AttemptTestAccess.New(41L), "/stale", serializer.GetBytes() }, null);
                SetAttempt(connection, 42L);

                dispatch.Invoke(connection, new[] { staleFrame });

                Assert.IsFalse(invoked);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public void TopicListCallbacksStopWhenFirstCallbackInvalidatesAttempt(bool includeTypes)
        {
            var gameObject = new GameObject("topic list callback invalidation");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                const long attemptId = 42;
                SetLiveAttempt(connection, attemptId);
                int secondCallbackCount = 0;
                Action invalidate = () => AttemptTestAccess.Retire(connection);

                if (includeTypes)
                {
                    connection.GetTopicAndTypeList(_ => invalidate());
                    connection.GetTopicAndTypeList(_ => secondCallbackCount++);
                }
                else
                {
                    connection.GetTopicList(_ => invalidate());
                    connection.GetTopicList(_ => secondCallbackCount++);
                }

                string json = JsonUtility.ToJson(new SysCommand_TopicsResponse
                {
                    topics = new[] { "/topic" },
                    types = new[] { TimeMsg.k_RosMessageName }
                });
                typeof(ROSConnection).GetMethod("ReceiveSysCommand", BindingFlags.Instance | BindingFlags.NonPublic)
                    .Invoke(connection, new object[] { AttemptTestAccess.Current(connection), SysCommand.k_SysCommand_TopicList, json });

                Assert.AreEqual(0, secondCallbackCount);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public void TopicListRequestsRequireLiveAttemptAndDoNotJoinOfflineEpoch(bool includeTypes)
        {
            var gameObject = new GameObject("offline topic list admission");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                SetAttempt(connection, 95);
                connection.QueueSysCommand("__offline-anchor", new SysCommand_TopicsRequest());

                object attempt = AttemptTestAccess.Current(connection);
                object outgoing = AttemptTestAccess.Root(connection, "Outgoing");
                FieldInfo entriesField = outgoing.GetType().GetField("m_Entries", BindingFlags.Instance | BindingFlags.NonPublic);
                int initialOutgoingCount = ((ICollection)entriesField.GetValue(outgoing)).Count;
                int initialRawUnits = (int)attempt.GetType().GetField("RawUnits",
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic).GetValue(attempt);
                var callbacks = (ICollection)AttemptTestAccess.Root(connection, includeTypes ? "TypeCallbacks" : "TopicCallbacks");

                Assert.AreEqual(0, callbacks.Count);
                Assert.Throws<IOException>(() =>
                {
                    if (includeTypes)
                        connection.GetTopicAndTypeList(_ => Assert.Fail("An offline topic-list callback must not be admitted."));
                    else
                        connection.GetTopicList(_ => Assert.Fail("An offline topic-list callback must not be admitted."));
                });

                Assert.AreEqual(0, callbacks.Count, "Offline rejection must not retain a topic-list callback.");
                Assert.AreEqual(initialOutgoingCount, ((ICollection)entriesField.GetValue(outgoing)).Count,
                    "Offline rejection must not append a topic-list command to the epoch outbox.");
                Assert.AreEqual(initialRawUnits, (int)attempt.GetType().GetField("RawUnits",
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic).GetValue(attempt),
                    "Offline rejection must not consume raw command capacity.");
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void TopicListOutstandingRegistrationsHaveIndependentBound()
        {
            var gameObject = new GameObject("bounded topic list requests");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                SetLiveAttempt(connection, 96L);
                for (int index = 0; index < 256; index++)
                    connection.GetTopicList(_ => { });

                Assert.Throws<IOException>(() => connection.GetTopicList(_ => { }),
                    "The topic-list callback population must have its own bounded admission.");
                var callbacks = (ICollection)AttemptTestAccess.Root(connection, "TopicCallbacks");
                Assert.AreEqual(256, callbacks.Count,
                    "A rejected topic-list request must not retain its callback.");
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void TopicListCallbackRegisteredDuringDeliveryWaitsForNextResponse()
        {
            var gameObject = new GameObject("topic list delivery transaction");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                SetLiveAttempt(connection, 97L);
                int firstCallbackCount = 0;
                int callbackRegisteredDuringDeliveryCount = 0;
                connection.GetTopicAndTypeList(_ =>
                {
                    firstCallbackCount++;
                    connection.GetTopicList(_ => callbackRegisteredDuringDeliveryCount++);
                });

                string json = JsonUtility.ToJson(new SysCommand_TopicsResponse
                {
                    topics = new[] { "/topic-list" },
                    types = new[] { TimeMsg.k_RosMessageName }
                });
                object attempt = AttemptTestAccess.Current(connection);
                typeof(ROSConnection).GetMethod("ReceiveSysCommand", BindingFlags.Instance | BindingFlags.NonPublic)
                    .Invoke(connection, new object[] { attempt, SysCommand.k_SysCommand_TopicList, json });

                Assert.AreEqual(1, firstCallbackCount);
                Assert.AreEqual(0, callbackRegisteredDuringDeliveryCount,
                    "A request registered during delivery must wait for a later response.");
                Assert.AreEqual(1, ((ICollection)AttemptTestAccess.Root(connection, "TopicCallbacks")).Count);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void OutboundQueueEntryCarriesAdmittingAttemptIdentity()
        {
            var gameObject = new GameObject("attempt-owned outbound queue");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                const long attemptId = 73;
                SetAttempt(connection, attemptId);

                connection.QueueSysCommand("__test", new SysCommand_TopicsRequest());

                object queue = AttemptTestAccess.Root(connection, "Outgoing");
                object[] arguments = { null };
                Assert.IsTrue((bool)queue.GetType().GetMethod("TryDequeue").Invoke(queue, arguments));
                FieldInfo entryAttempt = arguments[0].GetType().GetField("Attempt", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                Assert.NotNull(entryAttempt, "Outbound queue entries must carry attempt authority.");
                Assert.AreSame(AttemptTestAccess.Current(connection), entryAttempt.GetValue(arguments[0]));
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void PublishCrossingAttemptInvalidationLeavesNoQueuedPayload()
        {
            var gameObject = new GameObject("publish invalidation barrier");
            var blocked = new BlockingPublishMessage();
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                RosTopicState topic = connection.RegisterPublisher<TimeMsg>("/barrier");
                SetAttempt(connection, 91L);
                Exception failure = null;
                Task publish = Task.Run(() =>
                {
                    try { topic.Publish(blocked); }
                    catch (Exception e) { failure = e; }
                });
                Assert.IsTrue(blocked.Entered.Wait(TimeSpan.FromSeconds(2)),
                    "Publication must capture its epoch before the caller-side serializer barrier.");
                AttemptTestAccess.Retire(connection);
                blocked.Release.Set();
                Assert.IsTrue(publish.Wait(TimeSpan.FromSeconds(2)));
                Assert.IsInstanceOf<IOException>(failure);
                Assert.AreEqual(OutgoingMessageSender.SendToState.NoMessageToSendError,
                    topic.MessageSender.SendInternal(new MessageSerializer(), new MemoryStream()));
            }
            finally
            {
                blocked.Dispose();
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void QueueSizeOneServicePairsRejectWholeOverflowAndReuseCapacity()
        {
            var gameObject = new GameObject("atomic service pair");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                RosTopicState topic = connection.GetOrCreateTopic("/service", TimeMsg.k_RosMessageName, true);
                typeof(RosTopicState).GetMethod("CreateMessageSenderLocked", BindingFlags.Instance | BindingFlags.NonPublic)
                    .Invoke(topic, new object[] { 1 });
                SetLiveAttempt(connection, 55L);
                object attempt = AttemptTestAccess.Current(connection);
                TopicMessageSender sender = topic.MessageSender;

                MethodInfo send = typeof(ROSConnection).GetMethod("TryQueueServicePair", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.IsTrue((bool)send.Invoke(connection, new object[] { SysCommand.k_SysCommand_ServiceRequest,
                    new SysCommand_Service { srv_id = 101 }, sender, new TimeMsg(1, 11), attempt }));
                Assert.IsFalse((bool)send.Invoke(connection, new object[] { SysCommand.k_SysCommand_ServiceRequest,
                    new SysCommand_Service { srv_id = 102 }, sender, new TimeMsg(2, 22), attempt }));

                object queue = AttemptTestAccess.Root(connection, "Outgoing");
                MethodInfo dequeue = queue.GetType().GetMethod("TryDequeue");
                object[] first = { null };
                Assert.IsTrue((bool)dequeue.Invoke(queue, first));
                FieldInfo entryAttempt = first[0].GetType().GetField("Attempt");
                FieldInfo entrySender = first[0].GetType().GetField("Sender");
                Assert.AreSame(attempt, entryAttempt.GetValue(first[0]));
                Assert.AreNotSame(sender, entrySender.GetValue(first[0]));

                var wire = new MemoryStream();
                ((OutgoingMessageSender)entrySender.GetValue(first[0])).SendInternal(new MessageSerializer(), wire);
                Assert.IsTrue((bool)send.Invoke(connection, new object[] { SysCommand.k_SysCommand_ServiceRequest,
                    new SysCommand_Service { srv_id = 103 }, sender, new TimeMsg(3, 33), attempt }));
                object[] second = { null };
                Assert.IsTrue((bool)dequeue.Invoke(queue, second));
                ((OutgoingMessageSender)entrySender.GetValue(second[0])).SendInternal(new MessageSerializer(), wire);
                wire.Position = 0;
                Tuple<string, byte[]> control1 = ReadFrame(wire);
                Tuple<string, byte[]> payload1 = ReadFrame(wire);
                Tuple<string, byte[]> control2 = ReadFrame(wire);
                Tuple<string, byte[]> payload2 = ReadFrame(wire);
                Assert.AreEqual(SysCommand.k_SysCommand_ServiceRequest, control1.Item1);
                StringAssert.Contains("101", System.Text.Encoding.UTF8.GetString(control1.Item2));
                Assert.AreEqual("/service", payload1.Item1);
                Assert.AreEqual(1, BitConverter.ToUInt32(payload1.Item2, 0));
                Assert.AreEqual(SysCommand.k_SysCommand_ServiceRequest, control2.Item1);
                StringAssert.Contains("103", System.Text.Encoding.UTF8.GetString(control2.Item2));
                Assert.AreEqual("/service", payload2.Item1);
                Assert.AreEqual(3, BitConverter.ToUInt32(payload2.Item2, 0));
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void UnityServiceSubscriberInvalidatingAttemptPreventsImplementation()
        {
            var gameObject = new GameObject("service request invalidation");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                SetAttempt(connection, 31);
                int implementations = 0;
                connection.ImplementService<TimeMsg, TimeMsg>("/unity", request =>
                {
                    implementations++;
                    return request;
                });
                RosTopicState topic = connection.GetTopic("/unity");
                topic.AddSubscriber(_ => AttemptTestAccess.Retire(connection));

                typeof(RosTopicState).GetMethod("HandleUnityServiceRequest", BindingFlags.Instance | BindingFlags.NonPublic)
                    .Invoke(topic, new object[] { Serialize(new TimeMsg()), 101, AttemptTestAccess.Current(connection) });

                Assert.AreEqual(0, implementations);
            }
            finally { UnityEngine.Object.DestroyImmediate(gameObject); }
        }

        [Test]
        public void UnityServiceResponseCallbackInvalidatingAttemptPreventsQueueCommit()
        {
            var gameObject = new GameObject("service response invalidation");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                SetAttempt(connection, 32);
                connection.ImplementService<TimeMsg, TimeMsg>("/unity", request => new TimeMsg(9, 9));
                RosTopicState topic = connection.GetTopic("/unity");
                object queue = AttemptTestAccess.Root(connection, "Outgoing");
                ClearQueue(connection, queue);
                topic.ServiceResponseTopic.AddSubscriber(_ => AttemptTestAccess.Retire(connection));

                typeof(RosTopicState).GetMethod("HandleUnityServiceRequest", BindingFlags.Instance | BindingFlags.NonPublic)
                    .Invoke(topic, new object[] { Serialize(new TimeMsg()), 102, AttemptTestAccess.Current(connection) });

                Assert.AreEqual(0, (int)queue.GetType().GetProperty("Count").GetValue(queue));
            }
            finally { UnityEngine.Object.DestroyImmediate(gameObject); }
        }

        [Test]
        public void TopicRefreshStopsAfterFirstNotificationInvalidatesAttempt()
        {
            var gameObject = new GameObject("refresh invalidation");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                SetLiveAttempt(connection, 33);
                int notifications = 0;
                connection.ListenForTopics(_ =>
                {
                    notifications++;
                    AttemptTestAccess.Retire(connection);
                });
                connection.RefreshTopicsList();
                string json = JsonUtility.ToJson(new SysCommand_TopicsResponse
                {
                    topics = new[] { "/old-a", "/old-b" },
                    types = new[] { TimeMsg.k_RosMessageName, TimeMsg.k_RosMessageName }
                });
                typeof(ROSConnection).GetMethod("ReceiveSysCommand", BindingFlags.Instance | BindingFlags.NonPublic)
                    .Invoke(connection, new object[] { AttemptTestAccess.Current(connection), SysCommand.k_SysCommand_TopicList, json });

                Assert.AreEqual(1, notifications);
                Assert.IsNull(connection.GetTopic("/old-b"));
            }
            finally { UnityEngine.Object.DestroyImmediate(gameObject); }
        }

        [Test]
        public void DiscoveryListenerDeliveryStopsAtAttemptAuthorityBoundary()
        {
            var gameObject = new GameObject("discovery listener authority");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                SetLiveAttempt(connection, 98L);
                int firstNotifications = 0;
                int laterNotifications = 0;
                connection.ListenForTopics(_ =>
                {
                    firstNotifications++;
                    AttemptTestAccess.Retire(connection);
                });
                connection.ListenForTopics(_ => laterNotifications++);
                connection.RefreshTopicsList();

                string json = JsonUtility.ToJson(new SysCommand_TopicsResponse
                {
                    topics = new[] { "/discovered" },
                    types = new[] { TimeMsg.k_RosMessageName }
                });
                typeof(ROSConnection).GetMethod("ReceiveSysCommand", BindingFlags.Instance | BindingFlags.NonPublic)
                    .Invoke(connection, new object[] { AttemptTestAccess.Current(connection), SysCommand.k_SysCommand_TopicList, json });

                Assert.AreEqual(1, firstNotifications);
                Assert.AreEqual(0, laterNotifications,
                    "Discovery must not deliver a later listener after its attempt is revoked.");
                Assert.IsNotNull(connection.GetTopic("/discovered"),
                    "A committed discovery entry remains durable even when later delivery is revoked.");
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void DiscoveryPlaceholderMutationCannotCrossAttemptAuthorityBoundary()
        {
            var gameObject = new GameObject("discovery placeholder authority");
            var mutationEntered = new ManualResetEventSlim(false);
            var releaseMutation = new ManualResetEventSlim(false);
            FieldInfo mutationHook = null;
            Task<object> update = null;
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                SetLiveAttempt(connection, 99L);
                object attempt = AttemptTestAccess.Current(connection);
                MethodInfo discover = typeof(ROSConnection).GetMethod(
                    "GetOrCreateTopicForAttempt", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.NotNull(discover);
                RosTopicState initial = (RosTopicState)discover.Invoke(connection, new object[]
                {
                    "/discovery-placeholder", TimeMsg.k_RosMessageName, false, attempt
                });
                Assert.IsNotNull(initial);

                mutationHook = typeof(ROSConnection).GetField(
                    "m_BeforeDiscoveryPlaceholderMutationTestHook", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.NotNull(mutationHook,
                    "Discovery placeholder updates need an outside-the-lock authority barrier.");
                mutationHook.SetValue(connection, new Action(() =>
                {
                    mutationEntered.Set();
                    if (!releaseMutation.Wait(TimeSpan.FromSeconds(5)))
                        throw new TimeoutException("Discovery placeholder mutation did not reach the authority barrier.");
                }));

                update = Task.Run(() => discover.Invoke(connection, new object[]
                {
                    "/discovery-placeholder", "other_msgs/Other", false, attempt
                }));
                Assert.IsTrue(mutationEntered.Wait(TimeSpan.FromSeconds(5)),
                    "The stale discovery update must pause before it can mutate the placeholder.");
                AttemptTestAccess.Retire(connection);
                releaseMutation.Set();

                Assert.IsTrue(update.Wait(TimeSpan.FromSeconds(5)));
                if (update.IsFaulted)
                    Assert.Fail(update.Exception.ToString());
                Assert.IsNull(update.Result,
                    "A discovery operation from a retired attempt must not publish a stale placeholder update.");
                Assert.AreEqual(TimeMsg.k_RosMessageName, initial.RosMessageName,
                    "A retired discovery attempt must not mutate an existing placeholder.");
            }
            finally
            {
                releaseMutation.Set();
                if (update != null)
                    update.Wait(TimeSpan.FromSeconds(5));
                if (mutationHook != null && gameObject != null)
                    mutationHook.SetValue(gameObject.GetComponent<ROSConnection>(), null);
                UnityEngine.Object.DestroyImmediate(gameObject);
                releaseMutation.Dispose();
                mutationEntered.Dispose();
            }
        }

        [TestCase("subscriber")]
        [TestCase("publisher")]
        [TestCase("unity-service")]
        [TestCase("ros-service")]
        public void CompletedDiscoveryPlaceholderRejectsConflictingLaterDiscoveryForEveryLocalRole(string role)
        {
            var gameObject = new GameObject("completed discovery placeholder");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                SetLiveAttempt(connection, 100L);
                object attempt = AttemptTestAccess.Current(connection);
                const string topicName = "/completed-discovery-placeholder";
                MethodInfo discover = typeof(ROSConnection).GetMethod(
                    "GetOrCreateTopicForAttempt", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.NotNull(discover);
                RosTopicState initial = (RosTopicState)discover.Invoke(connection, new object[]
                {
                    topicName, TimeMsg.k_RosMessageName, false, attempt
                });
                Assert.IsNotNull(initial);

                switch (role)
                {
                    case "subscriber":
                        connection.Subscribe<TimeMsg>(topicName, _ => { });
                        break;
                    case "publisher":
                        connection.RegisterPublisher(topicName, TimeMsg.k_RosMessageName, 1, false);
                        break;
                    case "unity-service":
                        connection.ImplementService<TimeMsg, TimeMsg>(topicName, _ => new TimeMsg(), 1);
                        break;
                    default:
                        connection.RegisterRosService(topicName, TimeMsg.k_RosMessageName,
                            TimeMsg.k_RosMessageName, 1);
                        break;
                }

                Assert.AreSame(initial, connection.GetTopic(topicName));
                Assert.IsTrue(initial.IsPublisher || initial.IsRosService || initial.IsUnityService
                    || initial.HasSubscriberCallback,
                    "The discovery placeholder must be completed by the local role configuration.");

                FieldInfo desiredField = typeof(ROSConnection).GetField(
                    "m_DesiredRegistrations", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.NotNull(desiredField);
                object desiredDefinition = null;
                foreach (object definition in (IEnumerable)desiredField.GetValue(connection))
                {
                    if (ReferenceEquals(definition.GetType().GetField(
                        "Topic", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                        .GetValue(definition), initial))
                    {
                        desiredDefinition = definition;
                        break;
                    }
                }
                Assert.IsNotNull(desiredDefinition,
                    "Local configuration must leave one durable desired registration.");
                long desiredRevision = (long)desiredDefinition.GetType().GetField(
                    "Revision", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                    .GetValue(desiredDefinition);

                RosTopicState later = (RosTopicState)discover.Invoke(connection, new object[]
                {
                    topicName, "other_msgs/ConflictingType", false, attempt
                });

                Assert.AreSame(initial, later,
                    "A later discovery entry must be ignored after local configuration completes.");
                Assert.AreEqual(TimeMsg.k_RosMessageName, initial.RosMessageName,
                    "Later discovery must not rewrite the locally configured schema.");
                Assert.AreSame(desiredDefinition, FindDesiredDefinition(desiredField, connection, initial),
                    "Later discovery must not replace the local desired registration.");
                Assert.AreEqual(desiredRevision, (long)desiredDefinition.GetType().GetField(
                    "Revision", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                    .GetValue(desiredDefinition));
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        static object FindDesiredDefinition(FieldInfo desiredField, ROSConnection connection,
            RosTopicState topic)
        {
            foreach (object definition in (IEnumerable)desiredField.GetValue(connection))
            {
                if (ReferenceEquals(definition.GetType().GetField(
                    "Topic", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                    .GetValue(definition), topic))
                    return definition;
            }
            return null;
        }

        [Test]
        public void PendingServiceResponseUsesAdmissionSchemaAfterResponseReconfiguration()
        {
            const string observerName = "slice7/AdmissionObserver";
            const string requestedName = "slice7/AdmissionRequested";
            MessageRegistry.Register<Slice1ObserverResponse>(observerName, decoder => new Slice1ObserverResponse(),
                MessageSubtopic.Response);
            MessageRegistry.Register<Slice1Response>(requestedName, decoder => new Slice1Response { Value = 8 });
            using (var fixture = new ServicePublicationFixture(observerName))
            {
                var context = new ServiceDispatchContext();
                object observed = null;
                RosTopicState topic = fixture.Connection.GetTopic("/ros");
                topic.ServiceResponseTopic.AddSubscriber(message =>
                {
                    observed = message;
                    Assert.AreSame(context, SynchronizationContext.Current);
                });
                Task<Slice1Response> request = context.Run(() =>
                    fixture.Connection.SendServiceMessage<Slice1Response>("/ros", new TimeMsg()));
                int id = fixture.ReadRequestId();

                fixture.Connection.RegisterRosService<TimeMsg, UInt8MultiArrayMsg>("/ros");
                fixture.Respond(id, response: new TimeMsg(8, 8));
                context.RunOne();

                Assert.IsInstanceOf<Slice1ObserverResponse>(observed,
                    "A pending response must notify observers with the schema captured at admission.");
                Assert.AreEqual(8, request.GetAwaiter().GetResult().Value,
                    "A pending response must use its admitted response decoder after reconfiguration.");
            }
        }

        [Test]
        public void RosServiceCompletionInvalidatedBeforeContinuationSkipsCallbacks()
        {
            var gameObject = new GameObject("service continuation race");
            var continuationEntered = new ManualResetEventSlim(false);
            var releaseContinuation = new ManualResetEventSlim(false);
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                SetLiveAttempt(connection, 34);
                connection.RegisterRosService<TimeMsg, TimeMsg>("/ros");
                bool callbackInvoked = false;
                connection.GetTopic("/ros").ServiceResponseTopic.AddSubscriber(_ => callbackInvoked = true);
                typeof(ROSConnection).GetField("m_ServiceResponseContinuationTestHook", BindingFlags.Instance | BindingFlags.NonPublic)
                    .SetValue(connection, new Action(() =>
                    {
                        continuationEntered.Set();
                        releaseContinuation.Wait();
                    }));
                Task<Task<TimeMsg>> preparation = Task.Factory.StartNew(() =>
                    connection.SendServiceMessage<TimeMsg>("/ros", new TimeMsg()),
                    CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
                Assert.IsTrue(preparation.Wait(TimeSpan.FromSeconds(2)));
                Task<TimeMsg> request = preparation.Result;
                int id = (int)typeof(ROSConnection).GetField("m_NextSrvID", BindingFlags.Instance | BindingFlags.NonPublic)
                    .GetValue(connection) - 1;
                DispatchFrame(connection, AttemptTestAccess.Current(connection), SysCommand.k_SysCommand_ServiceResponse,
                    System.Text.Encoding.UTF8.GetBytes(JsonUtility.ToJson(new SysCommand_Service { srv_id = id })));
                DispatchFrame(connection, AttemptTestAccess.Current(connection), "/ros", Serialize(new TimeMsg(8, 8)));
                Assert.IsTrue(continuationEntered.Wait(TimeSpan.FromSeconds(2)));
                AttemptTestAccess.Retire(connection);
                releaseContinuation.Set();
                Assert.IsTrue(SpinWait.SpinUntil(() => request.IsCompleted, TimeSpan.FromSeconds(2)));
                Assert.IsTrue(request.IsFaulted);
                Assert.IsInstanceOf<IOException>(request.Exception.InnerException);
                Assert.IsFalse(callbackInvoked);
            }
            finally
            {
                releaseContinuation.Set();
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        static Tuple<string, byte[]> ReadFrame(Stream stream)
        {
            var reader = new BinaryReader(stream, System.Text.Encoding.UTF8, true);
            string topic = System.Text.Encoding.ASCII.GetString(reader.ReadBytes(reader.ReadInt32()));
            return Tuple.Create(topic, reader.ReadBytes(reader.ReadInt32()));
        }

        [Test]
        public void TerminalCleanupAllowsCleanSecondConnection()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            var gameObject = new GameObject("reconnecting connection");
            ROSConnectionConfig config = null;
            TcpClient first = null;
            TcpClient second = null;
            try
            {
                listener.Start();
                config = Config("127.0.0.1", ((IPEndPoint)listener.LocalEndpoint).Port);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectionConfig = config;

                Task<TcpClient> firstAccept = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(firstAccept.Wait(TimeSpan.FromSeconds(5)));
                first = firstAccept.Result;
                connection.Disconnect();
                Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));

                Task<TcpClient> secondAccept = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(secondAccept.Wait(TimeSpan.FromSeconds(5)));
                second = secondAccept.Result;
                connection.Disconnect();
                Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
            }
            finally
            {
                first?.Close();
                second?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
            }
        }

        [Test]
        public void FailedAttemptClosesAdmissionBeforeReconnectBackoff()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            var gameObject = new GameObject("reconnect backoff admission");
            ROSConnectionConfig config = null;
            TcpClient accepted = null;
            try
            {
                listener.Start();
                config = Config("127.0.0.1", ((IPEndPoint)listener.LocalEndpoint).Port);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectionConfig = config;
                Task<TcpClient> accept = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(accept.Wait(TimeSpan.FromSeconds(5)));
                accepted = accept.Result;
                var serializer = new MessageSerializer();
                serializer.Write(SysCommand.k_SysCommand_Handshake);
                serializer.WriteUnaligned(JsonUtility.ToJson(new SysCommand_Handshake
                {
                    version = ROSConnection.k_Version,
                    metadata = JsonUtility.ToJson(new SysCommand_Handshake_Metadata { protocol = "ROS1" })
                }));
                serializer.SendTo(accepted.GetStream());
                Assert.IsTrue(SpinWait.SpinUntil(() => AttemptTestAccess.Id(connection) != 0, TimeSpan.FromSeconds(5)));

                LogAssert.Expect(LogType.Error, new Regex("ROS Connection to .* failed"));
                accepted.Close();
                accepted = null;
                Assert.IsTrue(SpinWait.SpinUntil(() => AttemptTestAccess.Id(connection) == 0, TimeSpan.FromSeconds(5)));
                Assert.Throws<IOException>(() => connection.GetTopicList(_ => { }));
                AssertThrowsAsync<IOException>(() =>
                    connection.SendServiceMessage<TimeMsg>("/during-backoff", new TimeMsg()));
                Assert.IsTrue(SpinWait.SpinUntil(() => (bool)typeof(ROSConnection).GetField("m_HasOutputConnectionError",
                    BindingFlags.Instance | BindingFlags.NonPublic).GetValue(connection), TimeSpan.FromSeconds(5)),
                    "Admission now closes before failure reporting; wait for the separate diagnostic phase before fixture stop.");
            }
            finally
            {
                var connection = gameObject.GetComponent<ROSConnection>();
                if (connection != null)
                {
                    connection.Disconnect();
                    SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5));
                }
                accepted?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
            }
        }

        [Test]
        public void SubscriberAddedBetweenResetAndQueueClearRegistersOnceOnNextAttempt()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            var gameObject = new GameObject("subscriber teardown race");
            ROSConnectionConfig config = null;
            TcpClient firstPeer = null;
            TcpClient secondPeer = null;
            try
            {
                listener.Start();
                config = Config("127.0.0.1", ((IPEndPoint)listener.LocalEndpoint).Port);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                connection.listenForTFMessages = false;
                connection.ConnectionConfig = config;
                connection.Subscribe<TimeMsg>("/teardown", _ => { });
                RosTopicState topic = connection.GetTopic("/teardown");
                connection.QueueSysCommand("__first-teardown-fence", new SysCommand_TopicsRequest());

                Task<TcpClient> firstAccepted = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(firstAccepted.Wait(TimeSpan.FromSeconds(5)));
                firstPeer = firstAccepted.Result;
                firstPeer.GetStream().ReadTimeout = 5000;
                var firstFrames = new List<Tuple<string, byte[]>>();
                for (int index = 0; index < 32; index++)
                {
                    Tuple<string, byte[]> frame = ReadFrame(firstPeer.GetStream());
                    firstFrames.Add(frame);
                    if (frame.Item1 == "__first-teardown-fence") break;
                }
                Assert.AreEqual(1, firstFrames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_Subscribe).Count,
                    "The first attempt must claim the durable subscriber registration once.");
                Assert.Greater(firstFrames.FindIndex(frame => frame.Item1 == "__first-teardown-fence"),
                    firstFrames.FindIndex(frame => frame.Item1 == SysCommand.k_SysCommand_Subscribe));
                Assert.IsTrue(topic.SentSubscriberRegistration);

                connection.Disconnect();
                Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                firstPeer.Close();
                firstPeer = null;

                // Recreate the intent after the old attempt's reset/queue-clear window.
                // The next attempt must reconcile the latest durable state, not replay an
                // obsolete transition or rely on the former immediate-send callback.
                topic.UnsubscribeAll();
                topic.AddSubscriber(_ => { });
                connection.QueueSysCommand("__second-teardown-fence", new SysCommand_TopicsRequest());

                Task<TcpClient> secondAccepted = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(secondAccepted.Wait(TimeSpan.FromSeconds(5)));
                secondPeer = secondAccepted.Result;
                secondPeer.GetStream().ReadTimeout = 5000;
                var secondFrames = new List<Tuple<string, byte[]>>();
                for (int index = 0; index < 32; index++)
                {
                    Tuple<string, byte[]> frame = ReadFrame(secondPeer.GetStream());
                    secondFrames.Add(frame);
                    if (frame.Item1 == "__second-teardown-fence") break;
                }
                Assert.AreEqual(1, secondFrames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_Subscribe).Count,
                    "The next attempt must reconcile the resubscribed topic once from parsed wire frames.");
                int secondRegistration = secondFrames.FindIndex(frame => frame.Item1 == SysCommand.k_SysCommand_Subscribe);
                int secondFence = secondFrames.FindIndex(frame => frame.Item1 == "__second-teardown-fence");
                Assert.GreaterOrEqual(secondRegistration, 0);
                Assert.Greater(secondFence, secondRegistration);
                Assert.IsTrue(topic.SentSubscriberRegistration,
                    "The sent flag must describe the current attempt's completed wire write.");
            }
            finally
            {
                try { if (gameObject != null) gameObject.GetComponent<ROSConnection>()?.Disconnect(); }
                catch { }
                firstPeer?.Close();
                secondPeer?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
            }
        }

        [Test]
        public void CancellationImmediatelyAfterSchedulingReachesTerminalCleanup()
        {
            var gameObject = new GameObject("immediately cancelled connection");
            ROSConnectionConfig config = null;
            try
            {
                config = Config("192.0.2.1", 65000);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectionConfig = config;

                connection.Connect();
                connection.Disconnect();

                Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
            }
        }

        [Test]
        public void DisconnectClosesLifecycleOwnedActiveSocketAndCleansUp()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            var gameObject = new GameObject("active socket connection");
            ROSConnectionConfig config = null;
            TcpClient accepted = null;
            try
            {
                listener.Start();
                config = Config("127.0.0.1", ((IPEndPoint)listener.LocalEndpoint).Port);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectionConfig = config;
                Task<TcpClient> accept = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(accept.Wait(TimeSpan.FromSeconds(5)));
                accepted = accept.Result;

                Assert.IsNotNull(AttemptTestAccess.Root(connection, "Client"), "The active socket must be attempt-owned.");

                connection.Disconnect();

                Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                Assert.IsNull(AttemptTestAccess.Root(connection, "Client"));
                accepted.ReceiveTimeout = 1000;
                var buffer = new byte[32];
                int bytesRead;
                do
                {
                    bytesRead = accepted.GetStream().Read(buffer, 0, buffer.Length);
                }
                while (bytesRead > 0);
                Assert.AreEqual(0, bytesRead, "Disconnect must close the exact active peer socket after buffered handshake bytes.");
            }
            finally
            {
                accepted?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
            }
        }

        [Test]
        public void ThrowingQueueCleanupCannotSkipAttemptInvalidationOrTerminalCleanup()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            var gameObject = new GameObject("throwing cleanup connection");
            ROSConnectionConfig config = null;
            TcpClient accepted = null;
            try
            {
                listener.Start();
                config = Config("127.0.0.1", ((IPEndPoint)listener.LocalEndpoint).Port);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectionConfig = config;
                Task<TcpClient> accept = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(accept.Wait(TimeSpan.FromSeconds(5)));
                accepted = accept.Result;
                Assert.IsTrue(SpinWait.SpinUntil(() => AttemptTestAccess.Id(connection) != 0, TimeSpan.FromSeconds(5)));

                object outgoing = AttemptTestAccess.Root(connection, "Outgoing");
                object attempt = AttemptTestAccess.Current(connection);
                LogAssert.Expect(LogType.Exception, new Regex("cleanup failure"));
                object lifecycleLock = typeof(ROSConnection).GetField("m_ConnectionLifecycleLock", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(connection);
                Monitor.Enter(lifecycleLock);
                try
                {
                    connection.Disconnect();
                    outgoing.GetType().GetMethod("Enqueue").Invoke(outgoing, new object[] { attempt, new ThrowingCleanupSender() });
                }
                finally
                {
                    Monitor.Exit(lifecycleLock);
                }

                Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                Assert.AreEqual(0L, AttemptTestAccess.Id(connection));
            }
            finally
            {
                accepted?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
            }
        }

        [Test]
        public void PublicAndSerializedCompatibilityMembersRemainAvailable()
        {
            Assert.NotNull(typeof(RosTopicState).GetProperty("SentPublisherRegistration", BindingFlags.Instance | BindingFlags.Public));
            FieldInfo showHud = typeof(ROSConnection).GetField("m_ShowHUD", BindingFlags.Instance | BindingFlags.NonPublic);
            var aliases = (FormerlySerializedAsAttribute[])showHud.GetCustomAttributes(typeof(FormerlySerializedAsAttribute), false);
            Assert.IsTrue(Array.Exists(aliases, alias => alias.oldName == "showHUD"));
        }

        [Test]
        public void ListenForTopicsInitialSnapshotIsLinearizedWithTopicCreation()
        {
            var gameObject = new GameObject("topic listener race");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                object lifecycleLock = typeof(ROSConnection).GetField("m_ConnectionLifecycleLock", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(connection);
                int notifications = 0;
                RosTopicState observed = null;
                var creatorStarted = new ManualResetEventSlim(false);
                var listenerStarted = new ManualResetEventSlim(false);

                Monitor.Enter(lifecycleLock);
                Task creator;
                Task listener;
                try
                {
                    creator = Task.Run(() =>
                    {
                        creatorStarted.Set();
                        connection.RegisterPublisher<TimeMsg>("/linearized", 3, true);
                    });
                    Assert.IsTrue(creatorStarted.Wait(TimeSpan.FromSeconds(1)));
                    listener = Task.Run(() =>
                    {
                        listenerStarted.Set();
                        connection.ListenForTopics(state =>
                        {
                            if (state.Topic == "/linearized")
                            {
                                observed = state;
                                Interlocked.Increment(ref notifications);
                                Assert.IsTrue(state.IsPublisher);
                                Assert.IsTrue(state.IsPublisherLatched);
                                Assert.AreEqual(3, state.MessageSender.QueueSize);
                            }
                        }, true);
                    });
                    Assert.IsTrue(listenerStarted.Wait(TimeSpan.FromSeconds(1)));
                    Thread.Sleep(20);
                }
                finally
                {
                    Monitor.Exit(lifecycleLock);
                }

                Assert.IsTrue(Task.WaitAll(new[] { creator, listener }, TimeSpan.FromSeconds(2)));
                Assert.AreEqual(1, notifications,
                    "A listener registration and topic creation must publish one delivery for the creation ID.");
                Assert.IsNotNull(observed);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [TestCase("OnDestroy")]
        [TestCase("OnApplicationQuit")]
        public void UnityTeardownInitiatesCancellationAndEventuallyCleansWorker(string callbackName)
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            var gameObject = new GameObject("teardown connection");
            ROSConnectionConfig config = null;
            TcpClient accepted = null;
            try
            {
                listener.Start();
                config = Config("127.0.0.1", ((IPEndPoint)listener.LocalEndpoint).Port);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectionConfig = config;
                Task<TcpClient> accept = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(accept.Wait(TimeSpan.FromSeconds(5)));
                accepted = accept.Result;

                typeof(ROSConnection).GetMethod(callbackName, BindingFlags.Instance | BindingFlags.NonPublic)
                    .Invoke(connection, null);

                Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
            }
            finally
            {
                accepted?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
            }
        }

        [Test]
        public void ConnectionErrorsAreIndependentPerInstance()
        {
            var firstObject = new GameObject("first connection");
            var secondObject = new GameObject("second connection");
            try
            {
                var first = firstObject.AddComponent<ROSConnection>();
                var second = secondObject.AddComponent<ROSConnection>();
                typeof(ROSConnection).GetField("m_HasConnectionError", BindingFlags.Instance | BindingFlags.NonPublic)
                    .SetValue(first, true);

                Assert.IsTrue(first.HasConnectionError);
                Assert.IsFalse(second.HasConnectionError);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(firstObject);
                UnityEngine.Object.DestroyImmediate(secondObject);
            }
        }

        [Test]
        public void SubscriberCallbackCanDisconnectWithoutDeadlock()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            var gameObject = new GameObject("callback disconnect connection");
            ROSConnectionConfig config = null;
            TcpClient accepted = null;
            try
            {
                listener.Start();
                config = Config("127.0.0.1", ((IPEndPoint)listener.LocalEndpoint).Port);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectionConfig = config;
                Task<TcpClient> accept = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(accept.Wait(TimeSpan.FromSeconds(5)));
                accepted = accept.Result;

                connection.Subscribe<TimeMsg>("/callback", _ => connection.Disconnect());
                var serializer = new MessageSerializer();
                serializer.SerializeMessage(new TimeMsg());
                typeof(RosTopicState).GetMethod("OnMessageReceived", BindingFlags.Instance | BindingFlags.NonPublic)
                    .Invoke(connection.GetTopic("/callback"), new object[] { serializer.GetBytes() });

                Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
            }
            finally
            {
                accepted?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
            }
        }

        [Test]
        public void SubscriberCallbacksStopWhenFirstCallbackInvalidatesAttempt()
        {
            var gameObject = new GameObject("subscriber callback invalidation");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                const long attemptId = 62;
                SetAttempt(connection, attemptId);
                int secondCallbackCount = 0;
                connection.Subscribe<TimeMsg>("/callback", _ => AttemptTestAccess.Retire(connection));
                connection.Subscribe<TimeMsg>("/callback", _ => secondCallbackCount++);
                var serializer = new MessageSerializer();
                serializer.SerializeMessage(new TimeMsg());
                Type incomingType = typeof(ROSConnection).GetNestedType("IncomingMessage", BindingFlags.NonPublic);
                object incoming = Activator.CreateInstance(incomingType, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                    null, new object[] { AttemptTestAccess.Current(connection), "/callback", serializer.GetBytes() }, null);

                typeof(ROSConnection).GetMethod("DispatchIncomingMessage", BindingFlags.Instance | BindingFlags.NonPublic)
                    .Invoke(connection, new[] { incoming });

                Assert.AreEqual(0, secondCallbackCount);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void UnityServiceRequestAuthorityIsRecheckedAfterRequestDeserialization()
        {
            var gameObject = new GameObject("c5 request deserialization authority");
            using (var entered = new ManualResetEventSlim(false))
            {
                FieldInfo hook = null;
                RosTopicState topic = null;
                try
                {
                    var connection = gameObject.AddComponent<ROSConnection>();
                    SetLiveAttempt(connection, 301L);
                    int implementations = 0;
                    connection.ImplementService<TimeMsg, TimeMsg>("/c5-deserialize", request =>
                    {
                        Interlocked.Increment(ref implementations);
                        return new TimeMsg(1, 1);
                    });
                    topic = connection.GetTopic("/c5-deserialize");
                    object attempt = AttemptTestAccess.Current(connection);
                    hook = typeof(RosTopicState).GetField(
                        "m_AfterUnityServiceRequestDeserializationTestHook",
                        BindingFlags.Instance | BindingFlags.NonPublic);
                    Assert.NotNull(hook,
                        "Unity-service request handling needs a post-deserialization authority barrier.");
                    hook.SetValue(topic, new Action(() =>
                    {
                        entered.Set();
                        Task stop = Task.Run(() => connection.Disconnect());
                        Assert.IsTrue(stop.Wait(TimeSpan.FromSeconds(5)),
                            "Disconnect must revoke the request attempt without waiting for service code.");
                    }));

                    typeof(RosTopicState).GetMethod("HandleUnityServiceRequest",
                        BindingFlags.Instance | BindingFlags.NonPublic).Invoke(topic,
                        new object[] { Serialize(new TimeMsg()), 301, attempt });

                    Assert.IsTrue(entered.IsSet,
                        "The request must reach the post-deserialization authority barrier.");
                    Assert.AreEqual(0, Volatile.Read(ref implementations),
                        "A request whose attempt was revoked after deserialization must not invoke the implementation.");
                }
                finally
                {
                    if (hook != null && topic != null)
                        hook.SetValue(topic, null);
                    UnityEngine.Object.DestroyImmediate(gameObject);
                }
            }
        }

        [Test]
        public void UnityServiceRequestAuthorityIsRecheckedAfterAsyncImplementation()
        {
            var gameObject = new GameObject("c5 async implementation authority");
            using (var implementationEntered = new ManualResetEventSlim(false))
            using (var barrierEntered = new ManualResetEventSlim(false))
            {
                FieldInfo hook = null;
                RosTopicState topic = null;
                ROSConnection connection = null;
                var releaseImplementation = new TaskCompletionSource<TimeMsg>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                int implementations = 0;
                try
                {
                    connection = gameObject.AddComponent<ROSConnection>();
                    SetLiveAttempt(connection, 302L);
                    connection.ImplementService<TimeMsg, TimeMsg>("/c5-async", request =>
                    {
                        Interlocked.Increment(ref implementations);
                        implementationEntered.Set();
                        return releaseImplementation.Task;
                    });
                    topic = connection.GetTopic("/c5-async");
                    object attempt = AttemptTestAccess.Current(connection);
                    hook = typeof(RosTopicState).GetField(
                        "m_AfterUnityServiceRequestImplementationTestHook",
                        BindingFlags.Instance | BindingFlags.NonPublic);
                    Assert.NotNull(hook,
                        "Unity-service request handling needs a post-implementation authority barrier.");
                    hook.SetValue(topic, new Action(() =>
                    {
                        barrierEntered.Set();
                        Task stop = Task.Run(() => connection.Disconnect());
                        Assert.IsTrue(stop.Wait(TimeSpan.FromSeconds(5)),
                            "Disconnect must revoke the request attempt before response commit.");
                    }));

                    SynchronizationContext previous = SynchronizationContext.Current;
                    SynchronizationContext.SetSynchronizationContext(null);
                    try
                    {
                        typeof(RosTopicState).GetMethod("HandleUnityServiceRequest",
                            BindingFlags.Instance | BindingFlags.NonPublic).Invoke(topic,
                            new object[] { Serialize(new TimeMsg()), 302, attempt });
                        Assert.IsTrue(implementationEntered.Wait(TimeSpan.FromSeconds(2)),
                            "The async service implementation must receive the request.");
                        releaseImplementation.SetResult(new TimeMsg(2, 2));
                        Assert.IsTrue(barrierEntered.Wait(TimeSpan.FromSeconds(5)),
                            "The request must reach the post-implementation authority barrier.");
                    }
                    finally
                    {
                        SynchronizationContext.SetSynchronizationContext(previous);
                    }

                    Assert.AreEqual(1, Volatile.Read(ref implementations));
                }
                finally
                {
                    releaseImplementation.TrySetResult(new TimeMsg());
                    if (hook != null && topic != null)
                        hook.SetValue(topic, null);
                    if (connection != null)
                        connection.Disconnect();
                    UnityEngine.Object.DestroyImmediate(gameObject);
                }
            }
        }

        [Test]
        public void RawPairParserCannotReinstallStateAfterAttemptInvalidation()
        {
            var gameObject = new GameObject("raw pair parser authority");
            using (var installEntered = new ManualResetEventSlim(false))
            using (var installRelease = new ManualResetEventSlim(false))
            using (var routeEntered = new ManualResetEventSlim(false))
            using (var routeRelease = new ManualResetEventSlim(false))
            {
                FieldInfo installHook = null;
                FieldInfo routeHook = null;
                Task install = null;
                Task route = null;
                try
                {
                    var connection = gameObject.AddComponent<ROSConnection>();
                    SetLiveAttempt(connection, 201L);
                    connection.RegisterRosService<TimeMsg, TimeMsg>("/c4");

                    installHook = typeof(ROSConnection).GetField(
                        "m_BeforePendingPairInstallationTestHook", BindingFlags.Instance | BindingFlags.NonPublic);
                    routeHook = typeof(ROSConnection).GetField(
                        "m_BeforePendingPairRoutingTestHook", BindingFlags.Instance | BindingFlags.NonPublic);
                    Assert.NotNull(installHook, "Pair installation needs an outside-the-gate authority barrier.");
                    Assert.NotNull(routeHook, "Pair routing needs an outside-the-gate authority barrier.");
                    installHook.SetValue(connection, (Action)(() =>
                    {
                        installEntered.Set();
                        install = Task.Run(() =>
                        {
                            AttemptTestAccess.Retire(connection);
                            SetLiveAttempt(connection, 202L);
                            installRelease.Set();
                        });
                        Assert.IsTrue(installRelease.Wait(TimeSpan.FromSeconds(5)));
                    }));

                    object attemptA = AttemptTestAccess.Current(connection);
                    Task<TimeMsg> first = connection.SendServiceMessage<TimeMsg>("/c4", new TimeMsg());
                    int firstId = (int)typeof(ROSConnection).GetField("m_NextSrvID",
                        BindingFlags.Instance | BindingFlags.NonPublic).GetValue(connection) - 1;
                    typeof(ROSConnection).GetMethod("ReceiveSysCommand", BindingFlags.Instance | BindingFlags.NonPublic)
                        .Invoke(connection, new object[]
                        {
                            attemptA, SysCommand.k_SysCommand_ServiceResponse,
                            JsonUtility.ToJson(new SysCommand_Service { srv_id = firstId })
                        });
                    Assert.IsTrue(installEntered.IsSet,
                        "The response parser must pause before installing A's pair state.");
                    Assert.IsTrue(install != null && install.Wait(TimeSpan.FromSeconds(3)));
                    Assert.IsNull(AttemptTestAccess.Root(connection, "PendingPair"),
                        "A's parsed pair must not be installed into replacement attempt B.");
                    Assert.IsTrue(first.IsFaulted);

                    installHook.SetValue(connection, null);
                    object attemptB = AttemptTestAccess.Current(connection);
                    Task<TimeMsg> second = connection.SendServiceMessage<TimeMsg>("/c4", new TimeMsg());
                    int secondId = (int)typeof(ROSConnection).GetField("m_NextSrvID",
                        BindingFlags.Instance | BindingFlags.NonPublic).GetValue(connection) - 1;
                    typeof(ROSConnection).GetMethod("ReceiveSysCommand", BindingFlags.Instance | BindingFlags.NonPublic)
                        .Invoke(connection, new object[]
                        {
                            attemptB, SysCommand.k_SysCommand_ServiceResponse,
                            JsonUtility.ToJson(new SysCommand_Service { srv_id = secondId })
                        });
                    Assert.NotNull(AttemptTestAccess.Root(connection, "PendingPair"));

                    routeHook.SetValue(connection, (Action)(() =>
                    {
                        routeEntered.Set();
                        route = Task.Run(() =>
                        {
                            AttemptTestAccess.Retire(connection);
                            SetLiveAttempt(connection, 203L);
                            routeRelease.Set();
                        });
                        Assert.IsTrue(routeRelease.Wait(TimeSpan.FromSeconds(5)));
                    }));
                    var payload = new MessageSerializer();
                    payload.SerializeMessage(new TimeMsg(8, 9));
                    Type frameType = typeof(ROSConnection).GetNestedType("IncomingMessage", BindingFlags.NonPublic);
                    object frame = Activator.CreateInstance(frameType,
                        BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null,
                        new object[] { attemptB, "/c4", payload.GetBytes() }, null);
                    typeof(ROSConnection).GetMethod("DispatchIncomingMessage",
                        BindingFlags.Instance | BindingFlags.NonPublic).Invoke(connection, new[] { frame });
                    Assert.IsTrue(routeEntered.IsSet,
                        "Pair routing must pause after consuming B's state and before user-visible routing.");
                    Assert.IsTrue(route != null && route.Wait(TimeSpan.FromSeconds(3)));
                    Assert.IsNull(AttemptTestAccess.Root(connection, "PendingPair"),
                        "A consumed pair must not route its payload through replacement attempt C.");
                    Assert.IsTrue(second.IsFaulted);
                }
                finally
                {
                    installRelease.Set();
                    routeRelease.Set();
                    if (install != null) install.Wait(TimeSpan.FromSeconds(5));
                    if (route != null) route.Wait(TimeSpan.FromSeconds(5));
                    if (installHook != null && gameObject != null)
                        installHook.SetValue(gameObject.GetComponent<ROSConnection>(), null);
                    if (routeHook != null && gameObject != null)
                        routeHook.SetValue(gameObject.GetComponent<ROSConnection>(), null);
                    UnityEngine.Object.DestroyImmediate(gameObject);
                }
            }
        }
    }
}
