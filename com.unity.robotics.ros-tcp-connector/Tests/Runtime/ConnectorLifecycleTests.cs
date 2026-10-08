using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Unity.Robotics.ROSTCPConnector;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using RosMessageTypes.BuiltinInterfaces;
using UnityEngine;
using UnityEngine.TestTools;
using System.Text.RegularExpressions;

namespace UnitTests
{
    // Isolated metadata fixtures for inherited unit tests, not proof of transport lifecycle.
    // All lifecycle acceptance tests below use real Connect/Disconnect sessions.
    internal static class AttemptTestAccess
    {
        const BindingFlags Private = BindingFlags.Instance | BindingFlags.NonPublic;
        internal static object Field(object owner, string name)
        {
            FieldInfo field = owner.GetType().GetField(name, Private | BindingFlags.Public);
            return field != null ? field.GetValue(owner) : owner.GetType().GetProperty(name, Private | BindingFlags.Public).GetValue(owner);
        }
        internal static object Current(ROSConnection connection) => Field(connection, "m_CurrentAttempt");
        internal static object Root(ROSConnection connection, string name) => Field(Current(connection), name);
        internal static long Id(ROSConnection connection) => (long)typeof(ROSConnection).GetProperty("CurrentAttemptId", Private).GetValue(connection);
        internal static object New(long id)
        {
            return Activator.CreateInstance(typeof(ROSConnection).GetNestedType("AttemptContext", BindingFlags.NonPublic),
                Private | BindingFlags.Public, null, new object[] { id, null }, null);
        }
        internal static void Replace(ROSConnection connection, long id)
        {
            object gate = Field(connection, "m_ConnectionLifecycleLock");
            lock (gate)
            {
                object attempt = id == 0 ? null : New(id);
                typeof(ROSConnection).GetField("m_CurrentAttempt", Private).SetValue(connection, attempt);
            }
        }
        internal static void Retire(ROSConnection connection)
        {
            typeof(ROSConnection).GetMethod("RetireAttempt", Private).Invoke(connection, new[] { Current(connection) });
        }
    }

    public class ConnectorLifecycleTests
    {
        const BindingFlags Private = BindingFlags.Instance | BindingFlags.NonPublic;

        static void Hook(ROSConnection connection, string name, Action callback)
        {
            typeof(ROSConnection).GetField(name, Private).SetValue(connection, callback);
        }

        static ROSConnectionConfig Config(int port)
        {
            var config = ScriptableObject.CreateInstance<ROSConnectionConfig>();
            typeof(ROSConnectionConfig).GetField("m_RosIPAddress", Private).SetValue(config, "127.0.0.1");
            typeof(ROSConnectionConfig).GetField("m_RosPort", Private).SetValue(config, port);
            return config;
        }

        static object Field(object owner, string name)
        {
            return AttemptTestAccess.Field(owner, name);
        }

        static void SendFrame(TcpClient peer, string topic, byte[] contents)
        {
            NetworkStream stream = peer.GetStream();
            byte[] topicBytes = Encoding.ASCII.GetBytes(topic);
            byte[] topicLength = BitConverter.GetBytes(topicBytes.Length);
            byte[] contentsLength = BitConverter.GetBytes(contents.Length);
            stream.Write(topicLength, 0, topicLength.Length);
            stream.Write(topicBytes, 0, topicBytes.Length);
            stream.Write(contentsLength, 0, contentsLength.Length);
            stream.Write(contents, 0, contents.Length);
            stream.Flush();
        }

        static string ConnectorSource()
        {
            string manifestPath = Path.GetFullPath(Path.Combine(Application.dataPath, "../Packages/manifest.json"));
            string manifest = File.ReadAllText(manifestPath);
            const string marker = "\"com.unity.robotics.ros-tcp-connector\": \"file:";
            int start = manifest.IndexOf(marker, StringComparison.Ordinal);
            Assert.GreaterOrEqual(start, 0, "The test project must resolve the connector from the authorized source package.");
            start += marker.Length;
            int end = manifest.IndexOf('\"', start);
            Assert.Greater(end, start, "The connector package path in the test manifest is incomplete.");
            return Path.GetFullPath(Path.Combine(Path.GetDirectoryName(manifestPath),
                manifest.Substring(start, end - start), "Runtime/TcpConnector/ROSConnection.cs"));
        }

        static bool IsInsideLifecycleLock(string source, int position)
        {
            int search = 0;
            const string lockText = "lock (m_ConnectionLifecycleLock)";
            while ((search = source.IndexOf(lockText, search, StringComparison.Ordinal)) >= 0)
            {
                int bodyStart = source.IndexOf('{', search + lockText.Length);
                int statementEnd = source.IndexOf(';', search + lockText.Length);
                if (bodyStart < 0 || (statementEnd >= 0 && statementEnd < bodyStart))
                {
                    search += lockText.Length;
                    continue;
                }

                int depth = 0;
                for (int index = bodyStart; index < source.Length; index++)
                {
                    if (source[index] == '{') depth++;
                    else if (source[index] == '}') depth--;
                    if (depth == 0)
                    {
                        if (position >= bodyStart && position <= index)
                            return true;
                        break;
                    }
                }
                search += lockText.Length;
            }
            return false;
        }

        [Test]
        public void CancellationChecksAreOutsideLifecycleLock()
        {
            string source = File.ReadAllText(ConnectorSource());
            const string cancellationCheck = "token.ThrowIfCancellationRequested();";
            int position = 0;
            int checks = 0;
            while ((position = source.IndexOf(cancellationCheck, position, StringComparison.Ordinal)) >= 0)
            {
                Assert.IsFalse(IsInsideLifecycleLock(source, position),
                    "Cancellation checks must not execute while holding the lifecycle lock.");
                position += cancellationCheck.Length;
                checks++;
            }
            Assert.Greater(checks, 0, "The transport source must retain an explicit cancellation check.");
        }

        sealed class LiveConnection : IDisposable
        {
            readonly GameObject m_GameObject = new GameObject("session ownership fixture");
            readonly TcpListener m_Listener = new TcpListener(IPAddress.Loopback, 0);
            readonly ROSConnectionConfig m_Config;
            internal readonly ROSConnection Connection;
            internal readonly TcpClient Peer;
            internal readonly object Session;
            internal readonly CancellationTokenSource Cancellation;

            internal LiveConnection()
            {
                Connection = m_GameObject.AddComponent<ROSConnection>();
                Connection.ConnectOnStart = false;
                Connection.listenForTFMessages = false;
                m_Listener.Start();
                m_Config = Config(((IPEndPoint)m_Listener.LocalEndpoint).Port);
                Connection.ConnectionConfig = m_Config;
                Task<TcpClient> accept = m_Listener.AcceptTcpClientAsync();
                Connection.Connect();
                Assert.IsTrue(accept.Wait(TimeSpan.FromSeconds(5)));
                Peer = accept.Result;
                Session = Field(Connection, "m_WorkerSession");
                Cancellation = (CancellationTokenSource)Field(Session, "Cancellation");
            }

            public void Dispose()
            {
                Connection.Disconnect();
                bool stopped = SpinWait.SpinUntil(() => !Connection.HasConnectionThread, TimeSpan.FromSeconds(5));
                Peer.Close();
                m_Listener.Stop();
                UnityEngine.Object.DestroyImmediate(m_GameObject);
                UnityEngine.Object.DestroyImmediate(m_Config);
                Assert.IsTrue(stopped);
            }
        }

        sealed class HeldMessage : TimeMsg
        {
            internal readonly ManualResetEventSlim Entered = new ManualResetEventSlim(false);
            internal readonly ManualResetEventSlim Release = new ManualResetEventSlim(false);
            public override void SerializeTo(MessageSerializer serializer)
            {
                Entered.Set();
                Release.Wait();
                base.SerializeTo(serializer);
            }
        }

        [Test]
        public void DelayedDisconnectCannotClearNewAttempt()
        {
            using (var live = new LiveConnection())
            {
                ROSConnection connection = live.Connection;
                RosTopicState topic = connection.RegisterPublisher<TimeMsg>("/survives-old-stop");
                object attemptA = Field(connection, "m_CurrentAttempt");
                object queueA = Field(attemptA, "Outgoing");
                object incomingA = Field(attemptA, "Incoming");
                connection.Disconnect();
                Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                connection.Connect();
                var held = new HeldMessage();
                Task<Exception> heldPublication = Task.Run(() =>
                {
                    try
                    {
                        connection.Publish(topic.Topic, held);
                        return (Exception)null;
                    }
                    catch (Exception exception) { return exception; }
                });
                try
                {
                    Assert.IsTrue(held.Entered.Wait(TimeSpan.FromSeconds(5)));
                    object attemptB = Field(connection, "m_CurrentAttempt");
                    object queueB = Field(attemptB, "Outgoing");
                    object incomingB = Field(attemptB, "Incoming");
                    var fresh = new TimeMsg(123, 456);
                    connection.Publish(topic.Topic, fresh);
                    Type frameType = typeof(ROSConnection).GetNestedType("IncomingMessage", BindingFlags.NonPublic);
                    object frame = Activator.CreateInstance(frameType, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                        null, new object[] { attemptB, "/incoming-B", new byte[] { 7 } }, null);
                    incomingB.GetType().GetMethod("Enqueue").Invoke(incomingB, new[] { frame });
                    typeof(ROSConnection).GetMethod("ReceiveSysCommand", Private).Invoke(connection,
                        new[] { attemptB, SysCommand.k_SysCommand_ServiceResponse, "{\"srv_id\":777}" });
                    object pairB = Field(attemptB, "PendingPair");
                    int queuedB = (int)queueB.GetType().GetProperty("Count").GetValue(queueB);
                    Assert.IsTrue(topic.SentPublisherRegistration);
                    Assert.AreNotSame(queueA, queueB, "Each attempt must own a different outbox root.");
                    Assert.AreNotSame(incomingA, incomingB, "Each attempt must own a different mailbox root.");
                    typeof(ROSConnection).GetMethod("RetireAttempt", Private).Invoke(connection, new[] { attemptA });
                    Assert.IsTrue(topic.SentPublisherRegistration, "Old retirement must not reset B's registration state.");
                    Assert.AreEqual(queuedB, queueB.GetType().GetProperty("Count").GetValue(queueB));
                    Assert.AreEqual(1, incomingB.GetType().GetProperty("Count").GetValue(incomingB));
                    Assert.AreSame(pairB, Field(attemptB, "PendingPair"));
                    if (queuedB > 0)
                    {
                        Assert.IsTrue(topic.MessageSender.PeekNextMessageToSend(out Message pending));
                        Assert.AreSame(fresh, pending, "Old drain must not release B's queued publication.");
                    }
                }
                finally
                {
                    held.Release.Set();
                    Assert.IsTrue(heldPublication.Wait(TimeSpan.FromSeconds(5)),
                        "Caller-side publication must finish after its serializer is released.");
                    Assert.IsNull(heldPublication.Result,
                        "The captured replacement-epoch publication should succeed before teardown.");
                    connection.Disconnect();
                    Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                    held.Entered.Dispose();
                    held.Release.Dispose();
                }
            }
        }

        [Test]
        public void ForgedNumericAttemptIdentityCannotDeliverAnOldIncomingFrame()
        {
            var gameObject = new GameObject("reference attempt authority");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                connection.listenForTFMessages = false;
                int deliveries = 0;
                connection.Subscribe<TimeMsg>("/reference-authority", _ => deliveries++);

                // These intentionally distinct owner objects have matching diagnostic IDs.
                // A delayed A frame must not gain B's authority merely by carrying that number.
                const long sharedDiagnosticId = 91;
                AttemptTestAccess.Replace(connection, sharedDiagnosticId);
                object attemptA = AttemptTestAccess.Current(connection);
                AttemptTestAccess.Replace(connection, sharedDiagnosticId);
                object attemptB = AttemptTestAccess.Current(connection);
                Assert.AreNotSame(attemptA, attemptB);

                Type frameType = typeof(ROSConnection).GetNestedType("IncomingMessage", BindingFlags.NonPublic);
                ConstructorInfo constructor = frameType.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)[0];
                Type authorityType = constructor.GetParameters()[0].ParameterType;
                object authority = authorityType == typeof(long)
                    ? Field(attemptA, "Id")
                    : attemptA;
                var serializer = new MessageSerializer();
                serializer.SerializeMessage(new TimeMsg(7, 7));
                object staleFrame = constructor.Invoke(new object[] { authority, "/reference-authority", serializer.GetBytes() });

                typeof(ROSConnection).GetMethod("DispatchIncomingMessage", BindingFlags.Instance | BindingFlags.NonPublic)
                    .Invoke(connection, new[] { staleFrame });

                Assert.AreEqual(0, deliveries,
                    "Reference-distinct attempt A must not deliver through B just because a numeric diagnostic ID matches.");
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void ForgedNumericAttemptIdentityCannotCommitSenderCache()
        {
            var gameObject = new GameObject("reference cache authority");
            TopicMessageSender sender = null;
            object publication = null;
            object use = null;
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                connection.listenForTFMessages = false;
                AttemptTestAccess.Replace(connection, 92L);
                object attemptA = AttemptTestAccess.Current(connection);
                AttemptTestAccess.Replace(connection, 92L);
                object attemptB = AttemptTestAccess.Current(connection);
                Assert.AreNotSame(attemptA, attemptB);

                sender = new TopicMessageSender("/reference-cache", TimeMsg.k_RosMessageName, 1);
                MethodInfo acquire = typeof(TopicMessageSender).GetMethod("AcquireMessageUse", BindingFlags.Instance | BindingFlags.NonPublic);
                use = acquire.Invoke(sender, new object[] { new TimeMsg(3, 4) });
                Type publicationType = typeof(TopicMessageSender).GetNestedType("OwnedPublication", BindingFlags.NonPublic);
                publication = Activator.CreateInstance(publicationType, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                    null, new[] { use }, null);

                MethodInfo commit = typeof(ROSConnection).GetMethod("TryCommitSenderCache", BindingFlags.Instance | BindingFlags.NonPublic);
                Type authorityType = commit.GetParameters()[2].ParameterType;
                object staleAuthority = authorityType == typeof(long) ? Field(attemptA, "Id") : attemptA;
                commit.Invoke(connection, new[] { sender, publication, staleAuthority });

                FieldInfo cache = typeof(TopicMessageSender).GetField("m_LastMessageSent", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.IsNull(cache.GetValue(sender),
                    "A stale reference-distinct attempt must not commit a sender cache through a matching diagnostic ID.");
            }
            finally
            {
                if (sender != null)
                {
                    MethodInfo invalidate = typeof(TopicMessageSender).GetMethod("InvalidateCache", BindingFlags.Instance | BindingFlags.NonPublic);
                    object cachedUse = invalidate.Invoke(sender, null);
                    if (cachedUse != null)
                        cachedUse.GetType().GetMethod("Release", BindingFlags.Instance | BindingFlags.NonPublic)
                            .Invoke(cachedUse, new object[] { false });
                }
                if (publication != null && sender != null)
                {
                    MethodInfo releaseLocked = publication.GetType().GetMethod("ReleaseLocked", BindingFlags.Instance | BindingFlags.NonPublic);
                    object remainingUse = releaseLocked.Invoke(publication, null);
                    if (remainingUse != null)
                        remainingUse.GetType().GetMethod("Release", BindingFlags.Instance | BindingFlags.NonPublic)
                            .Invoke(remainingUse, new object[] { false });
                }
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void OutboundQueueEntryCarriesReferenceAttemptAuthority()
        {
            var gameObject = new GameObject("reference outbound authority");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                connection.listenForTFMessages = false;
                AttemptTestAccess.Replace(connection, 93L);
                object attempt = AttemptTestAccess.Current(connection);
                connection.QueueSysCommand("__test", new SysCommand_TopicsRequest());

                object queue = Field(attempt, "Outgoing");
                object[] arguments = { null };
                Assert.IsTrue((bool)queue.GetType().GetMethod("TryDequeue").Invoke(queue, arguments));
                object entry = arguments[0];
                FieldInfo authority = entry.GetType().GetField("Attempt", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                Assert.NotNull(authority, "An outbound unit must retain its admitting AttemptContext reference.");
                Assert.AreSame(attempt, authority.GetValue(entry));
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void ForgedNumericAttemptIdentityCannotNotifyTopicCallback()
        {
            var gameObject = new GameObject("reference topic callback authority");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                connection.listenForTFMessages = false;
                RosTopicState topic = connection.GetOrCreateTopic("/reference-topic-callback", TimeMsg.k_RosMessageName);
                int deliveries = 0;
                topic.AddSubscriber(_ => deliveries++);
                AttemptTestAccess.Replace(connection, 94L);
                object attemptA = AttemptTestAccess.Current(connection);
                AttemptTestAccess.Replace(connection, 94L);
                object attemptB = AttemptTestAccess.Current(connection);
                Assert.AreNotSame(attemptA, attemptB);

                MethodInfo notify = typeof(RosTopicState).GetMethod("NotifyMessageForAttempt", BindingFlags.Instance | BindingFlags.NonPublic);
                Type authorityType = notify.GetParameters()[1].ParameterType;
                object staleAuthority = authorityType == typeof(long) ? Field(attemptA, "Id") : attemptA;
                bool notified = (bool)notify.Invoke(topic, new object[] { new TimeMsg(5, 6), staleAuthority });

                Assert.IsFalse(notified, "A stale attempt reference must not grant a topic callback permit through a matching diagnostic ID.");
                Assert.AreEqual(0, deliveries);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void DomainLifetimeAdaptersOwnConnectorStopAndRecyclerShutdown()
        {
            Assembly assembly = typeof(ROSConnection).Assembly;
            Type lifetime = assembly.GetType("Unity.Robotics.ROSTCPConnector.ConnectorDomainLifetime");
            Assert.NotNull(lifetime, "The connector needs one domain-lifetime owner for runtime/editor shutdown.");
            Assert.NotNull(lifetime.GetMethod("Register", BindingFlags.Static | BindingFlags.NonPublic));
            Assert.NotNull(lifetime.GetMethod("Unregister", BindingFlags.Static | BindingFlags.NonPublic));
            Assert.NotNull(lifetime.GetMethod("RequestStop", BindingFlags.Static | BindingFlags.NonPublic));
            Assert.NotNull(lifetime.GetMethod("OnApplicationQuitting", BindingFlags.Static | BindingFlags.NonPublic));
            Assert.NotNull(lifetime.GetMethod("OnBeforeAssemblyReload", BindingFlags.Static | BindingFlags.NonPublic));
            Assert.NotNull(lifetime.GetMethod("OnPlayModeStateChanged", BindingFlags.Static | BindingFlags.NonPublic));

            Type recycler = assembly.GetType("Unity.Robotics.ROSTCPConnector.MessageRecycler");
            Assert.NotNull(recycler, "Message recycling must remain one domain-owned worker.");
            Assert.NotNull(recycler.GetMethod("RequestStop", BindingFlags.Static | BindingFlags.NonPublic));
            Assert.NotNull(recycler.GetMethod("DropPendingReturns", BindingFlags.Static | BindingFlags.NonPublic));
        }

        sealed class CountingRecyclePool : IMessagePool, IDisposable
        {
            internal readonly ManualResetEventSlim Offered = new ManualResetEventSlim(false);
            internal int Offers;

            public void AddMessage(Message message)
            {
                Interlocked.Increment(ref Offers);
                Offered.Set();
            }

            public void Dispose()
            {
                Offered.Dispose();
            }
        }

        static void QueueAndClearStandalonePublication(TopicMessageSender sender, Message source)
        {
            MethodInfo acquire = typeof(TopicMessageSender).GetMethod("AcquireMessageUse", Private);
            MethodInfo queue = typeof(TopicMessageSender).GetMethod("Queue", Private);
            object use = acquire.Invoke(sender, new object[] { source });
            object displaced = queue.Invoke(sender, new[] { use });
            if (displaced != null)
                displaced.GetType().GetMethod("Release", Private).Invoke(displaced, new object[] { true });
            sender.ClearAllQueuedData();
        }

        [Test]
        public void DomainLifetimeStopsRegisteredLiveConnection()
        {
            using (var live = new LiveConnection())
            {
                Type lifetime = typeof(ROSConnection).Assembly.GetType(
                    "Unity.Robotics.ROSTCPConnector.ConnectorDomainLifetime");
                MethodInfo stop = lifetime.GetMethod("OnBeforeAssemblyReload",
                    BindingFlags.Static | BindingFlags.NonPublic);
                Assert.NotNull(stop);

                stop.Invoke(null, null);

                Assert.IsTrue(SpinWait.SpinUntil(() => !live.Connection.HasConnectionThread,
                    TimeSpan.FromSeconds(5)),
                    "The domain stop hook must revoke and stop a registered live connection.");
            }
        }

        [Test]
        public void MessageRecyclerRestartsPoolingAfterCompletedDomainStop()
        {
            Type assemblyType = typeof(ROSConnection).Assembly.GetType("Unity.Robotics.ROSTCPConnector.ConnectorDomainLifetime");
            Type recyclerType = typeof(ROSConnection).Assembly.GetType("Unity.Robotics.ROSTCPConnector.MessageRecycler");
            MethodInfo stop = assemblyType.GetMethod("OnBeforeAssemblyReload", BindingFlags.Static | BindingFlags.NonPublic);
            FieldInfo threadField = recyclerType.GetField("s_Thread", BindingFlags.Static | BindingFlags.NonPublic);
            PropertyInfo accepting = recyclerType.GetProperty("AcceptingOffers", BindingFlags.Static | BindingFlags.NonPublic);
            Assert.NotNull(stop);
            Assert.NotNull(threadField);
            Assert.NotNull(accepting);

            var firstPool = new CountingRecyclePool();
            var secondPool = new CountingRecyclePool();
            var firstSender = new TopicMessageSender("/recycler-first", TimeMsg.k_RosMessageName, 1);
            var secondSender = new TopicMessageSender("/recycler-second", TimeMsg.k_RosMessageName, 1);
            try
            {
                firstSender.SetMessagePool(firstPool);
                QueueAndClearStandalonePublication(firstSender, new TimeMsg(1, 2));
                Assert.IsTrue(firstPool.Offered.Wait(TimeSpan.FromSeconds(3)),
                    "The first ownership generation must reach the domain recycler.");

                Thread firstThread = (Thread)threadField.GetValue(null);
                Assert.NotNull(firstThread);
                stop.Invoke(null, null);
                Assert.IsTrue(SpinWait.SpinUntil(() => !firstThread.IsAlive, TimeSpan.FromSeconds(3)),
                    "The completed domain stop must let the old recycler finish without joining it under a lifecycle lock.");
                Assert.IsNull(threadField.GetValue(null),
                    "A completed recycler must release its old thread identity before restart.");

                secondSender.SetMessagePool(secondPool);
                QueueAndClearStandalonePublication(secondSender, new TimeMsg(3, 4));
                Assert.IsTrue(secondPool.Offered.Wait(TimeSpan.FromSeconds(3)),
                    "A later same-AppDomain connection must be able to recycle after the old recycler drained.");
                Assert.AreEqual(1, Volatile.Read(ref secondPool.Offers));
                Assert.IsTrue((bool)accepting.GetValue(null, null),
                    "Completed recycler shutdown must re-enable later pooling.");
                Thread secondThread = (Thread)threadField.GetValue(null);
                Assert.NotNull(secondThread, "A later offer must have a live domain recycler owner.");
                Assert.AreNotSame(firstThread, secondThread,
                    "A later offer must not reuse the completed recycler thread identity.");
            }
            finally
            {
                firstSender.Dispose();
                secondSender.Dispose();
                try { stop.Invoke(null, null); } catch (Exception) { }
                Thread remaining = (Thread)threadField.GetValue(null);
                if (remaining != null)
                    SpinWait.SpinUntil(() => !remaining.IsAlive, TimeSpan.FromSeconds(3));
                firstPool.Dispose();
                secondPool.Dispose();
            }
        }

        [Test]
        public void UnexpectedWorkerFaultStillNeedsExplicitDisconnect()
        {
            using (var live = new LiveConnection())
            using (var workerBodyCompleted = new ManualResetEventSlim(false))
            using (var releaseWorkerBody = new ManualResetEventSlim(false))
            {
                ROSConnection connection = live.Connection;
                ROSConnectionConfig replacement = Config(connection.RosPort);
                Hook(connection, "m_WorkerBeforeCloseTestHook", () =>
                    throw new InvalidOperationException("controlled worker finalization fault"));
                Hook(connection, "m_WorkerBodyCompletedTestHook", () =>
                {
                    workerBodyCompleted.Set();
                    releaseWorkerBody.Wait(TimeSpan.FromSeconds(5));
                });
                try
                {
                    LogAssert.Expect(LogType.Error, new Regex("ROS Connection to .* failed -"));
                    LogAssert.Expect(LogType.Exception, new Regex(".*controlled worker finalization fault.*"));
                    live.Peer.Close();
                    Assert.IsTrue(workerBodyCompleted.Wait(TimeSpan.FromSeconds(5)),
                        "The faulted worker must reach its terminal finalization path.");
                    releaseWorkerBody.Set();
                    Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread,
                        TimeSpan.FromSeconds(5)));
                    Assert.Throws<InvalidOperationException>(() => connection.ConnectionConfig = replacement,
                        "An unexpected worker fault must not authorize config replacement by itself.");
                    connection.Disconnect();
                    Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread,
                        TimeSpan.FromSeconds(5)));
                    Assert.DoesNotThrow(() => connection.ConnectionConfig = replacement,
                        "Explicit Disconnect must complete the remaining terminal cleanup.");
                }
                finally
                {
                    releaseWorkerBody.Set();
                    Hook(connection, "m_WorkerBodyCompletedTestHook", null);
                    Hook(connection, "m_WorkerBeforeCloseTestHook", null);
                    UnityEngine.Object.DestroyImmediate(replacement);
                }
            }
        }

        [Test]
        public void RevokedAttemptReaderInterruptionIsNotAConnectionError()
        {
            using (var live = new LiveConnection())
            using (var closeClaimed = new ManualResetEventSlim(false))
            using (var releaseClose = new ManualResetEventSlim(false))
            using (var readerTerminated = new ManualResetEventSlim(false))
            {
                SendFrame(live.Peer, SysCommand.k_SysCommand_Handshake, new byte[0]);
                Assert.IsTrue(SpinWait.SpinUntil(() => !live.Connection.HasConnectionError,
                    TimeSpan.FromSeconds(5)), "The reader must consume the handshake before the stop race.");
                Hook(live.Connection, "m_SocketCloseTestHook", () =>
                {
                    closeClaimed.Set();
                    releaseClose.Wait(TimeSpan.FromSeconds(5));
                });
                Hook(live.Connection, "m_ReaderTerminatingTestHook", () => readerTerminated.Set());
                Task stop = Task.Run(() => live.Connection.Disconnect());
                try
                {
                    Assert.IsTrue(closeClaimed.Wait(TimeSpan.FromSeconds(5)),
                        "Disconnect must revoke the attempt before claiming the socket close.");
                    live.Peer.Close();
                    Assert.IsTrue(readerTerminated.Wait(TimeSpan.FromSeconds(5)),
                        "Closing the revoked attempt's peer must interrupt its reader.");
                    Assert.IsFalse(live.Connection.HasConnectionError,
                        "An expected reader interruption after attempt revocation is not a connection error.");
                    LogAssert.NoUnexpectedReceived();
                }
                finally
                {
                    releaseClose.Set();
                    Assert.IsTrue(stop.Wait(TimeSpan.FromSeconds(5)));
                    Hook(live.Connection, "m_SocketCloseTestHook", null);
                    Hook(live.Connection, "m_ReaderTerminatingTestHook", null);
                }
            }
        }

        [Test]
        public void WorkerSessionOwnsWakeAndAttemptOwnsQueuedCleanup()
        {
            Type connectionType = typeof(ROSConnection);
            Type sessionType = connectionType.GetNestedType("WorkerSession", BindingFlags.NonPublic);
            Type queueType = connectionType.GetNestedType("OutgoingMessageQueue", BindingFlags.NonPublic);
            Assert.NotNull(sessionType);
            Assert.NotNull(queueType);
            Assert.NotNull(sessionType.GetField("WakeSignal", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic),
                "WorkerSession must own the worker wake signal.");
            Assert.NotNull(queueType.GetMethod("ClearQueuedData", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic),
                "Attempt outboxes must clear only their own queued units.");
            Assert.IsNull(connectionType.GetMethod("ClearMessageQueue", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic),
                "Connection-global queue drains are not an ownership boundary.");
        }

        [Test]
        public void AttemptAuthorityUsesReferencesAcrossLifecycleBoundaries()
        {
            Type connectionType = typeof(ROSConnection);
            Type attemptType = connectionType.GetNestedType("AttemptContext", BindingFlags.NonPublic);
            Type frameType = connectionType.GetNestedType("IncomingMessage", BindingFlags.NonPublic);
            Type serviceCallType = connectionType.GetNestedType("ServiceCall", BindingFlags.NonPublic);
            Type queueType = connectionType.GetNestedType("OutgoingMessageQueue", BindingFlags.NonPublic);
            Type entryType = queueType.GetNestedType("Entry", BindingFlags.NonPublic);
            MethodInfo enqueue = queueType.GetMethod("Enqueue", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            MethodInfo connectionThread = connectionType.GetMethod("ConnectionThread", BindingFlags.Instance | BindingFlags.NonPublic);
            ConstructorInfo frameConstructor = frameType.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)[0];

            Assert.AreEqual(attemptType, frameConstructor.GetParameters()[0].ParameterType);
            Assert.AreEqual(attemptType, enqueue.GetParameters()[0].ParameterType);
            Assert.AreEqual(attemptType, connectionThread.GetParameters()[1].ParameterType);
            Assert.NotNull(entryType.GetField("Attempt", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic));
            Assert.IsNull(entryType.GetField("AttemptId", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic));
            Assert.IsNull(serviceCallType.GetField("AttemptId", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic));
            Assert.IsNull(connectionType.GetMethod("IsAttemptAccepting", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                null, new[] { typeof(long) }, null));
            Assert.IsNull(connectionType.GetMethod("TryGetAcceptingAttempt", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                null, new[] { typeof(long).MakeByRefType() }, null));
        }

        sealed class FailingWriteMessage : TimeMsg
        {
            public override void SerializeTo(MessageSerializer serializer)
            {
                throw new IOException("controlled writer failure");
            }
        }

        sealed class BlockingQueueSender : OutgoingMessageSender
        {
            internal readonly ManualResetEventSlim Entered = new ManualResetEventSlim(false);
            internal readonly ManualResetEventSlim Release = new ManualResetEventSlim(false);
            public override SendToState SendInternal(MessageSerializer messageSerializer, Stream stream)
            {
                Entered.Set();
                Release.Wait();
                return SendToState.Normal;
            }
            public override void ClearAllQueuedData() { }
        }

        sealed class RecordingQueueSender : OutgoingMessageSender
        {
            int m_Sends;
            internal int Sends => Volatile.Read(ref m_Sends);
            public override SendToState SendInternal(MessageSerializer messageSerializer, Stream stream)
            {
                Interlocked.Increment(ref m_Sends);
                return SendToState.Normal;
            }
            public override void ClearAllQueuedData() { }
        }

        static void EnqueueAttemptSender(object attempt, OutgoingMessageSender sender)
        {
            object queue = Field(attempt, "Outgoing");
            queue.GetType().GetMethod("Enqueue").Invoke(queue, new[] { attempt, sender });
        }

        [Test]
        public void WriterDoesNotClaimNextQueuedUnitAfterRevocationBeforeCancel()
        {
            using (var live = new LiveConnection())
            using (var stopCaptured = new ManualResetEventSlim(false))
            using (var releaseStop = new ManualResetEventSlim(false))
            {
                var first = new BlockingQueueSender();
                var second = new RecordingQueueSender();
                object attempt = Field(live.Connection, "m_CurrentAttempt");
                EnqueueAttemptSender(attempt, first);
                EnqueueAttemptSender(attempt, second);
                Assert.IsTrue(first.Entered.Wait(TimeSpan.FromSeconds(5)), "The writer must claim the first unit before stop starts.");
                Hook(live.Connection, "m_StopPlanCapturedTestHook", () => { stopCaptured.Set(); releaseStop.Wait(); });
                Task stop = Task.Run(() => live.Connection.Disconnect());
                try
                {
                    Assert.IsTrue(stopCaptured.Wait(TimeSpan.FromSeconds(5)), "Stop must revoke the attempt before cancellation is issued.");
                    first.Release.Set();
                    SpinWait.SpinUntil(() => second.Sends > 0, TimeSpan.FromMilliseconds(500));
                    Assert.AreEqual(0, second.Sends, "No queued unit may be claimed after its attempt was revoked, even before cancellation is issued.");
                }
                finally
                {
                    first.Release.Set();
                    releaseStop.Set();
                    Assert.IsTrue(stop.Wait(TimeSpan.FromSeconds(5)));
                    first.Entered.Dispose();
                    first.Release.Dispose();
                    Hook(live.Connection, "m_StopPlanCapturedTestHook", null);
                }
            }
        }

        [Test]
        public void AttemptAdmissionClosesBeforeReaderJoin()
        {
            using (var live = new LiveConnection())
            using (var terminating = new ManualResetEventSlim(false))
            using (var release = new ManualResetEventSlim(false))
            {
                ROSConnection connection = live.Connection;
                object attempt = Field(connection, "m_CurrentAttempt");
                Assert.IsTrue(SpinWait.SpinUntil(() => Field(attempt, "State").ToString() == "Ready", TimeSpan.FromSeconds(5)));
                connection.RegisterRosService<TimeMsg, TimeMsg>("/during-reader-join");
                connection.RegisterPublisher<TimeMsg>("/writer-failure");
                Hook(connection, "m_ReaderTerminatingTestHook", () => { terminating.Set(); release.Wait(); });
                try
                {
                    connection.Publish("/writer-failure", new TimeMsg());
                    live.Peer.Close();
                    Assert.IsTrue(terminating.Wait(TimeSpan.FromSeconds(5)), "Reader must still own its termination path.");
                    Assert.AreSame(attempt, Field(connection, "m_CurrentAttempt"), "Retry cannot start while reader teardown is held.");
                    Assert.Throws<IOException>(() => connection.GetTopicList(_ => Assert.Fail()),
                        "Failed attempt must revoke admission before waiting for its reader.");
                    Task<TimeMsg> request = connection.SendServiceMessage<TimeMsg>("/during-reader-join", new TimeMsg());
                    Assert.IsTrue(request.IsFaulted);
                    Assert.IsInstanceOf<IOException>(request.Exception.InnerException);
                    Assert.IsTrue(connection.HasConnectionThread);
                    Assert.Throws<InvalidOperationException>(() => connection.ConnectionConfig = connection.ConnectionConfig);
                }
                finally
                {
                    release.Set();
                    connection.Disconnect();
                    Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                    Hook(connection, "m_ReaderTerminatingTestHook", null);
                }
            }
        }

        [Test]
        public void ClosingSocketOwnerPinsWorkerUntilCloseReturns()
        {
            using (var live = new LiveConnection())
            using (var closing = new ManualResetEventSlim(false))
            using (var release = new ManualResetEventSlim(false))
            using (var workerCloseReturned = new ManualResetEventSlim(false))
            {
                int closeCalls = 0;
                Assert.IsTrue(SpinWait.SpinUntil(() => Field(Field(live.Connection, "m_CurrentAttempt"), "State").ToString() == "Ready", TimeSpan.FromSeconds(5)));
                Hook(live.Connection, "m_WorkerBeforeCloseTestHook", () => closing.Wait(TimeSpan.FromSeconds(5)));
                Hook(live.Connection, "m_SocketCloseTestHook", () =>
                {
                    if (Interlocked.Increment(ref closeCalls) == 1) { closing.Set(); release.Wait(); }
                });
                Hook(live.Connection, "m_WorkerSocketCloseReturnedTestHook", () => workerCloseReturned.Set());
                Task stop = Task.Run(() => live.Connection.Disconnect());
                try
                {
                    Assert.IsTrue(closing.Wait(TimeSpan.FromSeconds(5)));
                    live.Peer.Close();
                    Assert.IsTrue(workerCloseReturned.Wait(TimeSpan.FromSeconds(5)));
                    Assert.AreEqual(1, Volatile.Read(ref closeCalls), "Only one caller may execute this socket's close.");
                    Assert.AreEqual(false, Field(live.Session, "WorkerTerminal"), "Seeing Closing is not proof of completed disposal.");
                    Assert.IsTrue(live.Connection.HasConnectionThread);
                    Assert.Throws<InvalidOperationException>(() => live.Connection.ConnectionConfig = live.Connection.ConnectionConfig);
                }
                finally
                {
                    release.Set();
                    Assert.IsTrue(stop.Wait(TimeSpan.FromSeconds(5)));
                    Assert.IsTrue(SpinWait.SpinUntil(() => !live.Connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                    Hook(live.Connection, "m_SocketCloseTestHook", null);
                    Hook(live.Connection, "m_WorkerBeforeCloseTestHook", null);
                    Hook(live.Connection, "m_WorkerSocketCloseReturnedTestHook", null);
                }
                Assert.AreEqual(1, closeCalls);
            }
        }

        [TestCase("worker-start")]
        [TestCase("client-publication")]
        [TestCase("connect-async")]
        [TestCase("before-registration")]
        [TestCase("registration-write")]
        public void StopAtEveryWorkerStartupBoundary(string boundary)
        {
            var gameObject = new GameObject("startup stop boundary " + boundary);
            var listener = new TcpListener(IPAddress.Loopback, 0);
            ROSConnection connection = gameObject.AddComponent<ROSConnection>();
            connection.ConnectOnStart = false;
            connection.listenForTFMessages = false;
            ROSConnectionConfig config = null;
            ROSConnectionConfig replacement = null;
            TcpClient accepted = null;
            Task<TcpClient> accept = null;
            using (var entered = new ManualResetEventSlim(false))
            using (var release = new ManualResetEventSlim(false))
            using (var stopCaptured = new ManualResetEventSlim(false))
            {
                Action hook = () => { entered.Set(); release.Wait(); };
                try
                {
                    listener.Start();
                    int port = ((IPEndPoint)listener.LocalEndpoint).Port;
                    config = Config(port);
                    replacement = Config(port);
                    connection.ConnectionConfig = config;
                    if (boundary == "registration-write")
                        connection.RegisterPublisher<TimeMsg>("/startup-boundary");
                    string hookName = boundary == "worker-start" ? "m_BeforeWorkerStartGateTestHook"
                        : boundary == "client-publication" ? "m_BeforeClientPublicationTestHook"
                        : boundary == "connect-async" ? "m_BeforeConnectAttemptTestHook"
                        : boundary == "before-registration" ? "m_BeforeStartupRegistrationsTestHook"
                        : "m_BeforeImmediateCommandWriteTestHook";
                    Hook(connection, hookName, hook);
                    Hook(connection, "m_StopPlanCapturedTestHook", () => stopCaptured.Set());
                    accept = listener.AcceptTcpClientAsync();
                    connection.Connect();
                    if (boundary != "worker-start" && boundary != "client-publication" && boundary != "connect-async")
                    {
                        Assert.IsTrue(accept.Wait(TimeSpan.FromSeconds(5)));
                        accepted = accept.Result;
                    }
                    Assert.IsTrue(entered.Wait(TimeSpan.FromSeconds(5)), "The requested startup boundary was not reached: " + boundary);
                    Task stop = Task.Run(() => connection.Disconnect());
                    Assert.IsTrue(stopCaptured.Wait(TimeSpan.FromSeconds(5)),
                        "Disconnect must capture the stop plan before the startup barrier is released: " + boundary);
                    Assert.Throws<InvalidOperationException>(() => connection.ConnectionConfig = replacement,
                        "A paused startup boundary still owns the worker session: " + boundary);
                    connection.Connect();
                    Assert.IsFalse(listener.Pending(), "Stop/Connect at startup boundary must not create a second client: " + boundary);
                    release.Set();
                    Assert.IsTrue(stop.Wait(TimeSpan.FromSeconds(5)));
                    Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                    Assert.DoesNotThrow(() => connection.ConnectionConfig = replacement);
                }
                finally
                {
                    release.Set();
                    Hook(connection, "m_BeforeWorkerStartGateTestHook", null);
                    Hook(connection, "m_BeforeClientPublicationTestHook", null);
                    Hook(connection, "m_BeforeConnectAttemptTestHook", null);
                    Hook(connection, "m_BeforeStartupRegistrationsTestHook", null);
                    Hook(connection, "m_BeforeImmediateCommandWriteTestHook", null);
                    Hook(connection, "m_StopPlanCapturedTestHook", null);
                    connection.Disconnect();
                    SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5));
                    accepted?.Close();
                    listener.Stop();
                    UnityEngine.Object.DestroyImmediate(gameObject);
                    if (config != null) UnityEngine.Object.DestroyImmediate(config);
                    if (replacement != null) UnityEngine.Object.DestroyImmediate(replacement);
                }
            }
        }

        [Test]
        public void RepeatedDisconnectWithoutWorkerRetainsSingleStopOwner()
        {
            var gameObject = new GameObject("stopped core stop owner");
            var connection = gameObject.AddComponent<ROSConnection>();
            connection.ConnectOnStart = false;
            connection.listenForTFMessages = false;
            var config = Config(1);
            connection.ConnectionConfig = config;
            Task firstStop = null;
            using (var captured = new ManualResetEventSlim(false))
            using (var release = new ManualResetEventSlim(false))
            {
                int plans = 0;
                Hook(connection, "m_StopPlanCapturedTestHook", () =>
                {
                    if (Interlocked.Increment(ref plans) == 1)
                    {
                        captured.Set();
                        release.Wait();
                    }
                });
                try
                {
                    firstStop = Task.Run(() => connection.Disconnect());
                    Assert.IsTrue(captured.Wait(TimeSpan.FromSeconds(5)));
                    connection.Disconnect();
                    Assert.Throws<InvalidOperationException>(() => connection.ConnectionConfig = config,
                        "A second no-worker stop must not authorize replacement while the first owns cleanup.");
                    Assert.AreEqual(1, plans, "Repeated Disconnect must reuse the core stop claim even without a session.");
                    connection.Connect();
                    Assert.IsFalse(connection.HasConnectionThread);
                    release.Set();
                    Assert.IsTrue(firstStop.Wait(TimeSpan.FromSeconds(5)));
                    Assert.DoesNotThrow(() => connection.ConnectionConfig = config);
                }
                finally
                {
                    release.Set();
                    if (firstStop != null) Assert.IsTrue(firstStop.Wait(TimeSpan.FromSeconds(5)));
                    Hook(connection, "m_StopPlanCapturedTestHook", null);
                    connection.Disconnect();
                    Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                    UnityEngine.Object.DestroyImmediate(gameObject);
                    UnityEngine.Object.DestroyImmediate(config);
                }
            }
        }

        [Test]
        public void CancellationCallbackRunsOutsideCoreGate()
        {
            using (var live = new LiveConnection())
            using (var entered = new ManualResetEventSlim(false))
            using (var release = new ManualResetEventSlim(false))
            using (live.Cancellation.Token.Register(() => { entered.Set(); release.Wait(); }))
            {
                var socket = ((TcpClient)Field(Field(live.Connection, "m_CurrentAttempt"), "Client")).Client;
                Task stop = Task.Run(() => live.Connection.Disconnect());
                try
                {
                    Assert.IsTrue(entered.Wait(TimeSpan.FromSeconds(5)));
                    object gate = Field(live.Connection, "m_ConnectionLifecycleLock");
                    Task<bool> query = Task.Run(() => { lock (gate) return live.Connection.HasConnectionThread; });
                    Assert.IsTrue(query.Wait(TimeSpan.FromSeconds(2)), "The cancellation callback must not hold G.");
                    Assert.IsTrue(query.Result, "The stop callback retains a session lifetime claim.");
                    Assert.Throws<ObjectDisposedException>(() => socket.Poll(0, SelectMode.SelectRead),
                        "The captured socket must be closed before cancellation callbacks start.");
                    Assert.Throws<IOException>(() => live.Connection.GetTopicList(_ => Assert.Fail()));
                }
                finally
                {
                    release.Set();
                    Assert.IsTrue(stop.Wait(TimeSpan.FromSeconds(5)));
                }
            }
        }

        [Test]
        public void CancellationFailureCannotSkipLaterStopPhases()
        {
            using (var live = new LiveConnection())
            {
                live.Connection.RegisterRosService<TimeMsg, TimeMsg>("/pending-stop");
                Task<TimeMsg> request = live.Connection.SendServiceMessage<TimeMsg>("/pending-stop", new TimeMsg());
                var socket = ((TcpClient)Field(Field(live.Connection, "m_CurrentAttempt"), "Client")).Client;
                int cancellations = 0;
                int disposals = 0;
                Hook(live.Connection, "m_SessionDisposalTestHook", () => Interlocked.Increment(ref disposals));
                using (live.Cancellation.Token.Register(() =>
                {
                    Interlocked.Increment(ref cancellations);
                    throw new InvalidOperationException("controlled cancellation phase failure");
                }))
                {
                    Assert.Throws<AggregateException>(() => live.Connection.Disconnect());
                    live.Connection.Disconnect();
                    Assert.IsTrue(request.IsFaulted, "Throwing cancellation must not skip pending-call invalidation.");
                    Assert.IsInstanceOf<IOException>(request.Exception.InnerException);
                    Assert.Throws<ObjectDisposedException>(() => socket.Poll(0, SelectMode.SelectRead));
                    Assert.IsTrue(SpinWait.SpinUntil(() => !live.Connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                    Assert.AreEqual(1, cancellations);
                    Assert.AreEqual(1, disposals);
                    Assert.AreEqual(true, Field(live.Session, "StopCompleted"));
                    Assert.AreEqual(true, Field(live.Session, "ResourcesDisposed"));
                    Assert.DoesNotThrow(() => live.Connection.ConnectionConfig = live.Connection.ConnectionConfig);
                }
                Hook(live.Connection, "m_SessionDisposalTestHook", null);
            }
        }

        [Test]
        public void DisconnectCompletesFailureSelectedByConcurrentRetirement()
        {
            using (var live = new LiveConnection())
            using (var selected = new ManualResetEventSlim(false))
            using (var release = new ManualResetEventSlim(false))
            {
                live.Connection.RegisterRosService<TimeMsg, TimeMsg>("/retirement-race");
                Task<TimeMsg> request = live.Connection.SendServiceMessage<TimeMsg>("/retirement-race", new TimeMsg());
                Hook(live.Connection, "m_ServiceFailureSelectedTestHook", () => { selected.Set(); release.Wait(); });
                Task retirement = Task.Run(() => AttemptTestAccess.Retire(live.Connection));
                try
                {
                    Assert.IsTrue(selected.Wait(TimeSpan.FromSeconds(5)));
                    live.Connection.Disconnect();
                    Assert.IsTrue(request.IsFaulted, "Disconnect must deliver even a concurrently selected invalidation before returning.");
                    Assert.IsInstanceOf<IOException>(request.Exception.InnerException);
                }
                finally
                {
                    release.Set();
                    Assert.IsTrue(retirement.Wait(TimeSpan.FromSeconds(5)));
                    Hook(live.Connection, "m_ServiceFailureSelectedTestHook", null);
                }
            }
        }

        [Test]
        public void ExplicitDisconnectDuringSessionDisposalIsNotLost()
        {
            var gameObject = new GameObject("stop during session disposal");
            var listener = new TcpListener(IPAddress.Loopback, 0);
            var connection = gameObject.AddComponent<ROSConnection>();
            connection.ConnectOnStart = false;
            connection.listenForTFMessages = false;
            ROSConnectionConfig config = null;
            ROSConnectionConfig replacement = null;
            TcpClient peer = null;
            using (var disposing = new ManualResetEventSlim(false))
            using (var release = new ManualResetEventSlim(false))
            {
                try
                {
                    listener.Start();
                    int port = ((IPEndPoint)listener.LocalEndpoint).Port;
                    config = Config(port);
                    replacement = Config(port);
                    connection.ConnectionConfig = config;
                    Task<TcpClient> accept = listener.AcceptTcpClientAsync();
                    connection.Connect();
                    Assert.IsTrue(accept.Wait(TimeSpan.FromSeconds(5)));
                    peer = accept.Result;
                    object session = Field(connection, "m_WorkerSession");
                    Hook(connection, "m_SessionDisposalTestHook", () => { disposing.Set(); release.Wait(); });
                    // No explicit-stop intent: exercise actual unexpected completion first.
                    ((CancellationTokenSource)Field(session, "Cancellation")).Cancel();
                    Assert.IsTrue(disposing.Wait(TimeSpan.FromSeconds(5)));
                    connection.Disconnect();
                    Assert.IsTrue(connection.HasConnectionThread, "Disposal itself still owns the session.");
                    Assert.Throws<InvalidOperationException>(() => connection.ConnectionConfig = replacement);
                    release.Set();
                    Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                    Assert.DoesNotThrow(() => connection.ConnectionConfig = replacement,
                        "Explicit Disconnect racing final disposal must not be silently discarded.");
                }
                finally
                {
                    release.Set();
                    Hook(connection, "m_SessionDisposalTestHook", null);
                    connection.Disconnect();
                    Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                    peer?.Close();
                    listener.Stop();
                    UnityEngine.Object.DestroyImmediate(gameObject);
                    if (config != null) UnityEngine.Object.DestroyImmediate(config);
                    if (replacement != null) UnityEngine.Object.DestroyImmediate(replacement);
                }
            }
        }

        [Test]
        public void DelayedDisconnectCannotReleaseSessionBeforeStopPlanCompletes()
        {
            var gameObject = new GameObject("L4 stop owner");
            var listener = new TcpListener(IPAddress.Loopback, 0);
            ROSConnection connection = gameObject.AddComponent<ROSConnection>();
            connection.ConnectOnStart = false;
            connection.listenForTFMessages = false;
            ROSConnectionConfig first = null;
            ROSConnectionConfig second = null;
            TcpClient peer = null;
            Task stop = null;
            using (var captured = new ManualResetEventSlim(false))
            using (var release = new ManualResetEventSlim(false))
            using (var workerTerminal = new ManualResetEventSlim(false))
            {
                try
                {
                    listener.Start();
                    int port = ((IPEndPoint)listener.LocalEndpoint).Port;
                    first = Config(port);
                    second = Config(port);
                    connection.ConnectionConfig = first;
                    Task<TcpClient> accept = listener.AcceptTcpClientAsync();
                    connection.Connect();
                    Assert.IsTrue(accept.Wait(TimeSpan.FromSeconds(5)));
                    peer = accept.Result;
                    Hook(connection, "m_StopPlanCapturedTestHook", () => { captured.Set(); release.Wait(); });
                    Hook(connection, "m_WorkerBodyCompletedTestHook", () => workerTerminal.Set());
                    object session = Field(connection, "m_WorkerSession");
                    var cancellation = (CancellationTokenSource)Field(session, "Cancellation");
                    int disposals = 0;
                    Hook(connection, "m_SessionDisposalTestHook", () => Interlocked.Increment(ref disposals));
                    stop = Task.Run(() => connection.Disconnect());
                    Assert.IsTrue(captured.Wait(TimeSpan.FromSeconds(5)), "Stop must reach the post-revocation boundary.");
                    // Force transport termination independently of the paused stop owner.
                    peer.Close();
                    Assert.IsTrue(workerTerminal.Wait(TimeSpan.FromSeconds(5)), "Worker completion must reach its ownership finalizer.");
                    Assert.IsTrue(connection.HasConnectionThread,
                        "A terminal worker still belongs to its session while the captured stop plan is incomplete.");
                    Assert.AreEqual(0, disposals, "The pending cancellation owner pins CTS disposal.");
                    Assert.IsFalse(cancellation.Token.IsCancellationRequested);
                    connection.Disconnect(); // A repeated caller cannot issue a competing Cancel.
                    Assert.Throws<InvalidOperationException>(() => connection.ConnectionConfig = second);
                    connection.Connect();
                    Assert.IsFalse(listener.Pending(), "Connect while Stopping must not create a replacement client.");
                    release.Set();
                    Assert.IsTrue(stop.Wait(TimeSpan.FromSeconds(5)));
                    Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                    Assert.AreEqual(1, disposals);
                    Assert.Throws<ObjectDisposedException>(() => cancellation.Token.ThrowIfCancellationRequested());
                    connection.ConnectionConfig = second;
                    Assert.AreSame(second, connection.ConnectionConfig);
                }
                finally
                {
                    release.Set();
                    if (stop != null) Assert.IsTrue(stop.Wait(TimeSpan.FromSeconds(5)));
                    Hook(connection, "m_StopPlanCapturedTestHook", null);
                    Hook(connection, "m_WorkerBodyCompletedTestHook", null);
                    Hook(connection, "m_SessionDisposalTestHook", null);
                    connection.Disconnect();
                    Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                    peer?.Close();
                    listener.Stop();
                    UnityEngine.Object.DestroyImmediate(gameObject);
                    if (first != null) UnityEngine.Object.DestroyImmediate(first);
                    if (second != null) UnityEngine.Object.DestroyImmediate(second);
                }
            }
        }
    }
}
