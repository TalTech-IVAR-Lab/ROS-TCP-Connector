using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using RosMessageTypes.BuiltinInterfaces;
using RosMessageTypes.Std;
using Unity.Robotics.ROSTCPConnector;
using UnityEngine;
using UnityEngine.TestTools;

namespace UnitTests
{
    public class ConnectorRegistrationTests
    {
        const BindingFlags Private = BindingFlags.Instance | BindingFlags.NonPublic;

        static ROSConnectionConfig Config(int port)
        {
            var config = ScriptableObject.CreateInstance<ROSConnectionConfig>();
            typeof(ROSConnectionConfig).GetField("m_RosIPAddress", Private).SetValue(config, "127.0.0.1");
            typeof(ROSConnectionConfig).GetField("m_RosPort", Private).SetValue(config, port);
            return config;
        }

        static void ReadExactly(Stream stream, byte[] buffer, int offset, int length)
        {
            while (length > 0)
            {
                int read = stream.Read(buffer, offset, length);
                if (read == 0) throw new EndOfStreamException();
                offset += read;
                length -= read;
            }
        }

        static Tuple<string, byte[]> ReadFrame(Stream stream)
        {
            byte[] prefix = new byte[4];
            ReadExactly(stream, prefix, 0, prefix.Length);
            int topicLength = BitConverter.ToInt32(prefix, 0);
            byte[] topicBytes = new byte[topicLength];
            ReadExactly(stream, topicBytes, 0, topicBytes.Length);
            ReadExactly(stream, prefix, 0, prefix.Length);
            int payloadLength = BitConverter.ToInt32(prefix, 0);
            byte[] payload = new byte[payloadLength];
            ReadExactly(stream, payload, 0, payload.Length);
            return Tuple.Create(System.Text.Encoding.ASCII.GetString(topicBytes), payload);
        }

        [Test]
        public void OfflinePublisherRegistrationPrecedesExactlyOnePayload()
        {
            var gameObject = new GameObject("registration prefix");
            var listener = new TcpListener(IPAddress.Loopback, 0);
            ROSConnectionConfig config = null;
            TcpClient peer = null;
            try
            {
                listener.Start();
                config = Config(((IPEndPoint)listener.LocalEndpoint).Port);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                connection.listenForTFMessages = false;
                connection.ConnectionConfig = config;
                connection.RegisterPublisher<TimeMsg>("/registration-prefix", 1);
                var message = new TimeMsg(7, 11);
                connection.Publish("/registration-prefix", message);
                connection.QueueSysCommand("__registration-prefix-fence", new SysCommand_TopicsRequest());

                Task<TcpClient> accepted = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(accepted.Wait(TimeSpan.FromSeconds(5)));
                peer = accepted.Result;
                peer.GetStream().ReadTimeout = 5000;
                var frames = new List<Tuple<string, byte[]>>();
                for (int index = 0; index < 16; index++)
                {
                    Tuple<string, byte[]> frame = ReadFrame(peer.GetStream());
                    frames.Add(frame);
                    if (frame.Item1 == "__registration-prefix-fence") break;
                }

                Assert.AreEqual(1, frames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_Publish).Count);
                int registration = frames.FindIndex(frame => frame.Item1 == SysCommand.k_SysCommand_Publish);
                int payload = frames.FindIndex(frame => frame.Item1 == "/registration-prefix");
                int fence = frames.FindIndex(frame => frame.Item1 == "__registration-prefix-fence");
                Assert.GreaterOrEqual(registration, 0);
                Assert.Greater(payload, registration);
                Assert.Greater(fence, payload);
            }
            finally
            {
                try { if (gameObject != null) gameObject.GetComponent<ROSConnection>()?.Disconnect(); }
                catch { }
                peer?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
            }
        }
        [Test]
        public void AllConfiguredRolesRegisterExactlyOnceBeforeFirstData()
        {
            var gameObject = new GameObject("all role registration prefix");
            var listener = new TcpListener(IPAddress.Loopback, 0);
            ROSConnectionConfig config = null;
            TcpClient peer = null;
            try
            {
                listener.Start();
                config = Config(((IPEndPoint)listener.LocalEndpoint).Port);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                connection.listenForTFMessages = false;
                connection.ConnectionConfig = config;
                connection.Subscribe<TimeMsg>("/registered-subscriber", _ => { });
                connection.RegisterPublisher<TimeMsg>("/registered-publisher", 1, false);
                connection.RegisterRosService("/registered-ros-service", TimeMsg.k_RosMessageName,
                    TimeMsg.k_RosMessageName, 1);
                connection.ImplementService<TimeMsg, TimeMsg>("/registered-unity-service", _ => new TimeMsg(3, 5), 1);
                connection.ImplementService<TimeMsg, TimeMsg>("/registered-unity-service-async",
                    async _ =>
                    {
                        await Task.Yield();
                        return new TimeMsg(13, 17);
                    }, 1);
                connection.QueueSysCommand("__all-registration-fence", new SysCommand_TopicsRequest());

                Task<TcpClient> accepted = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(accepted.Wait(TimeSpan.FromSeconds(5)));
                peer = accepted.Result;
                peer.GetStream().ReadTimeout = 5000;
                var frames = new List<Tuple<string, byte[]>>();
                for (int index = 0; index < 32; index++)
                {
                    Tuple<string, byte[]> frame = ReadFrame(peer.GetStream());
                    frames.Add(frame);
                    if (frame.Item1 == "__all-registration-fence") break;
                }

                string[] registrationCommands =
                {
                    SysCommand.k_SysCommand_Subscribe,
                    SysCommand.k_SysCommand_Publish,
                    SysCommand.k_SysCommand_RosService
                };
                foreach (string command in registrationCommands)
                {
                    Assert.AreEqual(1, frames.FindAll(frame => frame.Item1 == command).Count,
                        "Each configured role must have one registration command: " + command);
                    Assert.Less(frames.FindIndex(frame => frame.Item1 == command),
                        frames.FindIndex(frame => frame.Item1 == "__all-registration-fence"));
                }
                Assert.AreEqual(2, frames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_UnityService).Count);
                Assert.Less(frames.FindIndex(frame => frame.Item1 == SysCommand.k_SysCommand_UnityService),
                    frames.FindIndex(frame => frame.Item1 == "__all-registration-fence"));

                connection.QueueSysCommand("__reconcile-fence", new SysCommand_TopicsRequest());
                object attempt = typeof(ROSConnection).GetField("m_CurrentAttempt", Private).GetValue(connection);
                MethodInfo reconcile = typeof(ROSConnection).GetMethod("ReconcileRegistrations", Private);
                reconcile.Invoke(connection, new[] { attempt });
                reconcile.Invoke(connection, new[] { attempt });
                var reconcileFrames = new List<Tuple<string, byte[]>>();
                for (int index = 0; index < 32; index++)
                {
                    Tuple<string, byte[]> frame = ReadFrame(peer.GetStream());
                    if (frame.Item1 != string.Empty)
                        reconcileFrames.Add(frame);
                    if (frame.Item1 == "__reconcile-fence") break;
                }
                Assert.AreEqual(0, reconcileFrames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_Subscribe).Count);
                Assert.AreEqual(0, reconcileFrames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_Publish).Count);
                Assert.AreEqual(0, reconcileFrames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_RosService).Count);
                Assert.AreEqual(0, reconcileFrames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_UnityService).Count);
            }
            finally
            {
                try { if (gameObject != null) gameObject.GetComponent<ROSConnection>()?.Disconnect(); }
                catch { }
                peer?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
            }
        }

        [Test]
        public void ServiceObserversDoNotRegisterTransportSubscriber()
        {
            var gameObject = new GameObject("service observer registration");
            var listener = new TcpListener(IPAddress.Loopback, 0);
            ROSConnectionConfig config = null;
            TcpClient peer = null;
            try
            {
                listener.Start();
                config = Config(((IPEndPoint)listener.LocalEndpoint).Port);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                connection.listenForTFMessages = false;
                connection.ConnectionConfig = config;
                connection.RegisterRosService<TimeMsg, TimeMsg>("/service-observer");
                RosTopicState service = connection.GetTopic("/service-observer");
                service.AddSubscriber(_ => { });
                service.ServiceResponseTopic.AddSubscriber(_ => { });
                connection.QueueSysCommand("__service-observer-fence", new SysCommand_TopicsRequest());

                Task<TcpClient> accepted = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(accepted.Wait(TimeSpan.FromSeconds(5)));
                peer = accepted.Result;
                peer.GetStream().ReadTimeout = 5000;
                var frames = new List<Tuple<string, byte[]>>();
                for (int index = 0; index < 32; index++)
                {
                    Tuple<string, byte[]> frame = ReadFrame(peer.GetStream());
                    frames.Add(frame);
                    if (frame.Item1 == "__service-observer-fence") break;
                }

                Assert.AreEqual(1, frames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_RosService).Count);
                Assert.AreEqual(0, frames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_Subscribe).Count,
                    "Service request/response observers must not create a transport subscriber.");
                Assert.AreEqual(0, frames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_RemoveSubscriber).Count);
            }
            finally
            {
                try { if (gameObject != null) gameObject.GetComponent<ROSConnection>()?.Disconnect(); }
                catch { }
                peer?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
            }
        }

        [Test]
        public void RegistrationDuringStartupReconcilesExactlyOnce()
        {
            var gameObject = new GameObject("startup registration window");
            var listener = new TcpListener(IPAddress.Loopback, 0);
            ROSConnectionConfig config = null;
            TcpClient peer = null;
            var startupEntered = new ManualResetEventSlim(false);
            var releaseStartup = new ManualResetEventSlim(false);
            try
            {
                listener.Start();
                config = Config(((IPEndPoint)listener.LocalEndpoint).Port);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                connection.listenForTFMessages = false;
                connection.ConnectionConfig = config;
                typeof(ROSConnection).GetField("m_BeforeStartupRegistrationsTestHook", Private).SetValue(
                    connection, new Action(() =>
                    {
                        startupEntered.Set();
                        releaseStartup.Wait(TimeSpan.FromSeconds(5));
                    }));

                Task<TcpClient> accepted = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(accepted.Wait(TimeSpan.FromSeconds(5)));
                peer = accepted.Result;
                peer.GetStream().ReadTimeout = 5000;
                Assert.IsTrue(startupEntered.Wait(TimeSpan.FromSeconds(5)));

                connection.Subscribe<TimeMsg>("/startup-window", _ => { });
                connection.QueueSysCommand("__startup-window-fence", new SysCommand_TopicsRequest());
                releaseStartup.Set();

                var frames = new List<Tuple<string, byte[]>>();
                for (int index = 0; index < 32; index++)
                {
                    Tuple<string, byte[]> frame = ReadFrame(peer.GetStream());
                    frames.Add(frame);
                    if (frame.Item1 == "__startup-window-fence") break;
                }

                Assert.AreEqual(1, frames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_Subscribe).Count,
                    "A registration admitted during establishment must be reconciled once.");
                Assert.Greater(frames.FindIndex(frame => frame.Item1 == SysCommand.k_SysCommand_Subscribe), -1);
                Assert.Greater(frames.FindIndex(frame => frame.Item1 == "__startup-window-fence"),
                    frames.FindIndex(frame => frame.Item1 == SysCommand.k_SysCommand_Subscribe));
            }
            finally
            {
                releaseStartup.Set();
                try { if (gameObject != null) gameObject.GetComponent<ROSConnection>()?.Disconnect(); }
                catch { }
                peer?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
            }
        }

        [Test]
        public void RegistrationWriteCompletionCannotPublishAfterAttemptRevocation()
        {
            var gameObject = new GameObject("registration write authority");
            var listener = new TcpListener(IPAddress.Loopback, 0);
            ROSConnectionConfig config = null;
            TcpClient firstPeer = null;
            TcpClient secondPeer = null;
            var writeCompleted = new ManualResetEventSlim(false);
            var releaseCompletion = new ManualResetEventSlim(false);
            FieldInfo afterWriteHook = null;
            try
            {
                listener.Start();
                config = Config(((IPEndPoint)listener.LocalEndpoint).Port);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                connection.listenForTFMessages = false;
                connection.ConnectionConfig = config;
                connection.Subscribe<TimeMsg>("/registration-authority", _ => { });
                afterWriteHook = typeof(ROSConnection).GetField("m_AfterImmediateCommandWriteTestHook", Private);
                Assert.NotNull(afterWriteHook,
                    "The registration writer needs an outside-the-lock completion barrier for the authority regression.");
                afterWriteHook.SetValue(connection, new Action(() =>
                {
                    writeCompleted.Set();
                    releaseCompletion.Wait(TimeSpan.FromSeconds(5));
                }));

                Task<TcpClient> firstAccepted = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(firstAccepted.Wait(TimeSpan.FromSeconds(5)));
                firstPeer = firstAccepted.Result;
                firstPeer.GetStream().ReadTimeout = 5000;
                Assert.IsTrue(writeCompleted.Wait(TimeSpan.FromSeconds(5)));
                Tuple<string, byte[]> firstFrame = ReadFrame(firstPeer.GetStream());
                while (firstFrame.Item1 == string.Empty)
                    firstFrame = ReadFrame(firstPeer.GetStream());
                Assert.AreEqual(SysCommand.k_SysCommand_Subscribe, firstFrame.Item1);

                connection.Disconnect();
                releaseCompletion.Set();
                Assert.IsTrue(SpinWait.SpinUntil(() => !connection.HasConnectionThread, TimeSpan.FromSeconds(5)));
                Assert.IsFalse(connection.GetTopic("/registration-authority").SentSubscriberRegistration,
                    "A completed write from a revoked attempt must not publish its sent state.");
                afterWriteHook.SetValue(connection, null);

                connection.QueueSysCommand("__registration-authority-fence", new SysCommand_TopicsRequest());
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
                    if (frame.Item1 == "__registration-authority-fence") break;
                }
                Assert.AreEqual(1, secondFrames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_Subscribe).Count,
                    "The new attempt must reconcile the durable desired registration exactly once.");
            }
            finally
            {
                releaseCompletion.Set();
                if (afterWriteHook != null && gameObject != null)
                    afterWriteHook.SetValue(gameObject.GetComponent<ROSConnection>(), null);
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
        public void SubscribeUnsubscribeResubscribeOrdersClaimedTransitions()
        {
            var gameObject = new GameObject("claimed subscriber transitions");
            var listener = new TcpListener(IPAddress.Loopback, 0);
            ROSConnectionConfig config = null;
            TcpClient peer = null;
            var firstWriteEntered = new ManualResetEventSlim(false);
            var releaseFirstWrite = new ManualResetEventSlim(false);
            int hookCalls = 0;
            try
            {
                listener.Start();
                config = Config(((IPEndPoint)listener.LocalEndpoint).Port);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                connection.listenForTFMessages = false;
                connection.ConnectionConfig = config;
                connection.Subscribe<TimeMsg>("/subscriber-transitions", _ => { });
                RosTopicState topic = connection.GetTopic("/subscriber-transitions");
                typeof(ROSConnection).GetField("m_BeforeImmediateCommandWriteTestHook", Private).SetValue(
                    connection, new Action(() =>
                    {
                        if (Interlocked.Increment(ref hookCalls) == 1)
                        {
                            firstWriteEntered.Set();
                            releaseFirstWrite.Wait(TimeSpan.FromSeconds(5));
                        }
                    }));
                connection.QueueSysCommand("__subscriber-transitions-fence", new SysCommand_TopicsRequest());

                Task<TcpClient> accepted = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(accepted.Wait(TimeSpan.FromSeconds(5)));
                peer = accepted.Result;
                peer.GetStream().ReadTimeout = 5000;
                Assert.IsTrue(firstWriteEntered.Wait(TimeSpan.FromSeconds(5)));

                topic.UnsubscribeAll();
                topic.AddSubscriber(_ => { });
                releaseFirstWrite.Set();

                var frames = new List<Tuple<string, byte[]>>();
                for (int index = 0; index < 32; index++)
                {
                    Tuple<string, byte[]> frame = ReadFrame(peer.GetStream());
                    if (frame.Item1 != string.Empty)
                        frames.Add(frame);
                    if (frame.Item1 == "__subscriber-transitions-fence") break;
                }

                int firstSubscribe = frames.FindIndex(frame => frame.Item1 == SysCommand.k_SysCommand_Subscribe);
                int remove = frames.FindIndex(frame => frame.Item1 == SysCommand.k_SysCommand_RemoveSubscriber);
                int secondSubscribe = frames.FindIndex(firstSubscribe + 1,
                    frame => frame.Item1 == SysCommand.k_SysCommand_Subscribe);
                int fence = frames.FindIndex(frame => frame.Item1 == "__subscriber-transitions-fence");
                Assert.AreEqual(2, frames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_Subscribe).Count);
                Assert.AreEqual(1, frames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_RemoveSubscriber).Count);
                Assert.GreaterOrEqual(firstSubscribe, 0);
                Assert.Greater(remove, firstSubscribe);
                Assert.Greater(secondSubscribe, remove);
                Assert.Greater(fence, secondSubscribe);
                Assert.IsTrue(topic.SentSubscriberRegistration,
                    "The final replacement must be reported sent only after its wire completion.");
            }
            finally
            {
                releaseFirstWrite.Set();
                try { if (gameObject != null) gameObject.GetComponent<ROSConnection>()?.Disconnect(); }
                catch { }
                peer?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
            }
        }

        [Test]
        public void OfflineSubscriberTransitionsCoalesceBeforeConnection()
        {
            var gameObject = new GameObject("offline subscriber transitions");
            var listener = new TcpListener(IPAddress.Loopback, 0);
            ROSConnectionConfig config = null;
            TcpClient peer = null;
            try
            {
                listener.Start();
                config = Config(((IPEndPoint)listener.LocalEndpoint).Port);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                connection.listenForTFMessages = false;
                connection.ConnectionConfig = config;
                connection.Subscribe<TimeMsg>("/offline-transitions", _ => { });
                RosTopicState topic = connection.GetTopic("/offline-transitions");
                topic.UnsubscribeAll();
                topic.AddSubscriber(_ => { });
                connection.QueueSysCommand("__offline-transitions-fence", new SysCommand_TopicsRequest());

                Task<TcpClient> accepted = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(accepted.Wait(TimeSpan.FromSeconds(5)));
                peer = accepted.Result;
                peer.GetStream().ReadTimeout = 5000;
                var frames = new List<Tuple<string, byte[]>>();
                for (int index = 0; index < 32; index++)
                {
                    Tuple<string, byte[]> frame = ReadFrame(peer.GetStream());
                    if (frame.Item1 != string.Empty)
                        frames.Add(frame);
                    if (frame.Item1 == "__offline-transitions-fence") break;
                }

                Assert.AreEqual(1, frames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_Subscribe).Count);
                Assert.AreEqual(0, frames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_RemoveSubscriber).Count,
                    "Offline obsolete registrations must coalesce before any wire transition is claimed.");
                Assert.IsTrue(topic.SentSubscriberRegistration);
            }
            finally
            {
                try { if (gameObject != null) gameObject.GetComponent<ROSConnection>()?.Disconnect(); }
                catch { }
                peer?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
            }
        }

        [Test]
        public void ConcurrentTopicCreatorsPublishOneCompleteCreation()
        {
            var gameObject = new GameObject("concurrent topic creators");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                int throwingListenerCalls = 0;
                int laterListenerCalls = 0;
                connection.ListenForTopics(state =>
                {
                    if (state.Topic == "/concurrent-creation")
                    {
                        Interlocked.Increment(ref throwingListenerCalls);
                        throw new InvalidOperationException("concurrent listener failure");
                    }
                });
                connection.ListenForTopics(state =>
                {
                    if (state.Topic == "/concurrent-creation")
                    {
                        Assert.IsTrue(state.IsService);
                        Assert.IsNotNull(state.ServiceResponseTopic);
                        Interlocked.Increment(ref laterListenerCalls);
                    }
                });
                LogAssert.Expect(LogType.Exception, new Regex("concurrent listener failure"));
                var release = new ManualResetEventSlim(false);
                Task<RosTopicState> first = Task.Run(() =>
                {
                    release.Wait();
                    return connection.GetOrCreateTopic("/concurrent-creation", TimeMsg.k_RosMessageName, true);
                });
                Task<RosTopicState> second = Task.Run(() =>
                {
                    release.Wait();
                    return connection.GetOrCreateTopic("/concurrent-creation", TimeMsg.k_RosMessageName, true);
                });
                release.Set();
                Assert.IsTrue(Task.WaitAll(new Task[] { first, second }, TimeSpan.FromSeconds(5)));
                Assert.AreSame(first.Result, second.Result);
                Assert.AreSame(first.Result, connection.GetTopic("/concurrent-creation"));
                Assert.AreEqual(1, throwingListenerCalls);
                Assert.AreEqual(1, laterListenerCalls);
                Assert.IsTrue(first.Result.IsService);
                Assert.IsNotNull(first.Result.ServiceResponseTopic);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void ConcurrentConflictingPublisherCreationRejectsLoser()
        {
            var gameObject = new GameObject("concurrent conflicting publisher creators");
            ROSConnection connection = null;
            FieldInfo publicationHook = null;
            using (var candidateBarrier = new Barrier(2))
            {
                try
                {
                    connection = gameObject.AddComponent<ROSConnection>();
                    publicationHook = typeof(ROSConnection).GetField(
                        "m_BeforeConfiguredTopicPublicationTestHook", Private);
                    Assert.NotNull(publicationHook,
                        "The concurrent publication regression needs a pre-publication barrier seam.");
                    publicationHook.SetValue(connection, new Action(() =>
                    {
                        if (!candidateBarrier.SignalAndWait(TimeSpan.FromSeconds(5)))
                            throw new TimeoutException("Concurrent publisher candidates did not meet at the publication barrier.");
                    }));

                    Task<RosTopicState> first = Task.Run(() =>
                        connection.RegisterPublisher<TimeMsg>("/concurrent-conflicting-publisher", 1, false));
                    Task<RosTopicState> second = Task.Run(() =>
                        connection.RegisterPublisher<UInt8MultiArrayMsg>(
                            "/concurrent-conflicting-publisher", 1, false));

                    Assert.IsTrue(SpinWait.SpinUntil(
                        () => first.IsCompleted && second.IsCompleted, TimeSpan.FromSeconds(5)),
                        "Both concurrent publisher candidates must complete.");
                    bool firstSucceeded = first.Status == TaskStatus.RanToCompletion;
                    bool secondSucceeded = second.Status == TaskStatus.RanToCompletion;
                    Assert.AreNotEqual(firstSucceeded, secondSucceeded,
                        "Exactly one conflicting publisher creator must lose with a schema conflict.");
                    Task<RosTopicState> loser = firstSucceeded ? second : first;
                    Assert.IsTrue(loser.IsFaulted);
                    Assert.IsInstanceOf<ArgumentException>(loser.Exception.InnerException,
                        "The losing concurrent publisher must report the same schema conflict as the sequential path.");

                    RosTopicState published = connection.GetTopic("/concurrent-conflicting-publisher");
                    Assert.IsNotNull(published);
                    Assert.IsTrue(published.IsPublisher);
                    Assert.AreEqual(firstSucceeded ? TimeMsg.k_RosMessageName : UInt8MultiArrayMsg.k_RosMessageName,
                        published.RosMessageName);
                }
                finally
                {
                    if (publicationHook != null && connection != null)
                        publicationHook.SetValue(connection, null);
                    try { connection?.Disconnect(); }
                    catch { }
                    UnityEngine.Object.DestroyImmediate(gameObject);
                }
            }
        }

        [Test]
        public void ConcurrentConflictingSubscriberCreationRejectsLoser()
        {
            var gameObject = new GameObject("concurrent conflicting subscriber creators");
            ROSConnection connection = null;
            FieldInfo publicationHook = null;
            using (var candidateBarrier = new Barrier(2))
            {
                try
                {
                    connection = gameObject.AddComponent<ROSConnection>();
                    publicationHook = typeof(ROSConnection).GetField(
                        "m_BeforeConfiguredTopicPublicationTestHook", Private);
                    Assert.NotNull(publicationHook,
                        "The concurrent publication regression needs a pre-publication barrier seam.");
                    publicationHook.SetValue(connection, new Action(() =>
                    {
                        if (!candidateBarrier.SignalAndWait(TimeSpan.FromSeconds(5)))
                            throw new TimeoutException("Concurrent subscriber candidates did not meet at the publication barrier.");
                    }));

                    Task first = Task.Run(() => connection.SubscribeByMessageName(
                        "/concurrent-conflicting-subscriber", TimeMsg.k_RosMessageName, _ => { }));
                    Task second = Task.Run(() => connection.SubscribeByMessageName(
                        "/concurrent-conflicting-subscriber", UInt8MultiArrayMsg.k_RosMessageName, _ => { }));

                    Assert.IsTrue(SpinWait.SpinUntil(
                        () => first.IsCompleted && second.IsCompleted, TimeSpan.FromSeconds(5)),
                        "Both concurrent subscriber candidates must complete.");
                    bool firstSucceeded = first.Status == TaskStatus.RanToCompletion;
                    bool secondSucceeded = second.Status == TaskStatus.RanToCompletion;
                    Assert.AreNotEqual(firstSucceeded, secondSucceeded,
                        "Exactly one conflicting subscriber creator must lose with a schema conflict.");
                    Task loser = firstSucceeded ? second : first;
                    Assert.IsTrue(loser.IsFaulted);
                    Assert.IsInstanceOf<ArgumentException>(loser.Exception.InnerException,
                        "The losing concurrent subscriber must report the same schema conflict as the sequential path.");

                    RosTopicState published = connection.GetTopic("/concurrent-conflicting-subscriber");
                    Assert.IsNotNull(published);
                    Assert.IsTrue(published.HasSubscriberCallback);
                    Assert.AreEqual(firstSucceeded ? TimeMsg.k_RosMessageName : UInt8MultiArrayMsg.k_RosMessageName,
                        published.RosMessageName);
                }
                finally
                {
                    if (publicationHook != null && connection != null)
                        publicationHook.SetValue(connection, null);
                    try { connection?.Disconnect(); }
                    catch { }
                    UnityEngine.Object.DestroyImmediate(gameObject);
                }
            }
        }

        [Test]
        public void ConcurrentConflictingUnityServiceCreationRejectsLoser()
        {
            var gameObject = new GameObject("concurrent conflicting Unity service creators");
            ROSConnection connection = null;
            FieldInfo publicationHook = null;
            using (var candidateBarrier = new Barrier(2))
            {
                try
                {
                    connection = gameObject.AddComponent<ROSConnection>();
                    publicationHook = typeof(ROSConnection).GetField(
                        "m_BeforeConfiguredTopicPublicationTestHook", Private);
                    Assert.NotNull(publicationHook,
                        "The concurrent publication regression needs a pre-publication barrier seam.");
                    publicationHook.SetValue(connection, new Action(() =>
                    {
                        if (!candidateBarrier.SignalAndWait(TimeSpan.FromSeconds(5)))
                            throw new TimeoutException("Concurrent Unity service candidates did not meet the publication barrier.");
                    }));

                    Task first = Task.Run(() => connection.ImplementService<TimeMsg, TimeMsg>(
                        "/concurrent-conflicting-unity-service", _ => new TimeMsg(), 1));
                    Task second = Task.Run(() => connection.ImplementService<UInt8MultiArrayMsg, TimeMsg>(
                        "/concurrent-conflicting-unity-service", _ => new TimeMsg(), 1));

                    Assert.IsTrue(SpinWait.SpinUntil(
                        () => first.IsCompleted && second.IsCompleted, TimeSpan.FromSeconds(5)),
                        "Both concurrent Unity service candidates must complete.");
                    bool firstSucceeded = first.Status == TaskStatus.RanToCompletion;
                    bool secondSucceeded = second.Status == TaskStatus.RanToCompletion;
                    Assert.AreNotEqual(firstSucceeded, secondSucceeded,
                        "Exactly one conflicting Unity service creator must lose with a schema conflict.");
                    Task loser = firstSucceeded ? second : first;
                    Assert.IsTrue(loser.IsFaulted);
                    Assert.IsInstanceOf<ArgumentException>(loser.Exception.InnerException,
                        "The losing concurrent Unity service must report the same schema conflict as the sequential path.");

                    RosTopicState published = connection.GetTopic("/concurrent-conflicting-unity-service");
                    Assert.IsNotNull(published);
                    Assert.IsTrue(published.IsUnityService);
                    Assert.AreEqual(firstSucceeded ? TimeMsg.k_RosMessageName : UInt8MultiArrayMsg.k_RosMessageName,
                        published.RosMessageName);
                }
                finally
                {
                    if (publicationHook != null && connection != null)
                        publicationHook.SetValue(connection, null);
                    try { connection?.Disconnect(); }
                    catch { }
                    UnityEngine.Object.DestroyImmediate(gameObject);
                }
            }
        }

        [TestCase("unity")]
        [TestCase("ros")]
        public void CrossRoleReplacementRemovesSubscriberBeforeServiceRegistration(string replacement)
        {
            var gameObject = new GameObject("cross-role registration replacement");
            var listener = new TcpListener(IPAddress.Loopback, 0);
            ROSConnectionConfig config = null;
            TcpClient peer = null;
            ROSConnection connection = null;
            FieldInfo beforeWriteHook = null;
            var firstWriteEntered = new ManualResetEventSlim(false);
            var releaseFirstWrite = new ManualResetEventSlim(false);
            int hookCalls = 0;
            try
            {
                listener.Start();
                config = Config(((IPEndPoint)listener.LocalEndpoint).Port);
                connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                connection.listenForTFMessages = false;
                connection.ConnectionConfig = config;
                connection.Subscribe<TimeMsg>("/cross-role-replacement", _ => { });
                connection.QueueSysCommand("__cross-role-initial-fence", new SysCommand_TopicsRequest());

                Task<TcpClient> accepted = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(accepted.Wait(TimeSpan.FromSeconds(5)));
                peer = accepted.Result;
                peer.GetStream().ReadTimeout = 5000;
                var initialFrames = new List<Tuple<string, byte[]>>();
                for (int index = 0; index < 16; index++)
                {
                    Tuple<string, byte[]> frame = ReadFrame(peer.GetStream());
                    initialFrames.Add(frame);
                    if (frame.Item1 == "__cross-role-initial-fence") break;
                }
                Assert.AreEqual(1, initialFrames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_Subscribe).Count);

                beforeWriteHook = typeof(ROSConnection).GetField("m_BeforeImmediateCommandWriteTestHook", Private);
                Assert.NotNull(beforeWriteHook,
                    "The registration writer needs an outside-the-lock barrier for the cross-role order regression.");
                beforeWriteHook.SetValue(connection, new Action(() =>
                {
                    if (Interlocked.Increment(ref hookCalls) == 1)
                    {
                        firstWriteEntered.Set();
                        if (!releaseFirstWrite.Wait(TimeSpan.FromSeconds(5)))
                            throw new TimeoutException("The blocking registration did not reach the write barrier.");
                    }
                }));
                connection.RegisterPublisher<TimeMsg>("/cross-role-order-barrier", 1);
                Assert.IsTrue(firstWriteEntered.Wait(TimeSpan.FromSeconds(5)),
                    "The writer must claim a registration before the replacement transition is admitted.");

                if (replacement == "unity")
                    connection.ImplementService<TimeMsg, TimeMsg>(
                        "/cross-role-replacement", _ => new TimeMsg(), 1);
                else
                    connection.RegisterRosService<TimeMsg, TimeMsg>("/cross-role-replacement");
                connection.QueueSysCommand("__cross-role-replacement-fence", new SysCommand_TopicsRequest());
                releaseFirstWrite.Set();

                var replacementFrames = new List<Tuple<string, byte[]>>();
                for (int index = 0; index < 16; index++)
                {
                    Tuple<string, byte[]> frame = ReadFrame(peer.GetStream());
                    replacementFrames.Add(frame);
                    if (frame.Item1 == "__cross-role-replacement-fence") break;
                }

                string serviceRegistration = replacement == "unity"
                    ? SysCommand.k_SysCommand_UnityService : SysCommand.k_SysCommand_RosService;
                int remove = replacementFrames.FindIndex(
                    frame => frame.Item1 == SysCommand.k_SysCommand_RemoveSubscriber);
                int service = replacementFrames.FindIndex(frame => frame.Item1 == serviceRegistration);
                Assert.AreEqual(1, replacementFrames.FindAll(
                    frame => frame.Item1 == SysCommand.k_SysCommand_RemoveSubscriber).Count);
                Assert.AreEqual(1, replacementFrames.FindAll(frame => frame.Item1 == serviceRegistration).Count);
                Assert.GreaterOrEqual(remove, 0);
                Assert.GreaterOrEqual(service, 0);
                Assert.Less(remove, service,
                    "A cross-role replacement must remove the old subscriber before registering the service role.");
            }
            finally
            {
                releaseFirstWrite.Set();
                if (beforeWriteHook != null && connection != null)
                    beforeWriteHook.SetValue(connection, null);
                try { connection?.Disconnect(); }
                catch { }
                peer?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
                releaseFirstWrite.Dispose();
                firstWriteEntered.Dispose();
            }
        }

        [TestCase("unity")]
        [TestCase("ros")]
        public void CrossRoleReplacementWhileSubscriberRegistrationIsWritingOrdersRemovalBeforeReplacement(string replacement)
        {
            var gameObject = new GameObject("cross-role writing replacement");
            var listener = new TcpListener(IPAddress.Loopback, 0);
            ROSConnectionConfig config = null;
            TcpClient peer = null;
            ROSConnection connection = null;
            FieldInfo beforeWriteHook = null;
            var firstWriteEntered = new ManualResetEventSlim(false);
            var releaseFirstWrite = new ManualResetEventSlim(false);
            int hookCalls = 0;
            try
            {
                listener.Start();
                config = Config(((IPEndPoint)listener.LocalEndpoint).Port);
                connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                connection.listenForTFMessages = false;
                connection.ConnectionConfig = config;
                connection.Subscribe<TimeMsg>("/cross-role-writing", _ => { });

                beforeWriteHook = typeof(ROSConnection).GetField("m_BeforeImmediateCommandWriteTestHook", Private);
                Assert.NotNull(beforeWriteHook,
                    "The registration writer needs an outside-the-lock barrier for the writing replacement regression.");
                beforeWriteHook.SetValue(connection, new Action(() =>
                {
                    if (Interlocked.Increment(ref hookCalls) == 1)
                    {
                        firstWriteEntered.Set();
                        if (!releaseFirstWrite.Wait(TimeSpan.FromSeconds(5)))
                            throw new TimeoutException("The initial subscriber registration did not reach the write barrier.");
                    }
                }));

                Task<TcpClient> accepted = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(accepted.Wait(TimeSpan.FromSeconds(5)));
                peer = accepted.Result;
                peer.GetStream().ReadTimeout = 5000;
                Assert.IsTrue(firstWriteEntered.Wait(TimeSpan.FromSeconds(5)),
                    "The old subscriber registration must be in Writing before the role replacement is selected.");

                if (replacement == "unity")
                    connection.ImplementService<TimeMsg, TimeMsg>(
                        "/cross-role-writing", _ => new TimeMsg(), 1);
                else
                    connection.RegisterRosService<TimeMsg, TimeMsg>("/cross-role-writing");
                connection.QueueSysCommand("__cross-role-writing-fence", new SysCommand_TopicsRequest());
                releaseFirstWrite.Set();

                var frames = new List<Tuple<string, byte[]>>();
                for (int index = 0; index < 16; index++)
                {
                    Tuple<string, byte[]> frame = ReadFrame(peer.GetStream());
                    frames.Add(frame);
                    if (frame.Item1 == "__cross-role-writing-fence") break;
                }

                string serviceRegistration = replacement == "unity"
                    ? SysCommand.k_SysCommand_UnityService : SysCommand.k_SysCommand_RosService;
                int remove = frames.FindIndex(frame => frame.Item1 == SysCommand.k_SysCommand_RemoveSubscriber);
                int service = frames.FindIndex(frame => frame.Item1 == serviceRegistration);
                Assert.AreEqual(1, frames.FindAll(
                    frame => frame.Item1 == SysCommand.k_SysCommand_RemoveSubscriber).Count);
                Assert.AreEqual(1, frames.FindAll(frame => frame.Item1 == serviceRegistration).Count);
                Assert.GreaterOrEqual(remove, 0,
                    "A written subscriber role must be followed by its removal during cross-role replacement.");
                Assert.GreaterOrEqual(service, 0);
                Assert.Less(remove, service,
                    "A cross-role replacement must remove a writing subscriber before registering the service role.");
            }
            finally
            {
                releaseFirstWrite.Set();
                if (beforeWriteHook != null && connection != null)
                    beforeWriteHook.SetValue(connection, null);
                try { connection?.Disconnect(); }
                catch { }
                peer?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
                releaseFirstWrite.Dispose();
                firstWriteEntered.Dispose();
            }
        }

        [Test]
        public void UnityServiceReplacementSelectsOnlyTheNewImplementation()
        {
            var gameObject = new GameObject("unity service replacement");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ImplementService<TimeMsg, TimeMsg>(
                    "/service-replacement", _ => new TimeMsg(1, 2), 1);
                RosTopicState topic = connection.GetTopic("/service-replacement");
                connection.ImplementService<TimeMsg, UInt8MultiArrayMsg>(
                    "/service-replacement",
                    async _ =>
                    {
                        await Task.Yield();
                        return new UInt8MultiArrayMsg();
                    }, 1);

                Assert.IsTrue(topic.IsUnityService);
                Assert.AreEqual(UInt8MultiArrayMsg.k_RosMessageName, topic.ServiceResponseTopic.RosMessageName);
                Assert.IsNull(typeof(RosTopicState).GetField("m_ServiceImplementation", Private).GetValue(topic));
                Assert.IsNotNull(typeof(RosTopicState).GetField("m_ServiceImplementationAsync", Private).GetValue(topic));
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void PublisherCreationListenerSeesCompleteConfiguredState()
        {
            var gameObject = new GameObject("atomic publisher catalog");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                RosTopicState observed = null;
                connection.ListenForTopics(state =>
                {
                    observed = state;
                    Assert.IsTrue(state.IsPublisher, "Creation must publish the publisher role atomically.");
                    Assert.IsTrue(state.IsPublisherLatched);
                    Assert.AreEqual(3, state.MessageSender.QueueSize);
                    Assert.AreEqual(TimeMsg.k_RosMessageName, state.RosMessageName);
                });

                RosTopicState returned = connection.RegisterPublisher<TimeMsg>("/atomic-publisher", 3, true);

                Assert.AreSame(returned, observed);
                Assert.IsTrue(returned.IsPublisher);
                Assert.IsNotNull(returned.MessageSender);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void InvalidPublisherConfigurationDoesNotPublishGhostTopic()
        {
            var gameObject = new GameObject("invalid publisher catalog");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                int notifications = 0;
                connection.ListenForTopics(_ => notifications++);

                Assert.Throws<Exception>(() => connection.RegisterPublisher<TimeMsg>("/invalid-publisher", 0, false));

                Assert.IsNull(connection.GetTopic("/invalid-publisher"));
                Assert.AreEqual(0, notifications);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void AdditionalSubscriberCallbackKeepsStableRegistrationRevision()
        {
            var gameObject = new GameObject("stable subscriber registration");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.Subscribe<TimeMsg>("/stable-subscriber", _ => { });
                RosTopicState topic = connection.GetTopic("/stable-subscriber");
                long firstRevision = DesiredRevision(connection, topic);

                topic.AddSubscriber(_ => { });

                Assert.AreEqual(firstRevision, DesiredRevision(connection, topic),
                    "Adding an observer must not create a new registration revision for the same role.");
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void EquivalentServiceIntentKeepsStableRegistrationRevision()
        {
            var gameObject = new GameObject("stable service registration");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ImplementService<TimeMsg, TimeMsg>("/stable-service", _ => new TimeMsg(), 1);
                RosTopicState topic = connection.GetTopic("/stable-service");
                long firstRevision = DesiredRevision(connection, topic);

                connection.ImplementService<TimeMsg, TimeMsg>("/stable-service", _ => new TimeMsg(), 1);

                Assert.AreEqual(firstRevision, DesiredRevision(connection, topic),
                    "Reapplying an equivalent service intent must not create a new registration revision.");
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void ServiceResponseSchemaChangeCreatesNewDesiredRevision()
        {
            var gameObject = new GameObject("service response schema revision");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ImplementService<TimeMsg, TimeMsg>("/service-response-revision", _ => new TimeMsg(), 1);
                RosTopicState topic = connection.GetTopic("/service-response-revision");
                long firstRevision = DesiredRevision(connection, topic);

                connection.ImplementService<TimeMsg, UInt8MultiArrayMsg>(
                    "/service-response-revision", _ => new UInt8MultiArrayMsg(), 1);

                Assert.AreNotEqual(firstRevision, DesiredRevision(connection, topic),
                    "Changing a service response schema must replace the desired registration revision.");
                Assert.AreEqual(UInt8MultiArrayMsg.k_RosMessageName,
                    topic.ServiceResponseTopic.RosMessageName);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void LiveQueuedRemoveIsCancelledWhenWrittenDefinitionIsRestored()
        {
            var gameObject = new GameObject("queued remove restoration");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                connection.listenForTFMessages = false;
                connection.Subscribe<TimeMsg>("/queued-remove-restoration", _ => { });
                RosTopicState topic = connection.GetTopic("/queued-remove-restoration");
                AttemptTestAccess.Replace(connection, 701L);
                object attempt = AttemptTestAccess.Current(connection);
                FieldInfo attemptState = attempt.GetType().GetField("State",
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                attemptState.SetValue(attempt, Enum.Parse(attemptState.FieldType, "Connecting"));
                MethodInfo reconcile = typeof(ROSConnection).GetMethod("ReconcileRegistrations", Private);
                reconcile.Invoke(connection, new[] { attempt });

                object binding = null;
                foreach (object candidate in (IEnumerable)attempt.GetType().GetField(
                    "Registrations", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic).GetValue(attempt))
                {
                    if (ReferenceEquals(candidate.GetType().GetField("Topic",
                        BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic).GetValue(candidate), topic))
                    {
                        binding = candidate;
                        break;
                    }
                }
                Assert.NotNull(binding, "The live attempt must own the subscriber registration binding.");
                FieldInfo activeField = binding.GetType().GetField("Active",
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                FieldInfo writtenField = binding.GetType().GetField("Written",
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                object active = activeField.GetValue(binding);
                Assert.NotNull(active, "The live attempt must queue the initial registration.");
                MethodInfo begin = typeof(ROSConnection).GetMethod("TryBeginRegistrationWrite", Private);
                MethodInfo complete = typeof(ROSConnection).GetMethod("CompleteRegistrationWrite", Private);
                object outgoing = attempt.GetType().GetField("Outgoing",
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic).GetValue(attempt);
                MethodInfo removeEntry = outgoing.GetType().GetMethod("RemoveRegistration",
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                Assert.IsTrue((bool)removeEntry.Invoke(outgoing, new[] { active }));
                Assert.IsTrue((bool)begin.Invoke(connection, new[] { active, attempt }));
                complete.Invoke(connection, new[] { active, attempt, (object)true });
                object written = writtenField.GetValue(binding);
                Assert.NotNull(written, "The initial registration must become the written definition.");
                Assert.IsTrue(topic.SentSubscriberRegistration);

                MethodInfo remove = typeof(ROSConnection).GetMethod("RemoveDesiredRegistrationLocked", Private);
                MethodInfo setDesired = typeof(ROSConnection).GetMethod("SetDesiredRegistrationLocked", Private);
                Type roleType = typeof(ROSConnection).GetNestedType("RegistrationRole", BindingFlags.NonPublic);
                object subscriberRole = Enum.Parse(roleType, "Subscriber");
                object gate = typeof(ROSConnection).GetField("m_ConnectionLifecycleLock", Private).GetValue(connection);
                lock (gate)
                {
                    remove.Invoke(connection, new[] { topic, subscriberRole, attempt });
                    Assert.NotNull(activeField.GetValue(binding),
                        "Removing the desired role must queue a live remove unit before it is claimed.");
                    setDesired.Invoke(connection, new[] { written, attempt, (object)true });
                }

                Assert.IsNull(activeField.GetValue(binding),
                    "Restoring the already-written definition must cancel the queued remove unit.");
                Assert.IsTrue(topic.SentSubscriberRegistration,
                    "The restored desired definition must remain registered without a remove/register gap.");
                FieldInfo entriesField = outgoing.GetType().GetField("m_Entries", Private);
                int registrationEntries = 0;
                foreach (object entry in (IEnumerable)entriesField.GetValue(outgoing))
                {
                    object kind = entry.GetType().GetField("Kind",
                        BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic).GetValue(entry);
                    if (string.Equals(kind.ToString(), "Registration", StringComparison.Ordinal))
                        registrationEntries++;
                }
                Assert.AreEqual(0, registrationEntries,
                    "Coalescing the restored definition must not leave a stale registration command queued.");
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void ConflictingPublisherSchemaDoesNotMutateExistingTopic()
        {
            var gameObject = new GameObject("conflicting publisher schema");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                RosTopicState topic = connection.RegisterPublisher<TimeMsg>("/conflicting-publisher", 1, false);

                Assert.Throws<ArgumentException>(() => connection.RegisterPublisher(
                    "/conflicting-publisher", "other_msgs/Type", 1, false));

                Assert.AreEqual(TimeMsg.k_RosMessageName, topic.RosMessageName);
                Assert.IsTrue(topic.IsPublisher);
                Assert.IsNotNull(topic.MessageSender);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void ConflictingSubscriberSchemaDoesNotMutateExistingTopic()
        {
            var gameObject = new GameObject("conflicting subscriber schema");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                RosTopicState topic = connection.RegisterPublisher<TimeMsg>("/conflicting-subscriber", 1, false);
                int desiredRegistrations = DesiredRegistrationCount(connection);
                int callbacks = 0;

                Assert.Throws<ArgumentException>(() => connection.SubscribeByMessageName(
                    "/conflicting-subscriber", UInt8MultiArrayMsg.k_RosMessageName, _ => callbacks++));

                Assert.AreEqual(TimeMsg.k_RosMessageName, topic.RosMessageName);
                Assert.IsFalse(topic.HasSubscriberCallback,
                    "A rejected subscriber schema must not mutate existing callbacks.");
                Assert.AreEqual(desiredRegistrations, DesiredRegistrationCount(connection),
                    "A rejected subscriber schema must not add desired registration metadata.");
                Assert.AreEqual(0, callbacks);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void ConflictingGenericSubscriberSchemaDoesNotMutateExistingTopic()
        {
            var gameObject = new GameObject("conflicting generic subscriber schema");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                RosTopicState topic = connection.RegisterPublisher<TimeMsg>("/conflicting-generic-subscriber", 1, false);

                Assert.Throws<ArgumentException>(() => connection.Subscribe<UInt8MultiArrayMsg>(
                    "/conflicting-generic-subscriber", _ => { }));

                Assert.AreEqual(TimeMsg.k_RosMessageName, topic.RosMessageName);
                Assert.IsFalse(topic.HasSubscriberCallback,
                    "A rejected generic subscriber schema must not mutate existing callbacks.");
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void ThrowingTopicListenerDoesNotPreventLaterListeners()
        {
            var gameObject = new GameObject("topic listener failure");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ListenForTopics(_ => throw new InvalidOperationException("listener failure"));
                int laterNotifications = 0;
                connection.ListenForTopics(_ => laterNotifications++);
                LogAssert.Expect(LogType.Exception, new Regex("listener failure"));

                Assert.DoesNotThrow(() => connection.GetOrCreateTopic("/listener-failure", TimeMsg.k_RosMessageName));
                Assert.AreEqual(1, laterNotifications);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void EveryConfiguredRolePublishesACompleteCatalogState()
        {
            var gameObject = new GameObject("complete catalog roles");
            var observed = new Dictionary<string, RosTopicState>();
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ListenForTopics(state => observed[state.Topic] = state);

                connection.Subscribe<TimeMsg>("/catalog-subscriber", _ => { });
                RosTopicState publisher = connection.RegisterPublisher<TimeMsg>("/catalog-publisher", 4, true);
                connection.RegisterRosService<TimeMsg, UInt8MultiArrayMsg>("/catalog-ros-service");
                connection.ImplementService<TimeMsg, UInt8MultiArrayMsg>(
                    "/catalog-unity-service", _ => new UInt8MultiArrayMsg(), 2);

                Assert.IsTrue(observed["/catalog-subscriber"].HasSubscriberCallback);
                Assert.IsFalse(observed["/catalog-subscriber"].IsPublisher);
                Assert.AreSame(connection.GetTopic("/catalog-subscriber"), observed["/catalog-subscriber"]);

                Assert.IsTrue(observed["/catalog-publisher"].IsPublisher);
                Assert.IsTrue(observed["/catalog-publisher"].IsPublisherLatched);
                Assert.AreEqual(4, observed["/catalog-publisher"].MessageSender.QueueSize);
                Assert.AreSame(publisher, observed["/catalog-publisher"]);

                RosTopicState rosService = observed["/catalog-ros-service"];
                Assert.IsTrue(rosService.IsRosService);
                Assert.IsNotNull(rosService.ServiceResponseTopic);
                Assert.AreEqual(UInt8MultiArrayMsg.k_RosMessageName,
                    rosService.ServiceResponseTopic.RosMessageName);

                RosTopicState unityService = observed["/catalog-unity-service"];
                Assert.IsTrue(unityService.IsUnityService);
                Assert.IsNotNull(unityService.ServiceResponseTopic);
                Assert.AreEqual(UInt8MultiArrayMsg.k_RosMessageName,
                    unityService.ServiceResponseTopic.RosMessageName);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void ActivePublisherCannotBeConvertedToRosService()
        {
            var gameObject = new GameObject("conflicting service role");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                RosTopicState topic = connection.RegisterPublisher<TimeMsg>("/conflicting-service-role", 1, false);

                Assert.Throws<InvalidOperationException>(() => connection.RegisterRosService<TimeMsg, TimeMsg>(
                    "/conflicting-service-role"));

                Assert.IsTrue(topic.IsPublisher);
                Assert.IsFalse(topic.IsRosService);
                Assert.IsNull(topic.ServiceResponseTopic);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        static int DesiredRegistrationCount(ROSConnection connection)
        {
            object desired = typeof(ROSConnection).GetField("m_DesiredRegistrations", Private).GetValue(connection);
            int count = 0;
            foreach (object _ in (IEnumerable)desired)
                count++;
            return count;
        }

        static long DesiredRevision(ROSConnection connection, RosTopicState topic)
        {
            object desired = typeof(ROSConnection).GetField("m_DesiredRegistrations", Private).GetValue(connection);
            foreach (object definition in (IEnumerable)desired)
            {
                if (ReferenceEquals(definition.GetType().GetField("Topic", Private).GetValue(definition), topic))
                    return (long)definition.GetType().GetField("Revision", Private).GetValue(definition);
            }
            Assert.Fail("The topic has no desired registration.");
            return 0;
        }

        [Test]
        public void ActiveConfiguredTopicRejectsConflictingDiscoverySchema()
        {
            var gameObject = new GameObject("configured discovery schema conflict");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                RosTopicState topic = connection.RegisterPublisher<TimeMsg>("/configured-discovery", 1, false);

                Assert.Throws<ArgumentException>(() => connection.GetOrCreateTopic(
                    "/configured-discovery", "other_msgs/ConflictingType"));

                Assert.AreEqual(TimeMsg.k_RosMessageName, topic.RosMessageName);
                Assert.IsTrue(topic.IsPublisher);
                Assert.IsNotNull(topic.MessageSender);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void InvalidRosServiceConfigurationDoesNotPublishGhostTopic()
        {
            var gameObject = new GameObject("invalid ROS service catalog");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                int notifications = 0;
                connection.ListenForTopics(_ => notifications++);

                Assert.Throws<ArgumentException>(() => connection.RegisterRosService(
                    "/invalid-ros-service", TimeMsg.k_RosMessageName, null, 1));

                Assert.IsNull(connection.GetTopic("/invalid-ros-service"));
                Assert.AreEqual(0, notifications);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [TestCase("unity")]
        [TestCase("ros")]
        public void InvalidExistingPlaceholderConfigurationDoesNotMutateCatalog(string replacement)
        {
            var gameObject = new GameObject("invalid existing placeholder configuration");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                RosTopicState topic = connection.GetOrCreateTopic(
                    "/invalid-existing-placeholder", "example_msgs/Placeholder");
                int desiredRegistrations = DesiredRegistrationCount(connection);

                if (replacement == "unity")
                {
                    Assert.That(() => connection.ImplementService<TimeMsg, TimeMsg>(
                        "/invalid-existing-placeholder", _ => new TimeMsg(), 0),
                        Throws.InstanceOf<Exception>());
                }
                else
                {
                    Assert.That(() => connection.RegisterRosService(
                        "/invalid-existing-placeholder", TimeMsg.k_RosMessageName,
                        TimeMsg.k_RosMessageName, 0),
                        Throws.InstanceOf<Exception>());
                }

                Assert.AreSame(topic, connection.GetTopic("/invalid-existing-placeholder"));
                Assert.AreEqual("example_msgs/Placeholder", topic.RosMessageName,
                    "A rejected existing-topic configuration must not mutate a discovery placeholder.");
                Assert.IsFalse(topic.IsPublisher);
                Assert.IsFalse(topic.IsRosService);
                Assert.IsFalse(topic.IsUnityService);
                Assert.IsNull(topic.ServiceResponseTopic);
                Assert.AreEqual(desiredRegistrations, DesiredRegistrationCount(connection));
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [Test]
        public void ExistingDiscoveryPlaceholderConfigurationPublishesRegistrationAtomically()
        {
            var gameObject = new GameObject("atomic existing discovery configuration");
            var listener = new TcpListener(IPAddress.Loopback, 0);
            ROSConnectionConfig config = null;
            TcpClient peer = null;
            FieldInfo configurationHook = null;
            Task<Exception> configuration = null;
            Task<Exception> publication = null;
            var configurationEntered = new ManualResetEventSlim(false);
            var releaseConfiguration = new ManualResetEventSlim(false);
            const string topicName = "/atomic-existing-discovery";
            try
            {
                listener.Start();
                config = Config(((IPEndPoint)listener.LocalEndpoint).Port);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectOnStart = false;
                connection.listenForTFMessages = false;
                connection.ConnectionConfig = config;

                Task<TcpClient> accepted = listener.AcceptTcpClientAsync();
                connection.Connect();
                Assert.IsTrue(accepted.Wait(TimeSpan.FromSeconds(5)));
                peer = accepted.Result;
                peer.GetStream().ReadTimeout = 5000;
                Assert.IsTrue(SpinWait.SpinUntil(() => AttemptTestAccess.Current(connection) != null,
                    TimeSpan.FromSeconds(5)), "The connection must publish a live attempt before discovery completion.");

                MethodInfo discover = typeof(ROSConnection).GetMethod(
                    "GetOrCreateTopicForAttempt", Private);
                Assert.NotNull(discover);
                RosTopicState placeholder = (RosTopicState)discover.Invoke(connection, new object[]
                {
                    topicName, TimeMsg.k_RosMessageName, false, AttemptTestAccess.Current(connection)
                });
                Assert.IsNotNull(placeholder);

                configurationHook = typeof(ROSConnection).GetField(
                    "m_BeforeExistingTopicConfigurationCommitTestHook", Private);
                Assert.NotNull(configurationHook,
                    "Existing-topic completion needs an outside-the-lock transaction barrier.");
                configurationHook.SetValue(connection, new Action(() =>
                {
                    configurationEntered.Set();
                    if (!releaseConfiguration.Wait(TimeSpan.FromSeconds(5)))
                        throw new TimeoutException("Existing-topic completion did not reach its transaction barrier.");
                }));

                configuration = Task.Run(() =>
                {
                    try
                    {
                        connection.RegisterPublisher(topicName, TimeMsg.k_RosMessageName, 1, false);
                        return (Exception)null;
                    }
                    catch (Exception exception) { return exception; }
                });
                Assert.IsTrue(configurationEntered.Wait(TimeSpan.FromSeconds(5)),
                    "Existing-topic configuration must expose a deterministic pre-commit boundary.");

                publication = Task.Run(() =>
                {
                    try
                    {
                        connection.Publish(topicName, new TimeMsg(1, 2));
                        return (Exception)null;
                    }
                    catch (Exception exception) { return exception; }
                });
                Assert.IsTrue(publication.Wait(TimeSpan.FromSeconds(5)),
                    "Publication must complete while existing-topic configuration is paused.");
                Assert.IsNotNull(publication.Result,
                    "A topic role must not become visible to publishers before its registration intent is committed.");

                releaseConfiguration.Set();
                Assert.IsTrue(configuration.Wait(TimeSpan.FromSeconds(5)));
                Assert.IsNull(configuration.Result, configuration.Result?.ToString());

                connection.Publish(topicName, new TimeMsg(7, 11));
                connection.QueueSysCommand("__atomic-existing-discovery-fence", new SysCommand_TopicsRequest());
                var frames = new List<Tuple<string, byte[]>>();
                for (int index = 0; index < 16; index++)
                {
                    Tuple<string, byte[]> frame = ReadFrame(peer.GetStream());
                    frames.Add(frame);
                    if (frame.Item1 == "__atomic-existing-discovery-fence") break;
                }

                Assert.AreEqual(1, frames.FindAll(frame => frame.Item1 == SysCommand.k_SysCommand_Publish).Count,
                    "The completed placeholder must reconcile exactly one publisher registration.");
                Assert.AreEqual(1, frames.FindAll(frame => frame.Item1 == topicName).Count,
                    "Only the post-commit publication may reach the wire.");
                int registration = frames.FindIndex(frame => frame.Item1 == SysCommand.k_SysCommand_Publish);
                int payload = frames.FindIndex(frame => frame.Item1 == topicName);
                int fence = frames.FindIndex(frame => frame.Item1 == "__atomic-existing-discovery-fence");
                Assert.GreaterOrEqual(registration, 0);
                Assert.Greater(payload, registration);
                Assert.Greater(fence, payload);
            }
            finally
            {
                releaseConfiguration.Set();
                if (configuration != null) configuration.Wait(TimeSpan.FromSeconds(5));
                if (publication != null) publication.Wait(TimeSpan.FromSeconds(5));
                if (configurationHook != null && gameObject != null)
                    configurationHook.SetValue(gameObject.GetComponent<ROSConnection>(), null);
                try { if (gameObject != null) gameObject.GetComponent<ROSConnection>()?.Disconnect(); }
                catch { }
                peer?.Close();
                listener.Stop();
                UnityEngine.Object.DestroyImmediate(gameObject);
                if (config != null) UnityEngine.Object.DestroyImmediate(config);
                releaseConfiguration.Dispose();
                configurationEntered.Dispose();
            }
        }

        [Test]
        public void RegistrationTransportHasNoImmediateNetworkEntryPoints()
        {
            Type internalApi = typeof(ROSConnection).GetNestedType("InternalAPI", Private);
            Assert.IsNull(internalApi.GetMethod("SendSubscriberRegistration", Private));
            Assert.IsNull(internalApi.GetMethod("SendPublisherRegistration", Private));
            Assert.IsNull(internalApi.GetMethod("SendRosServiceRegistration", Private));
            Assert.IsNull(internalApi.GetMethod("SendUnityServiceRegistration", Private));
            Assert.IsNull(internalApi.GetMethod("SendSubscriberUnregistration", Private));
            Assert.IsNull(internalApi.GetMethod("SendPublisherUnregistration", Private));
            Assert.IsNull(internalApi.GetMethod("SendRosServiceUnregistration", Private));
            Assert.IsNull(internalApi.GetMethod("SendUnityServiceUnregistration", Private));
            Assert.IsNull(typeof(ROSConnection).GetMethod("SendSysCommand", Private));
            Assert.IsNull(typeof(ROSConnection).GetMethod("SendSysCommandImmediate", Private));
        }
    }
}
