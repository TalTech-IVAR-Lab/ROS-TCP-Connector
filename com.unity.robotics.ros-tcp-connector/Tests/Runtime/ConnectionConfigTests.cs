using System;
using System.Linq;
using System.Reflection;
using NUnit.Framework;
using Unity.Robotics.ROSTCPConnector;
using UnityEngine;

namespace UnitTests
{
    public class ConnectionConfigTests
    {
        [Test]
        public void ConfigIsSoleEndpointSource()
        {
            var fields = typeof(ROSConnection).GetFields(BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.IsFalse(fields.Any(field => field.Name == "m_RosIPAddress"));
            Assert.IsFalse(fields.Any(field => field.Name == "m_RosPort"));
            Assert.IsFalse(typeof(ROSConnection).GetProperty("RosIPAddress").CanWrite);
            Assert.IsFalse(typeof(ROSConnection).GetProperty("RosPort").CanWrite);
            Assert.IsNull(typeof(ROSConnection).GetMethod("Connect", new[] { typeof(string), typeof(int) }));
        }

        [Test]
        public void AssignedConfigSuppliesEndpoint()
        {
            var gameObject = new GameObject("configured connection");
            var config = ScriptableObject.CreateInstance<ROSConnectionConfig>();
            try
            {
                SetConfig(config, "robot.local", 12000);
                var connection = gameObject.AddComponent<ROSConnection>();
                connection.ConnectionConfig = config;
                Assert.AreEqual("robot.local", connection.RosIPAddress);
                Assert.AreEqual(12000, connection.RosPort);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
                UnityEngine.Object.DestroyImmediate(config);
            }
        }

        [Test]
        public void ConnectRejectsMissingConfigBeforeStartingWorker()
        {
            var gameObject = new GameObject("missing config connection");
            try
            {
                var connection = gameObject.AddComponent<ROSConnection>();
                Assert.Throws<InvalidOperationException>(() => connection.Connect());
                Assert.IsFalse(connection.HasConnectionThread);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        [TestCase(null, 10000)]
        [TestCase(" ", 10000)]
        [TestCase("bad host!", 10000)]
        [TestCase("localhost", 0)]
        [TestCase("localhost", 65536)]
        public void ConfigValidationRejectsInvalidEndpoint(string address, int port)
        {
            var config = ScriptableObject.CreateInstance<ROSConnectionConfig>();
            try
            {
                SetConfig(config, address, port);
                Assert.IsFalse(config.IsValid);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(config);
            }
        }

        [TestCase("127.0.0.1", 1)]
        [TestCase("::1", 65535)]
        [TestCase("localhost", 10000)]
        [TestCase("1robot.example", 10000)]
        public void ConfigValidationAcceptsTcpClientTargets(string address, int port)
        {
            var config = ScriptableObject.CreateInstance<ROSConnectionConfig>();
            try
            {
                SetConfig(config, address, port);
                Assert.IsTrue(config.IsValid);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(config);
            }
        }

        [TestCase("host name", 10000)]
        [TestCase("host\nname", 10000)]
        [TestCase("-bad.example", 10000)]
        public void ConfigValidationRejectsMalformedHostNames(string address, int port)
        {
            var config = ScriptableObject.CreateInstance<ROSConnectionConfig>();
            try
            {
                SetConfig(config, address, port);
                Assert.IsFalse(config.IsValid);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(config);
            }
        }

        static void SetConfig(ROSConnectionConfig config, string address, int port)
        {
            typeof(ROSConnectionConfig).GetField("m_RosIPAddress", BindingFlags.Instance | BindingFlags.NonPublic)
                .SetValue(config, address);
            typeof(ROSConnectionConfig).GetField("m_RosPort", BindingFlags.Instance | BindingFlags.NonPublic)
                .SetValue(config, port);
        }
    }
}
