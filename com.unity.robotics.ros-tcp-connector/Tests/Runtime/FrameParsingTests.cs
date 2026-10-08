using System;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Unity.Robotics.ROSTCPConnector;

namespace UnitTests
{
    public class FrameParsingTests
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

        sealed class ZeroReadStream : MemoryStream
        {
            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                return Task.FromResult(0);
            }
        }

        static byte[] Frame(int topicLength, byte[] topic, int messageLength, byte[] message)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                writer.Write(topicLength);
                if (topic != null)
                    writer.Write(topic);
                writer.Write(messageLength);
                if (message != null)
                    writer.Write(message);
                return stream.ToArray();
            }
        }

        static Task<Tuple<string, byte[]>> Read(Stream stream)
        {
            MethodInfo method = typeof(ROSConnection).GetMethod(
                "ReadMessageContents", BindingFlags.Static | BindingFlags.NonPublic);
            Assert.NotNull(method);
            return (Task<Tuple<string, byte[]>>)method.Invoke(null, new object[]
            {
                stream, 1, CancellationToken.None
            });
        }

        [TestCase(-1)]
        [TestCase(4097)]
        public void TopicLengthOutsideBoundsIsRejected(int length)
        {
            AssertThrowsAsync<InvalidDataException>(() =>
                Read(new MemoryStream(Frame(length, null, 0, null))));
        }

        [TestCase(-1)]
        [TestCase(67108865)]
        public void MessageLengthOutsideBoundsIsRejected(int length)
        {
            AssertThrowsAsync<InvalidDataException>(() =>
                Read(new MemoryStream(Frame(1, new byte[] { (byte)'a' }, length, null))));
        }

        [Test]
        public void TruncatedFrameReportsClosedConnectionImmediately()
        {
            AssertThrowsAsync<EndOfStreamException>(() =>
                Read(new MemoryStream(Frame(1, new byte[] { (byte)'a' }, 2, new byte[] { 1 }))));
        }

        [Test]
        public void ZeroByteReadReportsClosedConnectionImmediately()
        {
            AssertThrowsAsync<EndOfStreamException>(() => Read(new ZeroReadStream()));
        }

        [Test]
        public void ConcurrentReadersUseIndependentBuffers()
        {
            byte[] firstPayload = { 1, 2, 3 };
            byte[] secondPayload = { 7, 8 };
            Task<Tuple<string, byte[]>> first = Read(new MemoryStream(
                Frame(4, System.Text.Encoding.ASCII.GetBytes("/one"), 3, firstPayload)));
            Task<Tuple<string, byte[]>> second = Read(new MemoryStream(
                Frame(4, System.Text.Encoding.ASCII.GetBytes("/two"), 2, secondPayload)));

            Tuple<string, byte[]>[] results = Task.WhenAll(first, second).GetAwaiter().GetResult();

            Assert.AreEqual("/one", results[0].Item1);
            CollectionAssert.AreEqual(firstPayload, results[0].Item2);
            Assert.AreEqual("/two", results[1].Item1);
            CollectionAssert.AreEqual(secondPayload, results[1].Item2);
        }
    }
}
