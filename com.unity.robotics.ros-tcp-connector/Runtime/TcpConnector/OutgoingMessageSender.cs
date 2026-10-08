using System.Collections.Generic;
using System.IO;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;

namespace Unity.Robotics.ROSTCPConnector
{
    public abstract class OutgoingMessageSender
    {

        public enum SendToState
        {
            Normal,
            NoMessageToSendError,
            QueueFullWarning
        }

        public abstract SendToState SendInternal(MessageSerializer m_MessageSerializer, System.IO.Stream stream);

        public abstract void ClearAllQueuedData();
    }

    /**
     * Simple implementation of a OutgoingMessageSender that is used for sys commands
     * as they are handled differently to typical ROS messages and sent as JSON strings.
     */
    public class SysCommandSender : OutgoingMessageSender
    {
        readonly object m_Gate = new object();
        readonly byte[] m_Bytes;
        bool m_Cleared;

        public SysCommandSender(List<byte[]> listOfSerializations)
        {
            if (listOfSerializations == null)
                throw new System.ArgumentNullException(nameof(listOfSerializations));
            long length = 0;
            foreach (byte[] statement in listOfSerializations)
            {
                if (statement == null)
                    throw new System.IO.IOException("A system command contains an unfinished segment.");
                length = checked(length + statement.LongLength);
            }
            m_Bytes = new byte[checked((int)length)];
            int offset = 0;
            foreach (byte[] statement in listOfSerializations)
            {
                System.Buffer.BlockCopy(statement, 0, m_Bytes, offset, statement.Length);
                offset += statement.Length;
            }
        }

        internal SysCommandSender(byte[] bytes)
        {
            m_Bytes = bytes ?? throw new System.ArgumentNullException(nameof(bytes));
        }

        internal byte[] FrozenBytes => m_Bytes;

        public override SendToState SendInternal(MessageSerializer m_MessageSerializer, Stream stream)
        {
            byte[] bytes;
            lock (m_Gate) bytes = m_Cleared ? null : m_Bytes;
            if (bytes == null)
                return SendToState.NoMessageToSendError;
            stream.Write(bytes, 0, bytes.Length);
            return SendToState.Normal;
        }

        public override void ClearAllQueuedData()
        {
            lock (m_Gate) m_Cleared = true;
        }
    }
}
