using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector
{
    public interface IMessagePool
    {
        // Connector handoff completes when this call RETURNS. Custom pools must not
        // expose a root for reuse while AddMessage still accesses it. After handoff,
        // callers must reacquire through their pool, never publish a stale raw alias.
        void AddMessage(Message messageToRecycle);
    }

    public class MessagePool<T> : IMessagePool where T : Message, new()
    {
        ConcurrentQueue<T> m_Contents;
        int m_MaxSize;

        public MessagePool(int maxSize = 10)
        {
            m_MaxSize = maxSize;
            m_Contents = new ConcurrentQueue<T>();
        }

        public void AddMessage(T messageToRecycle)
        {
            // not bothering to lock here - if multiple threads add things simultaneously we could end up a little over m_MaxSize but it's not a big deal
            if (m_Contents.Count < m_MaxSize)
            {
                m_Contents.Enqueue(messageToRecycle);
            }
        }

        void IMessagePool.AddMessage(Message message)
        {
            if (!(message is T))
            {
                throw new System.ArgumentException($"MessagePool<{MessageRegistry.GetRosMessageName<T>()}> can't store a \"{message.RosMessageName}\" message.");
            }

            AddMessage((T)message);
        }

        /// <summary>
        /// Get a message from the pool, or create one if necessary.
        /// </summary>
        /// <returns>A new or recycled message of type T, which may contain garbage data. Be sure to fill it in.</returns>
        public T GetOrCreateMessage()
        {
            T result;

            if (!m_Contents.TryDequeue(out result) || MessageUseRegistry.IsReturning(result))
            {
                // AddMessage enqueues before returning to the connector's recycler.
                // Abandon a dequeued candidate still in that handoff window; neither
                // wait for user code nor execute the T constructor under the registry gate.
                result = new T();
            }

            return result;
        }
    }
}
