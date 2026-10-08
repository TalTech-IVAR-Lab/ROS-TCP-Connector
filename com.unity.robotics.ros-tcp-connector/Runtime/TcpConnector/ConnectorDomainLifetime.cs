using System;
using System.Collections.Generic;
using UnityEngine;
#if UNITY_EDITOR
using UnityEditor;
#endif

namespace Unity.Robotics.ROSTCPConnector
{
    // Owns only the domain/application lifetime bridge. Connection teardown remains
    // connection-owned, and no Unity object work is performed under this gate.
    internal static class ConnectorDomainLifetime
    {
        static readonly object s_Gate = new object();
        static readonly List<ROSConnection> s_Connections = new List<ROSConnection>();

        internal static void Register(ROSConnection connection)
        {
            if (ReferenceEquals(connection, null))
                return;
            lock (s_Gate)
            {
                foreach (ROSConnection existing in s_Connections)
                    if (ReferenceEquals(existing, connection))
                        return;
                s_Connections.Add(connection);
            }
        }

        internal static void Unregister(ROSConnection connection)
        {
            if (ReferenceEquals(connection, null))
                return;
            lock (s_Gate)
            {
                for (int index = 0; index < s_Connections.Count; ++index)
                {
                    if (!ReferenceEquals(s_Connections[index], connection))
                        continue;
                    s_Connections.RemoveAt(index);
                    return;
                }
            }
        }

        internal static void RequestStop()
        {
            ROSConnection[] connections;
            lock (s_Gate)
                connections = s_Connections.ToArray();

            try
            {
                foreach (ROSConnection connection in connections)
                {
                    try { connection.Disconnect(); }
                    catch (Exception exception) { Debug.LogException(exception); }
                }
            }
            finally
            {
                // Do not wait for an arbitrary pool callback. Pending offers are
                // dropped, while an already-running callback is allowed to return.
                MessageRecycler.RequestStop();
                MessageRecycler.DropPendingReturns();
            }
        }

        internal static void OnApplicationQuitting()
        {
            RequestStop();
        }

        internal static void OnBeforeAssemblyReload()
        {
            RequestStop();
        }

#if UNITY_EDITOR
        [InitializeOnLoadMethod]
        static void RegisterEditorHooks()
        {
            AssemblyReloadEvents.beforeAssemblyReload -= OnBeforeAssemblyReload;
            AssemblyReloadEvents.beforeAssemblyReload += OnBeforeAssemblyReload;
            EditorApplication.playModeStateChanged -= OnPlayModeStateChanged;
            EditorApplication.playModeStateChanged += OnPlayModeStateChanged;
        }

        static void OnPlayModeStateChanged(PlayModeStateChange state)
        {
            if (state == PlayModeStateChange.ExitingPlayMode)
                RequestStop();
        }
#endif

        [RuntimeInitializeOnLoadMethod(RuntimeInitializeLoadType.BeforeSceneLoad)]
        static void RegisterRuntimeHooks()
        {
            Application.quitting -= OnApplicationQuitting;
            Application.quitting += OnApplicationQuitting;
        }
    }
}
