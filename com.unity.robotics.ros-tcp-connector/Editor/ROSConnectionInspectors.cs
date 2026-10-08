using UnityEditor;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector.Editor
{
    static class ROSConnectionEditorPolicy
    {
        public static bool EndpointConfigurationIsEditable(bool isPlaying)
        {
            return !isPlaying;
        }
    }

    [CustomEditor(typeof(ROSConnectionConfig), true)]
    class ROSConnectionConfigInspector : UnityEditor.Editor
    {
        public override void OnInspectorGUI()
        {
            EditorGUI.BeginDisabledGroup(!ROSConnectionEditorPolicy.EndpointConfigurationIsEditable(EditorApplication.isPlaying));
            DrawDefaultInspector();
            EditorGUI.EndDisabledGroup();
        }
    }

    [CustomEditor(typeof(ROSConnection), true)]
    class ROSConnectionInspector : UnityEditor.Editor
    {
        public override void OnInspectorGUI()
        {
            serializedObject.Update();
            SerializedProperty iterator = serializedObject.GetIterator();
            bool enterChildren = true;
            while (iterator.NextVisible(enterChildren))
            {
                enterChildren = false;
                bool disable = iterator.name == "m_ConnectionConfig"
                    && !ROSConnectionEditorPolicy.EndpointConfigurationIsEditable(EditorApplication.isPlaying);
                EditorGUI.BeginDisabledGroup(disable);
                EditorGUILayout.PropertyField(iterator, true);
                EditorGUI.EndDisabledGroup();
            }
            serializedObject.ApplyModifiedProperties();
        }
    }
}
