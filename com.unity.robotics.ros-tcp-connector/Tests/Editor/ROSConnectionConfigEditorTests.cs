using System;
using NUnit.Framework;
using UnityEditor;
using UnityEngine;
using System.Linq;
using System.Reflection;

namespace Unity.Robotics.ROSTCPConnector.Editor.Tests
{
    public class ROSConnectionConfigEditorTests
    {
        class DerivedConnection : ROSConnection { }
        class DerivedConfig : ROSConnectionConfig { }

        string m_TestFolder;
        string m_TestFolderGuid;

        [TearDown]
        public void TearDown()
        {
            if (!string.IsNullOrEmpty(m_TestFolderGuid)
                && AssetDatabase.GUIDToAssetPath(m_TestFolderGuid) == m_TestFolder)
                AssetDatabase.DeleteAsset(m_TestFolder);
        }

        [Test]
        public void EndpointConfigurationIsDisabledDuringPlayMode()
        {
            Type policy = AppDomain.CurrentDomain.GetAssemblies().Select(assembly =>
                assembly.GetType("Unity.Robotics.ROSTCPConnector.Editor.ROSConnectionEditorPolicy")).First(type => type != null);
            MethodInfo editable = policy.GetMethod("EndpointConfigurationIsEditable", BindingFlags.Static | BindingFlags.Public);
            Assert.IsTrue((bool)editable.Invoke(null, new object[] { false }));
            Assert.IsFalse((bool)editable.Invoke(null, new object[] { true }));
        }

        [Test]
        public void PlayModeInspectorsCoverSupportedSubclasses()
        {
            AssertInspectorCoversChildren(typeof(ROSConnection));
            AssertInspectorCoversChildren(typeof(ROSConnectionConfig));

            var gameObject = new GameObject("derived connection");
            var config = ScriptableObject.CreateInstance<DerivedConfig>();
            UnityEditor.Editor connectionEditor = null;
            UnityEditor.Editor configEditor = null;
            try
            {
                connectionEditor = UnityEditor.Editor.CreateEditor(gameObject.AddComponent<DerivedConnection>());
                configEditor = UnityEditor.Editor.CreateEditor(config);
                Assert.AreEqual("ROSConnectionInspector", connectionEditor.GetType().Name);
                Assert.AreEqual("ROSConnectionConfigInspector", configEditor.GetType().Name);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(connectionEditor);
                UnityEngine.Object.DestroyImmediate(configEditor);
                UnityEngine.Object.DestroyImmediate(config);
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }

        static void AssertInspectorCoversChildren(Type inspectedType)
        {
            FieldInfo inspectedTypeField = typeof(CustomEditor).GetField("m_InspectedType", BindingFlags.Instance | BindingFlags.NonPublic);
            FieldInfo childClassesField = typeof(CustomEditor).GetField("m_EditorForChildClasses", BindingFlags.Instance | BindingFlags.NonPublic);
            CustomEditor attribute = AppDomain.CurrentDomain.GetAssemblies().SelectMany(assembly => assembly.GetTypes())
                .SelectMany(type => type.GetCustomAttributes(typeof(CustomEditor), false).Cast<CustomEditor>())
                .Single(candidate => (Type)inspectedTypeField.GetValue(candidate) == inspectedType);
            Assert.IsTrue((bool)childClassesField.GetValue(attribute),
                $"The {inspectedType.Name} play-mode inspector must cover supported subclasses.");
        }

        [Test]
        public void PrefabPersistsConfigAssetReference()
        {
            string folderName = "ROSConnectionConfigEditorTests-" + Guid.NewGuid().ToString("N");
            m_TestFolder = "Assets/" + folderName;
            m_TestFolderGuid = AssetDatabase.CreateFolder("Assets", folderName);
            var config = ScriptableObject.CreateInstance<ROSConnectionConfig>();
            AssetDatabase.CreateAsset(config, m_TestFolder + "/connection.asset");
            var gameObject = new GameObject("ROSConnection");
            try
            {
                gameObject.AddComponent<ROSConnection>().ConnectionConfig = config;
                PrefabUtility.SaveAsPrefabAsset(gameObject, m_TestFolder + "/connection.prefab");

                var prefab = AssetDatabase.LoadAssetAtPath<GameObject>(m_TestFolder + "/connection.prefab");
                Assert.AreSame(config, prefab.GetComponent<ROSConnection>().ConnectionConfig);
            }
            finally
            {
                UnityEngine.Object.DestroyImmediate(gameObject);
            }
        }
    }
}
