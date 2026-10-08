using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector
{
    [CreateAssetMenu(fileName = "ROSConnectionConfig", menuName = "Robotics/ROS Connection Config")]
    public class ROSConnectionConfig : ScriptableObject
    {
        [SerializeField]
        string m_RosIPAddress = "127.0.0.1";

        [SerializeField]
        int m_RosPort = 10000;

        public string RosIPAddress => m_RosIPAddress;
        public int RosPort => m_RosPort;
        public bool IsValid => ROSConnection.IPFormatIsCorrect(m_RosIPAddress)
            && m_RosPort >= 1 && m_RosPort <= 65535;
    }
}
