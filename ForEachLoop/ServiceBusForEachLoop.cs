using Microsoft.ServiceBus.Messaging;
using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace BluewaterSQL.DTS.ServiceBus.Enumerators
{
    [DtsForEachEnumerator(DisplayName = "ForEach Brokered Message Enumerator", Description = "Service Bus Brokered Message Enumerator",
        UITypeName = "BluewaterSQL.DTS.ServiceBus.Enumerators.UI.ServiceBusForEachLoopUI, BluewaterSQL.DTS.ServiceBus.Enumerators, Version=1.0.0.0, Culture=neutral, PublicKeyToken=05b620296d11a2f5")]
    public sealed class ServiceBusForEachLoop : ForEachEnumerator, IDTSComponentPersist
    {
        #region Properties
        private int serverWaitTimeout = 5;
        private int batchSize = -1;
        private int messageCap = -1;
        private int maxRetrieveAttempts = 3;

        public string Connection { get; set; }

        [Description("Server Request Wait Time Out (seconds)")]
        public int ServerWaitTimeOut
        {
            get { return serverWaitTimeout;  }
            set { serverWaitTimeout = value; }
        }
        
        public int BatchSize
        {
            get { return batchSize;  }
            set { batchSize = value; }
        }

        public int MessageCap
        {
            get { return messageCap; }
            set { messageCap = value; }
        }

        public int MaxRetrieveAttempts
        {
            get { return maxRetrieveAttempts;  }
            set { maxRetrieveAttempts = value; }
        }
        #endregion

        #region IDTSComponentPersist Members
        public void LoadFromXML(System.Xml.XmlElement node, IDTSInfoEvents infoEvents)
        {
            if (node.Name != "FEEBROKERMSG")
            {
                throw new Exception("Invalid Persisted Data");
            }

            if (node.Attributes.GetNamedItem("Connection") != null)
                this.Connection = node.Attributes.GetNamedItem("Connection").Value;

            if (node.Attributes.GetNamedItem("ServerWaitTimeOut") != null)
                this.ServerWaitTimeOut = Convert.ToInt32(node.Attributes.GetNamedItem("ServerWaitTimeOut").Value);

            if (node.Attributes.GetNamedItem("BatchSize") != null)
                this.BatchSize = Convert.ToInt32(node.Attributes.GetNamedItem("BatchSize").Value);

            if (node.Attributes.GetNamedItem("MessageCap") != null)
                this.MessageCap = Convert.ToInt32(node.Attributes.GetNamedItem("MessageCap").Value);
        }

        public void SaveToXML(System.Xml.XmlDocument doc, IDTSInfoEvents infoEvents)
        {
            XmlNode node = doc.CreateNode(XmlNodeType.Element, "FEEBROKERMSG", "");
            XmlElement xmlElement = node as XmlElement;
            doc.AppendChild(node);

            xmlElement.Attributes.Append(doc.CreateAttribute("Connection"));
            xmlElement.Attributes["Connection"].Value = this.Connection;

            xmlElement.Attributes.Append(doc.CreateAttribute("ServerWaitTimeOut"));
            xmlElement.Attributes["ServerWaitTimeOut"].Value = this.ServerWaitTimeOut.ToString();

            xmlElement.Attributes.Append(doc.CreateAttribute("BatchSize"));
            xmlElement.Attributes["BatchSize"].Value = this.BatchSize.ToString();

            xmlElement.Attributes.Append(doc.CreateAttribute("MessageCap"));
            xmlElement.Attributes["MessageCap"].Value = this.MessageCap.ToString();
        }
        #endregion

        public override object GetEnumerator(Connections connections, VariableDispenser variableDispenser, IDTSInfoEvents events, IDTSLogging log)
        {
            object enumerator = null;
            var connMgr = connections[this.Connection];

            MessageClientEntity conn = (MessageClientEntity)connMgr.AcquireConnection(null);

            if (conn is QueueClient)
            {                
                enumerator = new QueueEnumerator(conn, this.ServerWaitTimeOut, this.BatchSize, this.MessageCap);
            }
            else if (conn is SubscriptionClient)
            {
                enumerator = new SubscriptionEnumerator(conn, this.ServerWaitTimeOut, this.BatchSize, this.MessageCap);
            }
            
            if (enumerator == null)
                return new EmptyEnumerator();

            return enumerator;
        }

        public override DTSExecResult Validate(Connections connections, VariableDispenser variableDispenser, IDTSInfoEvents infoEvents, IDTSLogging log)
        {
            bool isValid = true;

            if (string.IsNullOrEmpty(this.Connection))
            {
                infoEvents.FireError(0, null, "Connection must be set.", null, 0);
                isValid = false;
            }

            bool connectionFound = false;

            foreach (var conn in connections)
            {
                if (conn.Name.Equals(this.Connection))
                {
                    connectionFound = true;

                    var obj = conn.AcquireConnection(null);

                    if (!(obj is QueueClient) && !(obj is SubscriptionClient))
                    {
                        isValid = false;
                        infoEvents.FireError(0, null, "Invalid Service Bus Connection Type", null, 0);
                    } 
                }
            }

            if (!connectionFound)
            {
                infoEvents.FireError(0, null, "Connection undefined.", null, 0);
                isValid = false;
            }

            if (this.BatchSize != -1 && this.MessageCap != -1)
            {
                if (this.MessageCap < this.BatchSize)
                {
                    infoEvents.FireError(0, null, "Message Cap cannot be less than the configured Batch Size.", null, 0);
                    isValid = false;
                }
            }

            return (isValid == true) ? DTSExecResult.Success : DTSExecResult.Failure;
        }
    }
}
