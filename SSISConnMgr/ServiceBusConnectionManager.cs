using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Dts.Runtime;
using System.ComponentModel;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Globalization;
using System.Xml;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceBus;

namespace BluewaterSQL.DTS.ServiceBus.ConnectionManager
{
    //
    [DtsConnection(
        ConnectionType = "AZURESVCBUS",
        DisplayName = "Azure Service Bus Connection",
        Description = "Queue, Topic & Subscription Connection Manager for Azure Service Bus",
        UITypeName = "BluewaterSQL.DTS.ServiceBus.ConnectionManager.ServiceBusConnectionManagerUI,BluewaterSQL.DTS.ServiceBus.Connections,Version=1.0.0.0,Culture=neutral,PublicKeyToken=22979edccb0a9754"
    )]
    public sealed class ServiceBusConnectionManager : ConnectionManagerBase, IDTSComponentPersist
    {
        #region Members
        private MessageClientEntity client;
        #endregion

        #region Interface Methods
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override bool Equals(object otherObj)
        {
            return base.Equals(otherObj);
        }

        public override Microsoft.SqlServer.Dts.Runtime.DTSProtectionLevel ProtectionLevel
        {
            get { return base.ProtectionLevel; }
            set { base.ProtectionLevel = value; }
        }

        public override DTSExecResult Validate(IDTSInfoEvents infoEvents)
        {
            bool isValid = true;

            if (string.IsNullOrEmpty(this.Endpoint))
            {
                infoEvents.FireError(-1, null, "Endpoint cannot be null or empty.", null, 0);
                isValid = false;
            }

            if (string.IsNullOrEmpty(this.SharedAccessKeyName))
            {
                infoEvents.FireError(-1, null, "Shared Access Key Name cannot be null or empty.", null, 0);
                isValid = false;
            }

            if (string.IsNullOrEmpty(this.SharedAccessKey))
            {
                infoEvents.FireError(-1, null, "Shared Access Key cannot be null or empty.", null, 0);
                isValid = false;
            }
            
            switch (this.Mode)
            {
                case(Modes.QUEUE):
                    if (string.IsNullOrEmpty(this.Queue))
                    {
                        infoEvents.FireError(-1, null, "Queue cannot be null or empty.", null, 0);
                        isValid = false;
                    }

                    break;
                case (Modes.SUBSCRIPTION):
                    if (string.IsNullOrEmpty(this.Subscription))
                    {
                        infoEvents.FireError(-1, null, "Subscription cannot be null or empty.", null, 0);
                        isValid = false;
                    }

                    if (string.IsNullOrEmpty(this.Topic))
                    {
                        infoEvents.FireError(-1, null, "Topic cannot be null or empty.", null, 0);
                        isValid = false;
                    }

                    break;
                case (Modes.TOPIC):
                    if (string.IsNullOrEmpty(this.Topic))
                    {
                        infoEvents.FireError(-1, null, "Topic cannot be null or empty.", null, 0);
                        isValid = false;
                    }

                    break;
                default:
                    break;
            }

            return (isValid == true) ? DTSExecResult.Success : DTSExecResult.Failure;
        }

        public override object AcquireConnection(object txn)
        {
            if (client == null)
            {
                MessagingFactory Factory = MessagingFactory.CreateFromConnectionString(this.ConnectionString);

                switch (this.Mode)
                {
                    case (Modes.QUEUE):
                        this.client = Factory.CreateQueueClient(this.Queue, this.ReceiveMode);
                        break;
                    case (Modes.TOPIC):
                        this.client = Factory.CreateTopicClient(this.Topic);
                        break;
                    case (Modes.SUBSCRIPTION):
                        this.client = Factory.CreateSubscriptionClient(this.Topic, this.Subscription);
                        break;
                    default:
                        break;
                }

                if (this.CreateWhenNotExists)
                {
                    var namespaceMgr = NamespaceManager.CreateFromConnectionString(this.ConnectionString);

                    switch (this.Mode)
                    {
                        case (Modes.QUEUE):
                            if (!namespaceMgr.QueueExists(this.Queue))
                                namespaceMgr.CreateQueue(this.Queue);

                            break;
                        case (Modes.TOPIC):
                            if (!namespaceMgr.TopicExists(this.Topic))
                                namespaceMgr.CreateTopic(this.Topic);

                            break;
                        case (Modes.SUBSCRIPTION):
                            if (!namespaceMgr.SubscriptionExists(this.Topic, this.Subscription))
                                namespaceMgr.CreateSubscription(this.Topic, this.Subscription);

                            break;
                        default:
                            break;
                    }
                }
            }

            return client;
        }

        public override void ReleaseConnection(object connection)
        {
            if (client != null && !client.IsClosed)
            {
                client.Close();
            }

            client = null;
        }
        #endregion

        #region Properties
        public override string ConnectionString
        {
            get
            {
                return String.Format(CultureInfo.InvariantCulture,
                                     "Endpoint={0};SharedAccessKeyName={1};SharedAccessKey={2}",
                                     Endpoint,
                                     SharedAccessKeyName,
                                     SharedAccessKey);
            }
        }

        [Description("Endpoint")]
        public string Endpoint { get; set; }

        [Description("Connection User")]
        public string SharedAccessKeyName { get; set; }

        [Description("Connection Password")]
        public string SharedAccessKey { get; set; }

        [Description("Service Bus Mode: Queue, Topic or Subscription")]
        public Modes Mode { get; set; }

        [Description("Queue")]
        public string Queue { get; set; }

        [Description("Topic")]
        public string Topic { get; set; }

        [Description("Subscription")]
        public string Subscription { get; set; }

        [Description("Client Receive Mode")]
        public ReceiveMode ReceiveMode { get; set; }
        
        [Description("Create Service Bus Object When Not Exist")]
        public bool CreateWhenNotExists { get; set; }
        #endregion

        #region IDTSComponentPersist Members

        #region String Constants
        private const string PERSIST_XML_ELEMENT = "AZURESVCBUSConnectionManager";
        private const string PERSIST_XML_CONNECTIONSTRING = "ConnectionString";
        private const string PERSIST_XML_MODE = "Mode";
        private const string PERSIST_XML_QUEUENAME = "Queue";
        private const string PERSIST_XML_TOPICNAME = "Topic";
        private const string PERSIST_XML_SUBSCRIPTION = "Subscription";
        private const string PERSIST_XML_RECEIVE_MODE = "ReceiveMode";

        private const string PERSIST_XML_ENDPOINT = "Endpoint";
        private const string PERSIST_XML_SHAREDSECRETISSUER = "SharedSecretIssuer";
        private const string PERSIST_XML_SHAREDSECRETVALUE = "SharedSecretValue";
        private const string PERSIST_XML_CREATEWHENNOTEXISTS = "CreateWhenNotExists";
        #endregion

        void IDTSComponentPersist.LoadFromXML(XmlElement rootNode, IDTSInfoEvents infoEvents)
        {
            // Create an root node for the data
            if (rootNode.Name != PERSIST_XML_ELEMENT)
            {
                throw new ArgumentException("Unexpected element");
            }

            // Unpersist the properties
            if (rootNode.Attributes.GetNamedItem(PERSIST_XML_ENDPOINT) != null)
                this.Endpoint = rootNode.Attributes.GetNamedItem(PERSIST_XML_ENDPOINT).Value;

            if (rootNode.Attributes.GetNamedItem(PERSIST_XML_SHAREDSECRETISSUER) != null)
                this.SharedAccessKeyName= rootNode.Attributes.GetNamedItem(PERSIST_XML_SHAREDSECRETISSUER).Value;

            if (rootNode.Attributes.GetNamedItem(PERSIST_XML_SHAREDSECRETVALUE) != null)
                this.SharedAccessKey = rootNode.Attributes.GetNamedItem(PERSIST_XML_SHAREDSECRETVALUE).Value;

            if (rootNode.Attributes.GetNamedItem(PERSIST_XML_MODE) != null)
                this.Mode = (Modes)Enum.Parse(typeof(Modes), rootNode.Attributes.GetNamedItem(PERSIST_XML_MODE).Value);
            
            if (rootNode.Attributes.GetNamedItem(PERSIST_XML_QUEUENAME) != null)
                this.Queue = rootNode.Attributes.GetNamedItem(PERSIST_XML_QUEUENAME).Value;

            if (rootNode.Attributes.GetNamedItem(PERSIST_XML_TOPICNAME) != null)
                this.Topic = rootNode.Attributes.GetNamedItem(PERSIST_XML_TOPICNAME).Value;

            if (rootNode.Attributes.GetNamedItem(PERSIST_XML_SUBSCRIPTION) != null)
                this.Subscription = rootNode.Attributes.GetNamedItem(PERSIST_XML_SUBSCRIPTION).Value;

            if (rootNode.Attributes.GetNamedItem(PERSIST_XML_CREATEWHENNOTEXISTS) != null)
                this.CreateWhenNotExists = bool.Parse(rootNode.Attributes.GetNamedItem(PERSIST_XML_CREATEWHENNOTEXISTS).Value);

            if (rootNode.Attributes.GetNamedItem(PERSIST_XML_RECEIVE_MODE) != null)
                this.ReceiveMode = (ReceiveMode)Enum.Parse(typeof(ReceiveMode), rootNode.Attributes.GetNamedItem(PERSIST_XML_RECEIVE_MODE).Value);
        }

        void IDTSComponentPersist.SaveToXML(XmlDocument doc, IDTSInfoEvents infoEvents)
        {
            // Create a root node for the data
            XmlElement rootElement = doc.CreateElement(String.Empty, PERSIST_XML_ELEMENT, String.Empty);
            doc.AppendChild(rootElement);

            // Persist properties
            rootElement.Attributes.Append(doc.CreateAttribute(PERSIST_XML_ENDPOINT));
            rootElement.Attributes[PERSIST_XML_ENDPOINT].Value = this.Endpoint;
            
            rootElement.Attributes.Append(doc.CreateAttribute(PERSIST_XML_SHAREDSECRETISSUER));
            rootElement.Attributes[PERSIST_XML_SHAREDSECRETISSUER].Value = this.SharedAccessKeyName;

            rootElement.Attributes.Append(doc.CreateAttribute(PERSIST_XML_SHAREDSECRETVALUE));
            rootElement.Attributes[PERSIST_XML_SHAREDSECRETVALUE].Value = this.SharedAccessKey;

            rootElement.Attributes.Append(doc.CreateAttribute(PERSIST_XML_MODE));
            rootElement.Attributes[PERSIST_XML_MODE].Value = this.Mode.ToString();

            rootElement.Attributes.Append(doc.CreateAttribute(PERSIST_XML_QUEUENAME));
            rootElement.Attributes[PERSIST_XML_QUEUENAME].Value = this.Queue;

            rootElement.Attributes.Append(doc.CreateAttribute(PERSIST_XML_TOPICNAME));
            rootElement.Attributes[PERSIST_XML_TOPICNAME].Value = this.Topic;

            rootElement.Attributes.Append(doc.CreateAttribute(PERSIST_XML_SUBSCRIPTION));
            rootElement.Attributes[PERSIST_XML_SUBSCRIPTION].Value = this.Subscription;

            rootElement.Attributes.Append(doc.CreateAttribute(PERSIST_XML_CREATEWHENNOTEXISTS));
            rootElement.Attributes[PERSIST_XML_CREATEWHENNOTEXISTS].Value = this.CreateWhenNotExists.ToString();

            rootElement.Attributes.Append(doc.CreateAttribute(PERSIST_XML_RECEIVE_MODE));
            rootElement.Attributes[PERSIST_XML_RECEIVE_MODE].Value = this.ReceiveMode.ToString();
        }

        #endregion
    }
}
