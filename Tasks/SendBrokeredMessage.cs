using Microsoft.ServiceBus.Messaging;
using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

namespace BluewaterSQL.DTS.ServiceBus.Tasks
{
    [DtsTask(DisplayName = "Send Brokered Message",
        IconResource = "BluewaterSQL.DTS.ServiceBus.Tasks.Task.ico",
        UITypeName = "BluewaterSQL.DTS.ServiceBus.Tasks.SendBrokeredMessageUI, BluewaterSQL.DTS.ServiceBus.Tasks, Version=1.0.0.0, Culture=Neutral, PublicKeyToken=697bf476eb75315e")]
    public class SendBrokeredMessage : TaskBase, IDTSPersist
    {
        #region Properties
        public bool UseAsync
        {
            get;
            set;
        }

        public SourceType SourceType 
        {
            get { return _sourceType;  }
            set { _sourceType = value;  }
        }
        public string Source
        {
            get { return _brokeredMessage;  }
            set
            {
                _brokeredMessage = value;
                if (_brokeredMessage != null)
                    return;
                _brokeredMessage = string.Empty;
            }
        }
        #endregion

        #region Methods
        protected override DTSExecResult InternalExecute(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents componentEvents, IDTSLogging log, object transaction)
        {
            var connectionManager = this.ValidateConnection(connections);
            
            if ((DtsObject)connectionManager == null)
                return DTSExecResult.Failure;

            var data = GetMessageData(variableDispenser);

            if (data == null)
                return DTSExecResult.Failure;

            try
            {
                var conn = connectionManager.AcquireConnection(transaction);

                BrokeredMessage msg = new BrokeredMessage(data);

                if (conn is QueueClient)
                {
                    QueueClient client = conn as QueueClient;

                    if (this.UseAsync)
                        client.SendAsync(msg);
                    else
                        client.Send(msg);                    
                }
                else if (conn is TopicClient)
                {
                    TopicClient client = conn as TopicClient;

                    if (this.UseAsync)
                        client.SendAsync(msg);
                    else
                        client.Send(msg); 
                }

                return DTSExecResult.Success;
            }
            catch (Exception ex)
            {
                FireError(ex.Message);
                return DTSExecResult.Failure;
            }
        }

        protected override DTSExecResult InternalValidate(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents componentEvents, IDTSLogging log)
        {
            bool bValid = true;

            if (string.IsNullOrEmpty(this.Source))
            {
                bValid = false;
                FireError("Source cannot be null");
            }

            if (this.SourceType == Tasks.SourceType.Variable)
            {
                if (!variableDispenser.Contains(this.Source))
                {
                    bValid = false;
                    FireError("The Source Variable does not exist");
                }
            }

            var connectionManager = this.ValidateConnection(connections);

            if (connectionManager != null)
            {
                var conn = connectionManager.AcquireConnection(null);

                if (!(conn is QueueClient) && !(conn is TopicClient))
                {
                    bValid = false;
                    FireError("Invalid Service Bus Connection Type");
                }
            }
            else
            {
                bValid = false;
            }

            return (bValid == true) ? DTSExecResult.Success : DTSExecResult.Failure;
        }
        #endregion

        #region Helper Methods
        private object GetMessageData(VariableDispenser variableDispenser)
        {
            object data = null;

            try
            {
                if (this.SourceType == Tasks.SourceType.Direct)
                {
                    data = this.Source;
                }
                else if (this.SourceType == Tasks.SourceType.Variable)
                {
                    Variables variables = null;
                    variableDispenser.LockOneForRead(this.Source, ref variables);

                    object obj = variables[0].Value;
                    if (obj == null)
                    {
                        this.FireError("Invalid Variable");
                    }
                    else
                    {
                        data = obj;
                        variables.Unlock();
                    }
                }
            }
            catch (Exception ex)
            {
                FireError(ex.Message);
                return null;
            }

            return data;
        }
        #endregion

        #region IDTSComponentPersist Members
        public void LoadFromXML(System.Xml.XmlNode node, IDTSEvents events)
        {
            if (node.Name != "SVCBROKERMSGSENDER")
            {
                throw new Exception("Invalid Persisted Data");
            }

            if (node.Attributes.GetNamedItem("Connection") != null)
                this.ConnectionName = node.Attributes.GetNamedItem("ConnectionName").Value;

            if (node.Attributes.GetNamedItem("Source") != null)
                this.Source = node.Attributes.GetNamedItem("Source").Value;

            if (node.Attributes.GetNamedItem("UseAsync") != null)
                this.UseAsync = bool.Parse(node.Attributes.GetNamedItem("UseAsynce").Value);

            if (node.Attributes.GetNamedItem("SourceType") != null)
                this.SourceType = (SourceType)Enum.Parse(typeof(SourceType), node.Attributes.GetNamedItem("SourceType").Value);
        }

        public void SaveToXML(ref System.Xml.XmlDocument doc, System.Xml.XmlNode node, IDTSEvents events)
        {
            XmlNode xmlNode = doc.CreateNode(XmlNodeType.Element, "SVCBROKERMSGSENDER", "");

            XmlElement xmlElement = xmlNode as XmlElement;
            doc.AppendChild(node);

            xmlElement.Attributes.Append(doc.CreateAttribute("Connection"));
            xmlElement.Attributes["ConnectionName"].Value = this.ConnectionName;

            xmlElement.Attributes.Append(doc.CreateAttribute("Source"));
            xmlElement.Attributes["Source"].Value = this.Source;

            xmlElement.Attributes.Append(doc.CreateAttribute("Type"));
            xmlElement.Attributes["SourceType"].Value = this.SourceType.ToString();

            xmlElement.Attributes.Append(doc.CreateAttribute("UseAsync"));
            xmlElement.Attributes["UseAsync"].Value = this.UseAsync.ToString();
        }
        #endregion
    }
}
