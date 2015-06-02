using System;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.ServiceBus.Messaging;
using System.Xml;

namespace BluewaterSQL.DTS.ServiceBus.Tasks
{
    [DtsTask(DisplayName="Brokered Message HandShake",
        IconResource = "BluewaterSQL.DTS.ServiceBus.Tasks.Task.ico",
        UITypeName = "BluewaterSQL.DTS.ServiceBus.Tasks.HandShakeUI, BluewaterSQL.DTS.ServiceBus.Tasks, Version=1.0.0.0, Culture=Neutral, PublicKeyToken=697bf476eb75315e"
   )]
    public class HandShake: Task, IDTSPersist
    {
        public string BrokeredMessage { get; set; }
        public HandShakeType Type { get; set; }

        public bool UseAsync { get; set; }

        public override DTSExecResult Execute(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents componentEvents, IDTSLogging log, object transaction)
        {
            BrokeredMessage msg = null;
            bool fireAgain = false;

            if (variableDispenser.Contains(this.BrokeredMessage))
            {
                Variables variables = (Variables)null;
                variableDispenser.LockOneForRead(this.BrokeredMessage, ref variables);

                object obj = variables[0].Value;
                if (obj == null)
                {
                    componentEvents.FireError(-1, null, "Invalid Broker Message Variable", null, 0);
                    return DTSExecResult.Failure;
                }
                else
                {
                    msg = (BrokeredMessage)obj;
                    variables.Unlock();
                }
            }

            if (msg == null)
            {
                componentEvents.FireError(-1, null, "Invalid Broker Message", null, 0);
                return DTSExecResult.Failure;
            }

            if (this.Type == HandShakeType.Complete)
            {
                if (this.UseAsync)
                    msg.CompleteAsync();
                else
                    msg.Complete();

                componentEvents.FireInformation(0, null, string.Format("Message {0} Completed", msg.MessageId), null, 0, ref fireAgain);
            }
            else
            {
                if (this.UseAsync)
                    msg.AbandonAsync();
                else
                    msg.Abandon();

                componentEvents.FireInformation(0, null, string.Format("Message {0} Abandoned", msg.MessageId), null, 0, ref fireAgain);
            }

            return DTSExecResult.Success;
        }

        #region IDTSComponentPersist Members
        public void LoadFromXML(System.Xml.XmlNode node, IDTSEvents events)
        {
            if (node.Name != "SVCBROKERHANDSHAKE")
            {
                throw new Exception("Invalid Persisted Data");
            }

            if (node.Attributes.GetNamedItem("BrokeredMessage") != null)
                this.BrokeredMessage = node.Attributes.GetNamedItem("BrokeredMessage").Value;

            if (node.Attributes.GetNamedItem("Type") != null)
                this.Type = (HandShakeType)Enum.Parse(typeof(HandShakeType), node.Attributes.GetNamedItem("Type").Value);
        }

        public void SaveToXML(ref System.Xml.XmlDocument doc, System.Xml.XmlNode node, IDTSEvents events)
        {
            XmlNode xmlNode = doc.CreateNode(XmlNodeType.Element, "SVCBROKERHANDSHAKE", "");
            
            XmlElement xmlElement = xmlNode as XmlElement;
            doc.AppendChild(node);

            xmlElement.Attributes.Append(doc.CreateAttribute("BrokeredMessage"));
            xmlElement.Attributes["BrokeredMessage"].Value = this.BrokeredMessage;

            xmlElement.Attributes.Append(doc.CreateAttribute("Type"));
            xmlElement.Attributes["Type"].Value = this.Type.ToString();
        }
        #endregion
    }

    
}
