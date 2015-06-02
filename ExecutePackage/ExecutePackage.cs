using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Management.IntegrationServices;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Xml;

//http://muxtonmumbles.blogspot.com/2012/08/programmatically-executing-packages-in.html
//http://muxtonmumbles.blogspot.co.uk/2013/09/specifying-timeout-for-ssis-2012.html

namespace BluewaterSQL.DTS.Tasks
{
    /*
     [DtsTask(DisplayName = "Execute Package",
        IconResource = "BluewaterSQL.DTS.Tasks.Task.ico",
        UITypeName = "BluewaterSQL.DTS.Tasks.ExecutePackageUI, BluewaterSQL.DTS.Tasks, Version=1.0.0.0, Culture=Neutral, PublicKeyToken=6896b5f31ecde1d9")]
     */
    [DtsTask(DisplayName = "Execute Package",
        IconResource = "BluewaterSQL.DTS.Tasks.Task.ico")]
    public class ExecutePackage : Task, IDTSPersist
    {
        #region Properties
        public string Server { get; set; }
        public string Folder { get; set; }
        public string Catalog { get; set; }
        public string Project { get; set; }
        public string Package { get; set; }
        public bool Use32bitRuntime { get; set; }
        public string Parameters { get; set; }
        public int CommandTimeOut { get; set; }
        public int PollingInterval { get; set; }
        public bool RunAsync { get; set; }
        #endregion

        #region Methods
        public override DTSExecResult Validate(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents componentEvents, IDTSLogging log)
        {
            bool valid = true;
            bool fireAgain = false;

            if (string.IsNullOrEmpty(this.Server))
            {
                componentEvents.FireError(0, "ExecutePackage", "Server cannot be empty", null, 0);
                valid = false;
            }

            if (string.IsNullOrEmpty(this.Folder))
            {
                componentEvents.FireError(0, "ExecutePackage", "Folder cannot be empty", null, 0);
                valid = false;
            }

            if (string.IsNullOrEmpty(this.Project))
            {
                componentEvents.FireError(0, "ExecutePackage", "Project cannot be empty", null, 0);
                valid = false;
            }

            if (string.IsNullOrEmpty(this.Package))
            {
                componentEvents.FireError(0, "ExecutePackage", "Package cannot be empty", null, 0);
                valid = false;
            }

            if (string.IsNullOrEmpty(this.Catalog))
            {
                this.Catalog = "SSISDB";
                componentEvents.FireInformation(0, "ExecutePackage", "Catalog set to default (SSISDB).", null, 0, ref fireAgain);
            }

            if (!this.RunAsync)
            {
                if (this.PollingInterval == 0)
                {
                    this.PollingInterval = 10;
                    componentEvents.FireInformation(0, "ExecutePackage", "PollingInterval set to default (5 seconds).", null, 0, ref fireAgain);
                }
            }

            return (valid == true) ? DTSExecResult.Success : DTSExecResult.Failure;
        }

        public override DTSExecResult Execute(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents componentEvents, IDTSLogging log, object transaction)
        {
            ExecutePackageWorker worker = new ExecutePackageWorker();

            worker.ConnectionString = "";
            worker.Catalog = this.Catalog;
            worker.Folder = this.Folder;
            worker.Project = this.Project;
            worker.Package = this.Package;
            worker.Use32bitRuntime = this.Use32bitRuntime;

            string parameterJSON = "[{\"Name\":\"FailPackage\",\"Type\":30,\"Value\":false},{\"Name\":\"Timeout\",\"Type\":30,\"Value\":5}]";
            this.Parameters = parameterJSON;

            if (!string.IsNullOrEmpty(this.Parameters))
            {
                using (var ms = new MemoryStream())
                {
                    using (StreamWriter sw = new StreamWriter(ms))
                    {
                        sw.WriteLine(this.Parameters);
                        sw.Flush();

                        ms.Position = 0;

                        DataContractJsonSerializer ser = new DataContractJsonSerializer(typeof(Parameter[]));
                        Parameter[] p = (Parameter[])ser.ReadObject(ms);

                        worker.Parameters = p;
                    }
                }
            }

            Operation.ServerOperationStatus status = worker.ExecutePackage();

            switch (status)
            {
                case (Operation.ServerOperationStatus.Success):
                case (Operation.ServerOperationStatus.Completion):
                    return DTSExecResult.Success;
                default:
                    return DTSExecResult.Failure;
            }
        }
        #endregion

        #region IDTSComponentPersist Members
        public void LoadFromXML(System.Xml.XmlNode node, IDTSEvents events)
        {
            if (node.Name != "BLUEWATEREXECUTEPKG")
            {
                throw new Exception("Invalid Persisted Data");
            }

            if (node.Attributes.GetNamedItem("Server") != null)
                this.Server = node.Attributes.GetNamedItem("Server").Value;

            if (node.Attributes.GetNamedItem("Folder") != null)
                this.Folder = node.Attributes.GetNamedItem("Folder").Value;

            if (node.Attributes.GetNamedItem("Catalog") != null)
                this.Catalog = node.Attributes.GetNamedItem("Catalog").Value;

            if (node.Attributes.GetNamedItem("Project") != null)
                this.Project = node.Attributes.GetNamedItem("Project").Value;

            if (node.Attributes.GetNamedItem("Package") != null)
                this.Package = node.Attributes.GetNamedItem("Package").Value;

            if (node.Attributes.GetNamedItem("Parameters") != null)
                this.Parameters = node.Attributes.GetNamedItem("Parameters").Value;

            if (node.Attributes.GetNamedItem("Use32bitRuntime") != null)
                this.Use32bitRuntime = Convert.ToBoolean(node.Attributes.GetNamedItem("Use32bitRuntime").Value);

            if (node.Attributes.GetNamedItem("RunAsync") != null)
                this.RunAsync = Convert.ToBoolean(node.Attributes.GetNamedItem("RunAsync").Value);

            if (node.Attributes.GetNamedItem("CommandTimeOut") != null)
                this.CommandTimeOut = Convert.ToInt32(node.Attributes.GetNamedItem("CommandTimeOut").Value);

            if (node.Attributes.GetNamedItem("PollingInterval") != null)
                this.PollingInterval = Convert.ToInt32(node.Attributes.GetNamedItem("PollingInterval").Value);
        }

        public void SaveToXML(ref System.Xml.XmlDocument doc, System.Xml.XmlNode node, IDTSEvents events)
        {
            XmlNode xmlNode = doc.CreateNode(XmlNodeType.Element, "BLUEWATEREXECUTEPKG", "");

            XmlElement xmlElement = xmlNode as XmlElement;
            doc.AppendChild(node);

            xmlElement.Attributes.Append(doc.CreateAttribute("Server"));
            xmlElement.Attributes["Server"].Value = this.Server;

            xmlElement.Attributes.Append(doc.CreateAttribute("Folder"));
            xmlElement.Attributes["Folder"].Value = this.Folder;

            xmlElement.Attributes.Append(doc.CreateAttribute("Project"));
            xmlElement.Attributes["Project"].Value = this.Project;

            xmlElement.Attributes.Append(doc.CreateAttribute("Package"));
            xmlElement.Attributes["Package"].Value = this.Package;

            xmlElement.Attributes.Append(doc.CreateAttribute("Catalog"));
            xmlElement.Attributes["Catalog"].Value = this.Catalog;

            xmlElement.Attributes.Append(doc.CreateAttribute("Parameters"));
            xmlElement.Attributes["Parameters"].Value = this.Parameters;

            xmlElement.Attributes.Append(doc.CreateAttribute("Use32bitRuntime"));
            xmlElement.Attributes["Use32bitRuntime"].Value = this.Use32bitRuntime.ToString();

            xmlElement.Attributes.Append(doc.CreateAttribute("RunAsync"));
            xmlElement.Attributes["RunAsync"].Value = this.RunAsync.ToString();

            xmlElement.Attributes.Append(doc.CreateAttribute("CommandTimeOut"));
            xmlElement.Attributes["CommandTimeOut"].Value = this.CommandTimeOut.ToString();

            xmlElement.Attributes.Append(doc.CreateAttribute("PollingInterval"));
            xmlElement.Attributes["PollingInterval"].Value = this.PollingInterval.ToString();
        }
        #endregion
    }
}
