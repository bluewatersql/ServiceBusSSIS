using Microsoft.SqlServer.Management.IntegrationServices;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BluewaterSQL.DTS.Tasks
{
    public class ExecutePackageWorker
    {
        #region Constructor
        public ExecutePackageWorker()
        {
        }
        #endregion

        #region Members
        public int ExecutionID { get; private set; }
        public Parameter[] Parameters { get; set; }
        public string ConnectionString { get; set; }
        public string Folder { get; set; }
        public string Catalog { get; set; }
        public string Project { get; set;  }
        public string Package { get; set; }
        public bool Use32bitRuntime { get; set; }
        public string Environment { get; set; }
        public int CommandTimeOut { get; set; }
        public int PollingInterval { get; set; }
        public bool RunAsync { get; set; }
        #endregion

        public Operation.ServerOperationStatus ExecutePackage()
        {
            Task<Operation.ServerOperationStatus> exePkg = Task.Factory.StartNew<Operation.ServerOperationStatus>(() => RunPackage());
            exePkg.Wait();

            return exePkg.Result;
        }

        private Operation.ServerOperationStatus RunPackage()
        {
            Collection<PackageInfo.ExecutionValueParameterSet> executionParams = new Collection<PackageInfo.ExecutionValueParameterSet>();

            if (this.Parameters != null && this.Parameters.Length > 0)
            {
                foreach (var p in this.Parameters)
                {
                    executionParams.Add(new PackageInfo.ExecutionValueParameterSet()
                    {
                        ParameterName = p.Name,
                        ParameterValue = p.Value,
                        ObjectType = (short)p.Type
                    });
                }
            }

            Operation.ServerOperationStatus status = Operation.ServerOperationStatus.Created;

            using (SqlConnection connection = new SqlConnection(this.ConnectionString))
            {
                var ssis = new IntegrationServices(connection);

                if (ssis.Catalogs.Contains(this.Catalog))
                {
                    var catalog = ssis.Catalogs[this.Catalog];

                    if (catalog.Folders.Contains(this.Folder))
                    {
                        var folder = catalog.Folders[this.Folder];

                        if (folder.Projects.Contains(this.Project))
                        {
                            var project = folder.Projects[this.Project];

                            if (project.Packages.Contains(this.Package))
                            {
                                EnvironmentReference environmentRef = null;

                                if (!string.IsNullOrEmpty(this.Environment) && project.References.Contains(this.Environment, folder.Name))
                                {
                                    environmentRef  = project.References[this.Environment, folder.Name];
                                }

                                var package = project.Packages[this.Package];
                                
                                long executionId = package.Execute(this.Use32bitRuntime, environmentRef, executionParams, null);

                                if (!this.RunAsync)
                                {
                                    var execution = catalog.Executions[executionId];

                                    while (!execution.Completed)
                                    {
                                        execution.Refresh();
                                        Thread.Sleep(this.PollingInterval);
                                    }

                                    status = execution.Status;
                                }
                                else
                                {
                                    status = Operation.ServerOperationStatus.Success;
                                }
                            }
                        }
                    }
                }
            }

            return status;
        } 
    }
}
