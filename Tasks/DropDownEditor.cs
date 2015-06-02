using Microsoft.DataTransformationServices.Controls;
using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing.Design;
using System.Linq;
using System.Reflection;
using System.Security.Permissions;
using System.Text;
using System.Windows.Forms;
using System.Windows.Forms.Design;

namespace BluewaterSQL.DTS.ServiceBus.Tasks
{
    [PermissionSet(SecurityAction.LinkDemand, Name = "FullTrust")]
    [PermissionSet(SecurityAction.InheritanceDemand, Name = "FullTrust")]
    internal class DropDownEditor : UITypeEditor
    {
        private const string OLAPTYPE = "MSOLAP100";
        protected ListBoxEditor listBoxEditor;

        protected object GetSpecializedObject(object contextInstance)
        {
            if (contextInstance == null)
                return (object)null;
            if (contextInstance is DtsContainer)
                return contextInstance;
            PropertyInfo property = contextInstance.GetType().GetProperty("BrowsableObject");
            if (property == (PropertyInfo)null)
                return (object)null;
            MethodInfo getMethod = property.GetGetMethod(true);
            if (getMethod == (MethodInfo)null)
                return (object)null;
            try
            {
                object obj = getMethod.Invoke(contextInstance, new object[0]);
                if (obj is DtsContainer)
                    return obj;
            }
            catch
            {
            }
            return (object)null;
        }

        public override object EditValue(ITypeDescriptorContext context, IServiceProvider provider, object currentValue)
        {
            object obj = currentValue;
            if (provider != null)
            {
                IWindowsFormsEditorService edSvc = (IWindowsFormsEditorService)provider.GetService(typeof(IWindowsFormsEditorService));
                if (edSvc != null)
                {
                    object specializedObject = this.GetSpecializedObject(context.Instance);
                    if (specializedObject == null)
                        return obj;
                    ArrayList listBoxArray = this.GetListBoxArray(specializedObject, context.PropertyDescriptor.Name);
                    if (listBoxArray.Count == 0)
                        return obj;
                    if (this.listBoxEditor == null)
                        this.listBoxEditor = new ListBoxEditor();
                    this.listBoxEditor.Initialize(listBoxArray);
                    this.listBoxEditor.Start(edSvc, obj);
                    edSvc.DropDownControl((Control)this.listBoxEditor);
                    obj = this.listBoxEditor.Value;
                    this.listBoxEditor.End();
                }
            }
            return obj;
        }

        private ArrayList GetListBoxArray(object retrievalObject, string elementName)
        {
            ArrayList arrayList = new ArrayList();
            if (elementName.CompareTo("ConnectionName") == 0)
            {
                Connections connections = this.GetConnections(retrievalObject);
                if (connections != null)
                {
                    foreach (var connectionManager in connections)
                    {
                        if (connectionManager.CreationName == "AZURESVCBUS")
                            arrayList.Add((object)connectionManager.Name);
                    }
                }
            }
            else if (elementName.CompareTo("VariableName") == 0)
            {
                Variables variables = this.GetVariables(retrievalObject);
                if (variables != null)
                {
                    foreach (Variable variable in variables)
                    {
                        if (!variable.SystemVariable && !variable.ReadOnly)
                            arrayList.Add((object)variable.QualifiedName);
                    }
                }
            }
            return arrayList;
        }

        public override UITypeEditorEditStyle GetEditStyle(ITypeDescriptorContext context)
        {
            return UITypeEditorEditStyle.DropDown;
        }

        private Variables GetVariables(object retrivalObject)
        {
            DtsContainer dtsContainer = retrivalObject as DtsContainer;
            if ((DtsObject)dtsContainer != (DtsObject)null)
                return dtsContainer.Variables;
            else
                return (Variables)null;
        }

        private Connections GetConnections(object retrivalObject)
        {
            DtsContainer dtsContainer = retrivalObject as DtsContainer;
            if ((DtsObject)dtsContainer == (DtsObject)null)
                return (Connections)null;
            while ((DtsObject)dtsContainer != (DtsObject)null && !(dtsContainer is Package))
                dtsContainer = dtsContainer.Parent;
            if (dtsContainer is Package)
                return ((Package)dtsContainer).Connections;
            else
                return (Connections)null;
        }
    }
}
