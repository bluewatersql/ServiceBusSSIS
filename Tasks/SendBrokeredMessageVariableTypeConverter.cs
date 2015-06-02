using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;

namespace BluewaterSQL.DTS.ServiceBus.Tasks
{
    internal class SendBrokeredMessageVariableTypeConverter : StringConverter
    {
        private object GetSpecializedObject(object contextInstance)
        {
            return contextInstance;
        }

        public override TypeConverter.StandardValuesCollection GetStandardValues(ITypeDescriptorContext context)
        {
            object specializedObject = this.GetSpecializedObject(context.Instance);
            return new TypeConverter.StandardValuesCollection((ICollection)this.getVariables(specializedObject));
        }

        public override bool GetStandardValuesExclusive(ITypeDescriptorContext context)
        {
            return true;
        }

        public override bool GetStandardValuesSupported(ITypeDescriptorContext context)
        {
            return true;
        }

        private ArrayList getVariables(object retrievalObject)
        {
            SendBrokeredMessageGeneralView.GeneralViewNode ddlNode = (SendBrokeredMessageGeneralView.GeneralViewNode)retrievalObject;
            ddlNode.iDtsConnection.GetConnections();

            ArrayList arrayList = new ArrayList();
            arrayList.Add("New Variable");

            foreach (Variable variable in ddlNode.myTaskHost.Variables)
            {
                if (!variable.SystemVariable && !variable.ReadOnly && variable.DataType == TypeCode.Object)
                    arrayList.Add((object)variable.QualifiedName);
            }

            if (arrayList != null && arrayList.Count > 0)
                arrayList.Sort();

            return arrayList;
        }
    }
}
