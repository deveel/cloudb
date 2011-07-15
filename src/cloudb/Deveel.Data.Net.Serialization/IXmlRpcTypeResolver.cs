using System;
using System.Xml;

namespace Deveel.Data.Net.Serialization {
	public interface IXmlRpcTypeResolver {
		Type ResolveType(string elementName);
		
		string ResolveElementName(Type type);
		
		void WriteValue(XmlWriter xmlWriter, string elementName, object value, string format);
		
		object ReadValue(XmlReader reader, Type type);
	}
}