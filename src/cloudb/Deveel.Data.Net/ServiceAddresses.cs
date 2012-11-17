//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

using System;
using System.Collections.Generic;
using System.Reflection;

namespace Deveel.Data.Net {
	public static class ServiceAddresses {
		private static readonly object ScanLock = new object();
		private static readonly Dictionary<Type, IServiceAddressHandler> handlers = new Dictionary<Type, IServiceAddressHandler>();
		private static bool _scanned;
		
		private static void InspectAddressTypes() {
			if (_scanned)
				return;
			
			lock(ScanLock) {				
				List<Type> addresses = new List<Type>();
				List<IServiceAddressHandler> addressHandlers = new List<IServiceAddressHandler>();
				
				Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();
				for (int i = 0; i < assemblies.Length; i++) {
					Type[] types = assemblies[i].GetTypes();
					for (int j = 0; j < types.Length; j++) {
						Type type = types[j];
						if (typeof(IServiceAddressHandler).IsAssignableFrom(type) &&
						    !type.IsAbstract &&
						    !type.Equals(typeof(IServiceAddressHandler))) {
							IServiceAddressHandler handler = (IServiceAddressHandler)Activator.CreateInstance(type);
							addressHandlers.Add(handler);
						} else if (typeof(IServiceAddress).IsAssignableFrom(type) &&
						           !type.IsAbstract &&
						           !type.Equals(typeof(IServiceAddress)))
							addresses.Add(type);
					}
				}
				
				if (addresses.Count == 0)
					return;
				
				for (int i = 0; i < addresses.Count; i++) {
					Type type = addresses[i];
					for (int j = 0; j < addressHandlers.Count; j++) {
						if (addressHandlers[j].CanHandle(type))
							handlers[type] = addressHandlers[j];
					}
				}
				
				_scanned = true;
			}
		}
		
		public static Type GetAddressType(int code) {
			InspectAddressTypes();
			
			foreach(IServiceAddressHandler handler in handlers.Values) {
				try {
					Type a = handler.GetTypeFromCode(code);
					if (a != null)
						return a;
				} catch (Exception) {
				}
			}
			
			return null;
		}
		
		public static IServiceAddressHandler GetHandler(object obj) {
			if (obj == null)
				throw new ArgumentNullException("obj");
			
			if (!(obj is IServiceAddress))
				throw new ArgumentException("The given object is not a " + typeof(IServiceAddress) + ".");
			
			return GetHandler(obj.GetType());
		}
		
		public static IServiceAddressHandler GetHandler(Type type) {
			if (type == null)
				throw new ArgumentNullException("type");
			if (!typeof(IServiceAddress).IsAssignableFrom(type))
				throw new ArgumentException("The type '" + type + "' is not a " + typeof(IServiceAddress) + ".");
			
			InspectAddressTypes();
			
			IServiceAddressHandler handler;
			if (handlers.TryGetValue(type, out handler))
				return handler;
			
			return null;
		}
		
		public static IServiceAddressHandler GetHandler<T>() where T : IServiceAddress {
			return GetHandler(typeof(T));
		}
		
		public static IServiceAddress ParseString(string s) {
			InspectAddressTypes();

			foreach(IServiceAddressHandler handler in handlers.Values) {
				try {
					IServiceAddress a = handler.FromString(s);
					if (a != null)
						return a;
				} catch (Exception) {
				}
			}
			
			throw new ArgumentException("The string '" + s + "' is not of a recognized format.");
		}
	}
}