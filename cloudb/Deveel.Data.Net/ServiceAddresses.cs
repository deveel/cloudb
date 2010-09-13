using System;
using System.Collections.Generic;
using System.Reflection;

namespace Deveel.Data.Net {
	internal static class ServiceAddresses {
		private static readonly object scanLock = new object();
		private static readonly Dictionary<Type, IServiceAddressHandler> handlers = new Dictionary<Type, IServiceAddressHandler>();
		private static bool scanned;
		
		private static void InspectAddressTypes() {
			if (scanned)
				return;
			
			lock(scanLock) {				
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
				
				scanned = true;
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