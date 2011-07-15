using System;
using System.Reflection;

namespace Deveel.Data.Net.Client {
	//TODO:
	public sealed class EmbeddedPathClient : IPathClient {
		private readonly string assemblyName;
		private readonly string pathName;

		private readonly Assembly assembly;
		private readonly Type type;
		private readonly ConstructorInfo ctor;
		private readonly int ctorType;

		private object obj;
		private bool disposed;

		public EmbeddedPathClient(string assemblyName, string pathName) {
			if (String.IsNullOrEmpty(assemblyName))
				throw new ArgumentNullException("assemblyName");
			if (String.IsNullOrEmpty(pathName))
				throw new ArgumentNullException("pathName");

			this.assemblyName = assemblyName;
			this.pathName = pathName;

			assembly = Assembly.Load(assemblyName);
			type = FindType();

			if (type == null)
				throw new ArgumentException("Unable to find a valid type to initialize a context.");

			ctor = FindConstructor(out ctorType);
			if (ctor == null)
				throw new ArgumentException("The type '' does not define any valid constructor.");
		}

		~EmbeddedPathClient() {
			Dispose(false);
		}

		public Type Type {
			get { return type; }
		}

		private static bool InterfaceFilter(Type type, object criteria) {
			return type.Name == "IPathContext";
		}

		private Type FindType() {
			Type candidate = null;

			Type[] types = assembly.GetTypes();
			for (int i = 0; i < types.Length; i++) {
				Type t = types[i];
				Type[] interfaces = t.FindInterfaces(InterfaceFilter, null);
				if (interfaces.Length == 0)
					continue;

				candidate = t;
			}
			return candidate;
		}

		private ConstructorInfo FindConstructor(out int ctorType) {
			ctorType = -1;

			ConstructorInfo[] ctors = type.GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
			if (ctors.Length == 0)
				return null;

			ConstructorInfo candidate = null;

			for (int i = 0; i < ctors.Length; i++) {
				candidate = ctors[i];
				if (!IsValidConstructor(candidate, out ctorType)) {
					candidate = null;
					continue;
				}
			}

			return candidate;
		}

		private bool IsValidConstructor(ConstructorInfo ctor, out int ctorType) {
			throw new NotImplementedException();
		}

		private void Dispose(bool disposing) {
			if (disposed)
				return;

			if (disposing) {
				if (obj != null && obj is IDisposable) {
					(obj as IDisposable).Dispose();
					obj = null;
				}
			}

			disposed = true;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public string PathName {
			get { return pathName; }
		}

		public ClientState State {
			get { throw new NotImplementedException(); }
		}

		public void Open() {
			throw new NotImplementedException();
		}

		public void Close() {
			throw new NotImplementedException();
		}

		public IPathTransaction BeginTransaction() {
			throw new NotImplementedException();
		}
	}
}