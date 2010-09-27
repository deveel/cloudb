using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Deveel.Data.Net {
	public sealed class TcpServiceAddress : IServiceAddress {
		public TcpServiceAddress(byte[] address, int port)
			: this(new IPAddress(address), port) {
			if (address.Length != 16)
				throw new ArgumentException("Address must be a 16 byte IPv6 format.", "address");			
		}
		
		public TcpServiceAddress(string address, int port)
			: this(IPAddress.Parse(address), port) {
		}

		public TcpServiceAddress(IPAddress address, int port) {
			if (address.AddressFamily != AddressFamily.InterNetwork &&
			    address.AddressFamily != AddressFamily.InterNetworkV6)
				throw new ArgumentException("Only IPv4 and IPv6 addresses are permitted", "address");
			
			this.port = port;
			family = address.AddressFamily;
			
			if (IPAddress.IsLoopback(address))
				address = Dns.GetHostEntry(address).AddressList[0];
			
			byte[] b = address.GetAddressBytes();
			this.address = (byte[])b.Clone();
		}
		
		public TcpServiceAddress(byte[] address)
			: this(address, DefaultPort) {
		}
		
		public TcpServiceAddress(IPAddress address)
			: this(address, DefaultPort) {
		}
		
		public TcpServiceAddress(string address)
			: this(address, DefaultPort) {
		}
		
		private AddressFamily family;
		private byte[] address;
		private int port;
		
		public const int DefaultPort = 1058;

		public byte[] Address {
			get { return (byte[]) address.Clone(); }
		}

		public int Port {
			get { return port; }
		}
		
		internal bool IsIPv4 {
			get { return address.Length == 4; }
		}
		
		internal bool IsIPv6 {
			get { return address.Length == 16; }
		}

		#region Implementation of IComparable<IServiceAddress>
		
		int IComparable<IServiceAddress>.CompareTo(IServiceAddress other) {
			return CompareTo((TcpServiceAddress)other);
		}

		public int CompareTo(TcpServiceAddress other) {
			if (family != other.family)
				throw new ArgumentException("The given address is not of the same family of this address.");
			
			for (int i = 0; i < address.Length; ++i) {
				byte dbi = other.address[i];
				byte sbi = address[i];
				if (other.address[i] != address[i]) {
					return sbi - dbi;
				}
			}
			return port - other.port;
		}

		#endregion

		public override bool Equals(object obj) {
			TcpServiceAddress dest_ob = (TcpServiceAddress)obj;
			if (port != dest_ob.port)
				return false;
			if (family != dest_ob.family)
				return false;

			for (int i = 0; i < address.Length; ++i) {
				if (dest_ob.address[i] != address[i])
					return false;
			}

			return true;
		}

		public override int GetHashCode() {
			int v = 0;
			for (int i = 0; i < address.Length; ++i)
				v = v + address[i];
			v = v + port;
			return v;
		}

		public IPAddress ToIPAddress() {
			try {
				return new IPAddress(address);
			} catch (Exception e) {
				// It should not be possible for this exception to be generated since
				// the API should have no need to look up a naming database (it's an
				// IP address!)
				throw new FormatException(e.Message, e);
			}
		}

		public IPEndPoint ToEndPoint() {
			try {
				return new IPEndPoint(ToIPAddress(), port);
			} catch (Exception e) {
				throw new FormatException(e.Message, e);
			}
		}

		public override string ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append(ToIPAddress().ToString());
			buf.Append(":");
			buf.Append(port);
			return buf.ToString();
		}

		public static TcpServiceAddress Parse(string s) {
			int p = s.LastIndexOf(":");
			if (p == -1)
				throw new FormatException("Invalid format for the input string: " + s);

			string serviceAddr = s.Substring(0, p);
			string servicePort = s.Substring(p + 1);

			int port;
			if (!Int32.TryParse(servicePort, out port))
				throw new FormatException("The port number is invalid.");

			IPAddress ipAddress = null;

			try {
				ipAddress = IPAddress.Parse(serviceAddr);
			} catch(Exception) {
				throw new FormatException("Unable to resolve the address '" + serviceAddr + "'.");
			}
			
			if (ipAddress == null)
				throw new FormatException("Unable to resolve the address '" + serviceAddr + "'.");

			return new TcpServiceAddress(ipAddress, port);
		}
	}
}