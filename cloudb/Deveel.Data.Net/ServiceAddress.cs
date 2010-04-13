using System;
using System.IO;
using System.Net;
using System.Text;

namespace Deveel.Data.Net {
	public sealed class ServiceAddress : IComparable<ServiceAddress> {
		public ServiceAddress(byte[] address, int port) {
			if (address.Length != 16) {
				throw new ArgumentException("Address must be a 16 byte IPv6 format.", "address");
			}
			this.address = (byte[])address.Clone();
			this.port = port;
		}

		public ServiceAddress(IPAddress address, int port) {
			this.address = new byte[16];
			this.port = port;
			if (IPAddress.IsLoopback(address))
				address = Dns.GetHostEntry(address).AddressList[0];
			byte[] b = address.GetAddressBytes();
			// If the address is ipv4,
			if (b.Length == 4) {
				// Format the network address as an 16 byte ipv6 on ipv4 network address.
				//net_address[10] = (byte)0x0FF;
				//net_address[11] = (byte)0x0FF;
				for (int i = 0; i < 11; i++)
					this.address[i] = 0;
				/*
				if (IPAddress.IsLoopback(inet_address)) {
					net_address[12] = 0;
					net_address[13] = 0;
					net_address[14] = 0;
					net_address[15] = 1;
				} else {
				*/
				this.address[12] = b[0];
				this.address[13] = b[1];
				this.address[14] = b[2];
				this.address[15] = b[3];
				//}
			}
				// If the address is ipv6
			else if (b.Length == 16) {
				for (int i = 0; i < 16; ++i) {
					this.address[i] = b[i];
				}
			} else {
				// Some future inet_address format?
				throw new ArgumentException("Invalid IP address format");
			}
		}

		private readonly byte[] address;
		private readonly int port;

		public byte[] Address {
			get { return (byte[]) address.Clone(); }
		}

		public int Port {
			get { return port; }
		}

		#region Implementation of IComparable<ServiceAddress>

		public int CompareTo(ServiceAddress other) {
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
			ServiceAddress dest_ob = (ServiceAddress)obj;
			if (port != dest_ob.port)
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

		internal static ServiceAddress ReadFrom(BinaryReader input) {
			byte[] buf = new byte[16];
			for (int i = 0; i < 16; ++i)
				buf[i] = input.ReadByte();
			int port = input.ReadInt32();
			return new ServiceAddress(buf, port);
		}

		internal void WriteTo(BinaryWriter output) {
			for (int i = 0; i < 16; ++i) {
				output.Write(address[i]);
			}
			output.Write(port);
		}

		public static ServiceAddress Parse(string s) {
			int p = s.LastIndexOf(":");
			if (p == -1)
				throw new FormatException("Invalid format for the input string: " + s);

			string serviceAddr = s.Substring(0, p);
			string servicePort = s.Substring(p + 1);

			int port;
			if (!Int32.TryParse(servicePort, out port))
				throw new FormatException("The port number is invalid.");

			IPAddress ipAddress;

			try {
				ipAddress = Dns.GetHostAddresses(serviceAddr)[0];
			} catch(Exception) {
				throw new FormatException("Unable to resolve the address '" + serviceAddr + "'.");
			}

			return new ServiceAddress(ipAddress, port);
		}
	}
}