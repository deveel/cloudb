using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.Net.Security {
	public sealed class GuidTokenGenerator : ITokenGenerator, IConfigurable {
		private string format;

		public const string DefaultFormat = "N";

		public GuidTokenGenerator(string format) {
			if (String.IsNullOrEmpty(format))
				format = DefaultFormat;

			this.format = format;
		}

		public GuidTokenGenerator()
			: this(DefaultFormat) {
		}

		public string Format {
			get { return format; }
			set {
				if (String.IsNullOrEmpty(value))
					value = DefaultFormat;
				format = value;
			}
		}

		public IRequestToken CreateRequestToken(IConsumer consumer, OAuthParameters parameters) {
			return new OAuthRequestToken(Guid.NewGuid().ToString(Format), Guid.NewGuid().ToString(Format), consumer,
			                             TokenStatus.Unauthorized, parameters, null, new string[] {});
		}

		public IAccessToken CreateAccessToken(IConsumer consumer, IRequestToken requestToken) {
			return new OAuthAccessToken(Guid.NewGuid().ToString(Format), Guid.NewGuid().ToString(Format), consumer,
			                            TokenStatus.Unauthorized, requestToken);
		}

		public void Configure(ConfigSource configSource) {
			format = configSource.GetString("format");
			if (String.IsNullOrEmpty(format)) {
				format = DefaultFormat;
			} else if (String.Compare(format, "N", true) != 0 &&
				String.Compare(format, "D", true) != 0) {
				throw new ConfigurationException("Invalid or not supported GUID format specified", configSource, "format");
			}
		}
	}
}