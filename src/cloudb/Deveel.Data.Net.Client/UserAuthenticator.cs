using System;
using System.Collections.Generic;

using Deveel.Data.Configuration;

namespace Deveel.Data.Net.Security {
	public abstract class UserAuthenticator : IAuthenticator {
		private IUserDataSource dataSource;

		protected UserAuthenticator(IUserDataSource dataSource) {
			if (dataSource == null)
				throw new ArgumentNullException("dataSource");

			this.dataSource = dataSource;
		}

		protected UserAuthenticator() {
		}

		public IUserDataSource DataSource {
			get { return dataSource; }
			set { dataSource = value; }
		}

		protected abstract string GetUserName(AuthRequest authRequest);

		protected abstract string GetUserPassword(AuthRequest authRequest);

		protected virtual IDictionary<string, object> ProcessAuthData(IDictionary<string, object> authData) {
			return authData;
		}

		public virtual void Init(ConfigSource config) {
			ConfigSource dataConfig = config.GetChild("data");
			string dataSourceTypeString = dataConfig.GetString("type");
			if (String.IsNullOrEmpty(dataSourceTypeString))
				throw new InvalidOperationException("Data source type was not specified.");

			Type dataSourceType = Type.GetType(dataSourceTypeString, false, true);
			if (dataSourceType == null)
				throw new InvalidOperationException("The type '" + dataSourceTypeString + "' was not found.");

			if (!typeof(IUserDataSource).IsAssignableFrom(dataSourceType))
				throw new InvalidOperationException("The type '" + dataSourceType + "' is not a implementation of " + typeof(IUserDataSource));

			try {
				dataSource = (IUserDataSource)Activator.CreateInstance(dataSourceType, null);
			} catch (Exception e) {
				throw new InvalidOperationException("Unable to instantiate the type '" + dataSourceType + "': " + e.Message);
			}

			try {
				dataSource.Init(config);
			} catch (ConfigurationException) {
				throw;
			} catch(Exception e) {
				throw new ConfigurationException("Configuration error: " + e.Message);
			}
		}

		public AuthResult Authenticate(AuthRequest authRequest) {
			if (dataSource == null)
				return new AuthResult(false, (int) UserAuthenticationCode.UserNotFound, authRequest.AuthData);

			string userName = GetUserName(authRequest);
			if (String.IsNullOrEmpty(userName))
				return new AuthResult(false, (int) UserAuthenticationCode.UserNotFound, authRequest.AuthData);

			IUser user = dataSource.FindUser(userName);
			if (user == null)
				return new AuthResult(false, (int) UserAuthenticationCode.UserNotFound, authRequest.AuthData);

			IDictionary<string, object> authData = authRequest.AuthData;

			try {
				authData = ProcessAuthData(authData);
			} catch (Exception e) {
				return new AuthResult(false, (int) UserAuthenticationCode.UnknownError, e.Message, authData);
			}

			bool authd;
			string password = GetUserPassword(authRequest);

			try {
				authd = user.ValidatePassword(password);
			} catch (InvalidUserPasswordException) {
				return new AuthResult(false, (int)UserAuthenticationCode.InvalidPassword, "Invalid password provided.", authRequest.AuthData);
			} catch (Exception e) {
				return new AuthResult(false, (int) UserAuthenticationCode.UnknownError, e.Message, authRequest.AuthData);
			}

			UserAuthenticationCode code = (authd ? UserAuthenticationCode.Success : UserAuthenticationCode.UnknownError);
			return new AuthResult(authd, (int)code, authRequest.AuthData);
		}
	}
}
using System;
using System.Collections.Generic;

using Deveel.Data.Configuration;

namespace Deveel.Data.Net.Security {
	public abstract class UserAuthenticator : IAuthenticator {
		private IUserDataSource dataSource;

		protected UserAuthenticator(IUserDataSource dataSource) {
			if (dataSource == null)
				throw new ArgumentNullException("dataSource");

			this.dataSource = dataSource;
		}

		protected UserAuthenticator() {
		}

		public IUserDataSource DataSource {
			get { return dataSource; }
			set { dataSource = value; }
		}

		protected abstract string GetUserName(AuthRequest authRequest);

		protected abstract string GetUserPassword(AuthRequest authRequest);

		protected virtual IDictionary<string, object> ProcessAuthData(IDictionary<string, object> authData) {
			return authData;
		}

		public virtual void Configure(ConfigSource config) {
			ConfigSource dataConfig = config.GetChild("data");
			string dataSourceTypeString = dataConfig.GetString("type");
			if (String.IsNullOrEmpty(dataSourceTypeString))
				throw new InvalidOperationException("Data source type was not specified.");

			Type dataSourceType = Type.GetType(dataSourceTypeString, false, true);
			if (dataSourceType == null)
				throw new InvalidOperationException("The type '" + dataSourceTypeString + "' was not found.");

			if (!typeof(IUserDataSource).IsAssignableFrom(dataSourceType))
				throw new InvalidOperationException("The type '" + dataSourceType + "' is not a implementation of " + typeof(IUserDataSource));

			try {
				dataSource = (IUserDataSource)Activator.CreateInstance(dataSourceType, null);
			} catch (Exception e) {
				throw new InvalidOperationException("Unable to instantiate the type '" + dataSourceType + "': " + e.Message);
			}

			try {
				dataSource.Init(config);
			} catch (ConfigurationException) {
				throw;
			} catch(Exception e) {
				throw new ConfigurationException("Configuration error: " + e.Message);
			}
		}

		public AuthResult Authenticate(AuthRequest authRequest) {
			if (dataSource == null)
				return new AuthResult(false, (int) UserAuthenticationCode.UserNotFound, authRequest.AuthData);

			string userName = GetUserName(authRequest);
			if (String.IsNullOrEmpty(userName))
				return new AuthResult(false, (int) UserAuthenticationCode.UserNotFound, authRequest.AuthData);

			IUser user = dataSource.FindUser(userName);
			if (user == null)
				return new AuthResult(false, (int) UserAuthenticationCode.UserNotFound, authRequest.AuthData);

			IDictionary<string, object> authData = authRequest.AuthData;

			try {
				authData = ProcessAuthData(authData);
			} catch (Exception e) {
				return new AuthResult(false, (int) UserAuthenticationCode.UnknownError, e.Message, authData);
			}

			bool authd;
			string password = GetUserPassword(authRequest);

			try {
				authd = user.ValidatePassword(password);
			} catch (InvalidUserPasswordException) {
				return new AuthResult(false, (int)UserAuthenticationCode.InvalidPassword, "Invalid password provided.", authRequest.AuthData);
			} catch (Exception e) {
				return new AuthResult(false, (int) UserAuthenticationCode.UnknownError, e.Message, authRequest.AuthData);
			}

			UserAuthenticationCode code = (authd ? UserAuthenticationCode.Success : UserAuthenticationCode.UnknownError);
			return new AuthResult(authd, (int)code, authRequest.AuthData);
		}
	}
}