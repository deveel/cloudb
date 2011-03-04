using System;

namespace Deveel.Data.Net {
	public interface IServiceFactory {
		void Init(AdminService adminService);

		IService CreateService(IServiceAddress serviceAddress, ServiceType serviceType, IServiceConnector connector);
	}
}