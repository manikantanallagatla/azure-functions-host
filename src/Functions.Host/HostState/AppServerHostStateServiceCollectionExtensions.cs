// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using Azure.Functions.Rpc.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Azure.Functions.Host.HostState;

internal static class AppServerHostStateServiceCollectionExtensions
{
    public static IServiceCollection AddHttpOnDemandWorkerScaleIn(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddHttpClient(
            AppServerHostStateClient.HttpClientName,
            client => client.Timeout = AppServerHostStateClient.RequestTimeout);
        services.TryAddSingleton(TimeProvider.System);
        services.AddSingleton<AppServerHostStateClient>();
        services.AddSingleton<IWorkerScaleInStatePublisher>(
            provider => provider.GetRequiredService<AppServerHostStateClient>());
        services.AddSingleton<RuntimeRetirementManager>();
        services.AddSingleton<IRuntimeRetirementManager>(
            provider => provider.GetRequiredService<RuntimeRetirementManager>());
        services.AddSingleton<IHostedService, HttpOnDemandWorkerScaleInService>();

        return services;
    }
}
