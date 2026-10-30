// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;
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
        services.AddSingleton<RuntimeRetirementStatePublisher>();
        services.AddSingleton<IHostedService, RuntimeRetirementStatePublisherHostedService>();
        services.AddSingleton<RuntimeRetirementManager>();
        services.AddSingleton<IRuntimeRetirementManager>(
            provider => provider.GetRequiredService<RuntimeRetirementManager>());
        services.AddSingleton<AppVersionRuntimeDrainManager>();
        services.AddSingleton<IAppVersionRuntimeDrainManager>(
            provider => provider.GetRequiredService<AppVersionRuntimeDrainManager>());
        services.AddSingleton<IHostedService, HttpOnDemandWorkerScaleInService>();

        return services;
    }
}

internal sealed class RuntimeRetirementStatePublisherHostedService(
    RuntimeRetirementStatePublisher publisher) : IHostedService
{
    private readonly RuntimeRetirementStatePublisher _publisher =
        publisher ?? throw new ArgumentNullException(nameof(publisher));

    public Task StartAsync(CancellationToken cancellationToken) =>
        _publisher.StartAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken) =>
        _publisher.StopAsync(cancellationToken);
}
