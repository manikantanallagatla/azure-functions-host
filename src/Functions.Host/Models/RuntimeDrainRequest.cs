// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Azure.Functions.Host.RuntimeDrain;

/// <summary>
/// Identifies the Host-local scheduling policy for an app-version Runtime drain.
/// </summary>
public enum RuntimeDrainMode
{
    /// <summary>
    /// Selects Workers in phased non-HTTP batches with a delayed HTTP selection.
    /// </summary>
    Phased,

    /// <summary>
    /// Selects every remaining eligible Worker immediately.
    /// </summary>
    Immediate,
}

/// <summary>
/// Identifies the external reason for an app-version Runtime drain.
/// </summary>
public enum RuntimeDrainReason
{
    /// <summary>
    /// The app configuration changed.
    /// </summary>
    ConfigurationChange,

    /// <summary>
    /// The app is stopping.
    /// </summary>
    Stop,

    /// <summary>
    /// The app is being deleted.
    /// </summary>
    Delete,

    /// <summary>
    /// The app is restarting.
    /// </summary>
    Restart,
}

/// <summary>
/// Requests one app-version Runtime drain operation.
/// </summary>
[JsonConverter(typeof(RuntimeDrainRequestJsonConverter))]
public sealed class RuntimeDrainRequest
{
    /// <summary>
    /// Gets or sets the caller-owned idempotency identifier.
    /// </summary>
    [JsonProperty("operationId")]
    public Guid? OperationId { get; set; }

    /// <summary>
    /// Gets or sets the string drain mode.
    /// </summary>
    [JsonProperty("mode")]
    public string? Mode { get; set; }

    /// <summary>
    /// Gets or sets the string drain reason.
    /// </summary>
    [JsonProperty("reason")]
    public string? Reason { get; set; }
}

internal sealed class RuntimeDrainRequestJsonConverter : JsonConverter
{
    private const string ModePropertyName = "mode";
    private const string OperationIdPropertyName = "operationId";
    private const string ReasonPropertyName = "reason";

    public override bool CanWrite => false;

    public override bool CanConvert(Type objectType)
        => objectType == typeof(RuntimeDrainRequest);

    public override object? ReadJson(
        JsonReader reader,
        Type objectType,
        object? existingValue,
        JsonSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(reader);
        JObject body = JObject.Load(
            reader,
            new JsonLoadSettings
            {
                DuplicatePropertyNameHandling = DuplicatePropertyNameHandling.Error,
            });

        if (body.Count != 3 ||
            body.Property(OperationIdPropertyName, StringComparison.Ordinal) is not { Value.Type: JTokenType.String } operationIdProperty ||
            body.Property(ModePropertyName, StringComparison.Ordinal) is not { Value.Type: JTokenType.String } modeProperty ||
            body.Property(ReasonPropertyName, StringComparison.Ordinal) is not { Value.Type: JTokenType.String } reasonProperty ||
            !Guid.TryParse(operationIdProperty.Value.Value<string>(), out Guid operationId))
        {
            throw new JsonSerializationException(
                "The Runtime drain request must contain exactly operationId, mode, and reason.");
        }

        return new RuntimeDrainRequest
        {
            OperationId = operationId,
            Mode = modeProperty.Value.Value<string>(),
            Reason = reasonProperty.Value.Value<string>(),
        };
    }

    public override void WriteJson(JsonWriter writer, object? value, JsonSerializer serializer)
        => throw new NotSupportedException();
}

/// <summary>
/// Confirms irreversible Host admission closure and the AppServer lifecycle fence.
/// </summary>
/// <param name="OperationId">The accepted caller-owned operation identifier.</param>
/// <param name="Mode">The accepted canonical drain mode.</param>
/// <param name="AcceptedAtUtc">The Host-authoritative UTC acceptance time.</param>
public sealed record RuntimeDrainReceipt(
    [property: JsonProperty("operationId")] Guid OperationId,
    [property: JsonProperty("mode")] string Mode,
    [property: JsonProperty("acceptedAtUtc")] DateTimeOffset AcceptedAtUtc);

/// <summary>
/// Describes an app-version Runtime drain failure.
/// </summary>
/// <param name="Code">The stable machine-readable failure code.</param>
/// <param name="Detail">A safe diagnostic description.</param>
public sealed record RuntimeDrainError(
    [property: JsonProperty("code")] string Code,
    [property: JsonProperty("detail")] string Detail);

/// <summary>
/// Contains an app-version Runtime drain failure.
/// </summary>
/// <param name="Error">The failure code and diagnostic detail.</param>
public sealed record RuntimeDrainErrorResponse(
    [property: JsonProperty("error")] RuntimeDrainError Error);
