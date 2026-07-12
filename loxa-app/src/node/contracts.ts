export type NodeStatus = {
  node_id: string;
  health: "ready" | "unavailable";
  model: "loxa";
  engine: { name: string; version: string } | null;
  runtime_model: string | null;
  profile: string | null;
};

export type ModelList = {
  object: "list";
  data: [{ id: "loxa"; object: "model"; owned_by: string }];
};

export type OpenAIError = {
  message: string;
  type: string;
  param: string | null;
  code: string;
};

export class ContractError extends Error {}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNullableString(value: unknown): value is string | null {
  return typeof value === "string" || value === null;
}

function isEngine(
  value: unknown,
): value is { name: string; version: string } {
  return (
    isRecord(value) &&
    typeof value.name === "string" &&
    typeof value.version === "string"
  );
}

function invalid(contract: string): never {
  throw new ContractError(`invalid ${contract}`);
}

export function decodeNodeStatus(value: unknown): NodeStatus {
  if (
    !isRecord(value) ||
    typeof value.node_id !== "string" ||
    (value.health !== "ready" && value.health !== "unavailable") ||
    value.model !== "loxa" ||
    !isNullableString(value.runtime_model) ||
    !isNullableString(value.profile)
  ) {
    return invalid("node status");
  }

  const engine = value.engine;
  if (engine !== null && !isEngine(engine)) {
    return invalid("node status");
  }

  const runtimeFieldsReady =
    isEngine(engine) &&
    typeof value.runtime_model === "string" &&
    typeof value.profile === "string";
  const runtimeFieldsUnavailable =
    engine === null && value.runtime_model === null && value.profile === null;
  if (
    (value.health === "ready" && !runtimeFieldsReady) ||
    (value.health === "unavailable" && !runtimeFieldsUnavailable)
  ) {
    return invalid("node status");
  }

  return {
    node_id: value.node_id,
    health: value.health,
    model: value.model,
    engine:
      engine === null ? null : { name: engine.name, version: engine.version },
    runtime_model: value.runtime_model,
    profile: value.profile,
  };
}

export function decodeModelList(value: unknown): ModelList {
  if (
    !isRecord(value) ||
    value.object !== "list" ||
    !Array.isArray(value.data) ||
    value.data.length !== 1
  ) {
    return invalid("model list");
  }
  const model = value.data[0];
  if (
    !isRecord(model) ||
    model.id !== "loxa" ||
    model.object !== "model" ||
    typeof model.owned_by !== "string"
  ) {
    return invalid("model list");
  }
  return {
    object: "list",
    data: [{ id: "loxa", object: "model", owned_by: model.owned_by }],
  };
}

export function decodeOpenAIError(value: unknown): OpenAIError {
  const error = isRecord(value) ? value.error : undefined;
  if (
    !isRecord(error) ||
    typeof error.message !== "string" ||
    typeof error.type !== "string" ||
    !(typeof error.param === "string" || error.param === null) ||
    typeof error.code !== "string"
  ) {
    return invalid("OpenAI error");
  }
  return {
    message: error.message,
    type: error.type,
    param: error.param,
    code: error.code,
  };
}
