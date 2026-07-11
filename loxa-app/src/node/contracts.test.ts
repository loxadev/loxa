import { describe, expect, it } from "vitest";

import {
  ContractError,
  decodeModelList,
  decodeNodeStatus,
  decodeOpenAIError,
} from "./contracts";

const readyStatus = {
  node_id: "node-test",
  health: "ready",
  model: "loxa",
  engine: { name: "llama.cpp", version: "b9999" },
  runtime_model: "gemma-3-4b-it-q4",
  profile: "default",
};

const unavailableStatus = {
  node_id: "node-test",
  health: "unavailable",
  model: "loxa",
  engine: null,
  runtime_model: null,
  profile: null,
};

describe("decodeNodeStatus", () => {
  it("decodes the gateway ready fixture without coercion", () => {
    expect(decodeNodeStatus(readyStatus)).toEqual(readyStatus);
  });

  it("decodes the gateway unavailable fixture without inventing runtime values", () => {
    expect(decodeNodeStatus(unavailableStatus)).toEqual(unavailableStatus);
  });

  it.each([
    ["missing node id", { ...readyStatus, node_id: undefined }],
    ["wrong model alias", { ...readyStatus, model: "backend" }],
    ["partial engine", { ...readyStatus, engine: { name: "llama.cpp" } }],
    ["missing runtime model", { ...readyStatus, runtime_model: undefined }],
    ["missing profile", { ...readyStatus, profile: undefined }],
    ["non-object payload", []],
  ])("rejects %s", (_label, payload) => {
    expect(() => decodeNodeStatus(payload)).toThrow(ContractError);
  });
});

describe("decodeModelList", () => {
  const fixture = {
    object: "list",
    data: [{ id: "loxa", object: "model", owned_by: "loxa" }],
  };

  it("decodes the stable gateway model alias", () => {
    expect(decodeModelList(fixture)).toEqual(fixture);
  });

  it.each([
    { ...fixture, object: "collection" },
    { ...fixture, data: [] },
    { ...fixture, data: [{ ...fixture.data[0], id: "backend" }] },
    { ...fixture, data: [{ ...fixture.data[0], owned_by: 1 }] },
  ])("rejects a model list outside the gateway contract", (payload) => {
    expect(() => decodeModelList(payload)).toThrow(ContractError);
  });
});

describe("decodeOpenAIError", () => {
  it("decodes the exact OpenAI-shaped gateway error", () => {
    const fixture = {
      error: {
        message: "the managed engine is temporarily unavailable",
        type: "server_error",
        param: null,
        code: "engine_unavailable",
      },
    };

    expect(decodeOpenAIError(fixture)).toEqual(fixture.error);
  });

  it.each([
    {},
    { error: { message: 42, type: "server_error", param: null, code: "bad" } },
    { error: { message: "bad", type: "server_error", code: "bad" } },
    { error: { message: "bad", type: "server_error", param: [], code: "bad" } },
  ])("rejects malformed OpenAI errors", (payload) => {
    expect(() => decodeOpenAIError(payload)).toThrow(ContractError);
  });
});
