import Foundation
import XCTest

@testable import LoxaMLXCore

final class GenerationTypesTests: XCTestCase {
    func testSidecarVersionMatchesRustAdapterContract() {
        XCTAssertEqual(LoxaMLXVersion.current, "0.1.0")
    }

    func testRequestRejectsNonPositiveMaxTokens() throws {
        XCTAssertThrowsError(
            try GenerationRequest(
                requestID: "request-zero",
                prompt: "Hello",
                temperature: 0,
                maxTokens: 0
            )
        )

        XCTAssertThrowsError(
            try GenerationRequest(
                requestID: "request-negative",
                prompt: "Hello",
                temperature: 0,
                maxTokens: -1
            )
        )
    }

    func testRequestEncodesExactSnakeCaseWireFields() throws {
        let request = try GenerationRequest(
            requestID: "request-1",
            prompt: "Hello",
            temperature: 0.25,
            maxTokens: 8
        )

        let object = try XCTUnwrap(
            JSONSerialization.jsonObject(with: JSONEncoder().encode(request))
                as? [String: Any]
        )

        XCTAssertEqual(Set(object.keys), ["request_id", "prompt", "temperature", "max_tokens"])
        XCTAssertEqual(object["request_id"] as? String, "request-1")
        XCTAssertEqual(object["prompt"] as? String, "Hello")
        XCTAssertEqual(object["temperature"] as? Double, 0.25)
        XCTAssertEqual(object["max_tokens"] as? Int, 8)
    }

    func testRequestDecodesExactSnakeCaseWireFields() throws {
        let data = Data(
            #"{"request_id":"request-2","prompt":"Hi","temperature":0,"max_tokens":4}"#.utf8
        )

        let request = try JSONDecoder().decode(GenerationRequest.self, from: data)

        XCTAssertEqual(request.requestID, "request-2")
        XCTAssertEqual(request.prompt, "Hi")
        XCTAssertEqual(request.temperature, 0)
        XCTAssertEqual(request.maxTokens, 4)
    }

    func testRequestDecodingRejectsNonPositiveMaxTokens() {
        let data = Data(
            #"{"request_id":"request-3","prompt":"Hi","temperature":0,"max_tokens":0}"#.utf8
        )

        XCTAssertThrowsError(try JSONDecoder().decode(GenerationRequest.self, from: data))
    }

    func testUsageEncodesExactSnakeCaseWireFields() throws {
        let usage = GenerationUsage(promptTokens: 3, completionTokens: 2)

        let object = try XCTUnwrap(
            JSONSerialization.jsonObject(with: JSONEncoder().encode(usage))
                as? [String: Any]
        )

        XCTAssertEqual(Set(object.keys), ["prompt_tokens", "completion_tokens"])
        XCTAssertEqual(object["prompt_tokens"] as? Int, 3)
        XCTAssertEqual(object["completion_tokens"] as? Int, 2)
    }

    func testFinishReasonsEncodeAsWireValues() throws {
        let encoder = JSONEncoder()

        XCTAssertEqual(String(decoding: try encoder.encode(GenerationFinishReason.stop), as: UTF8.self), #""stop""#)
        XCTAssertEqual(String(decoding: try encoder.encode(GenerationFinishReason.length), as: UTF8.self), #""length""#)
        XCTAssertEqual(String(decoding: try encoder.encode(GenerationFinishReason.cancelled), as: UTF8.self), #""cancelled""#)
        XCTAssertEqual(String(decoding: try encoder.encode(GenerationFinishReason.error), as: UTF8.self), #""error""#)
    }

    func testGenerationEventsPreserveStreamOrder() {
        let usage = GenerationUsage(promptTokens: 3, completionTokens: 2)
        let events: [GenerationEvent] = [
            .started(requestID: "request-1"),
            .token("Hel"),
            .token("lo"),
            .usage(usage),
            .finished(.stop),
        ]

        XCTAssertEqual(
            events,
            [
                .started(requestID: "request-1"),
                .token("Hel"),
                .token("lo"),
                .usage(usage),
                .finished(.stop),
            ]
        )
    }
}
