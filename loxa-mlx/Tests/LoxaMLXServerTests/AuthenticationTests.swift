import Foundation
import LoxaMLXCore
import XCTest
@testable import LoxaMLXServer

final class AuthenticationTests: XCTestCase {
    func testParseAcceptsStrictValidOptions() throws {
        try withModelDirectory { modelDirectory in
            let options = try ServeCommand.parse([
                "--model", modelDirectory.path,
                "--host", "127.0.0.1",
                "--port", "65_535".replacingOccurrences(of: "_", with: ""),
                "--engine-token", "secret-token",
            ])

            XCTAssertEqual(options.modelDirectory, modelDirectory.standardizedFileURL)
            XCTAssertEqual(options.host, "127.0.0.1")
            XCTAssertEqual(options.port, 65_535)
            XCTAssertEqual(options.engineToken, "secret-token")
        }
    }

    func testParseRejectsMissingModelOption() throws {
        XCTAssertThrowsError(try ServeCommand.parse([
            "--host", "127.0.0.1",
            "--port", "8080",
            "--engine-token", "secret-token",
        ]))
    }

    func testParseRejectsRelativeModelPath() throws {
        XCTAssertThrowsError(try ServeCommand.parse([
            "--model", "relative/model",
            "--host", "127.0.0.1",
            "--port", "8080",
            "--engine-token", "secret-token",
        ]))
    }

    func testParseRejectsMissingModelDirectory() throws {
        let missing = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)

        XCTAssertThrowsError(try ServeCommand.parse([
            "--model", missing.path,
            "--host", "127.0.0.1",
            "--port", "8080",
            "--engine-token", "secret-token",
        ]))
    }

    func testParseRejectsFileAsModelDirectory() throws {
        let fileURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: false)
        try Data().write(to: fileURL)
        defer { try? FileManager.default.removeItem(at: fileURL) }

        XCTAssertThrowsError(try ServeCommand.parse([
            "--model", fileURL.path,
            "--host", "127.0.0.1",
            "--port", "8080",
            "--engine-token", "secret-token",
        ]))
    }

    func testParseAcceptsOnlyLiteralIPv4LoopbackHost() throws {
        try withModelDirectory { modelDirectory in
            for host in ["localhost", "::1", "0.0.0.0", "127.0.0.2"] {
                XCTAssertThrowsError(try ServeCommand.parse([
                    "--model", modelDirectory.path,
                    "--host", host,
                    "--port", "8080",
                    "--engine-token", "secret-token",
                ]), "host \(host) must be rejected")
            }
        }
    }

    func testParseRejectsPortsOutsideUInt16ServiceRange() throws {
        try withModelDirectory { modelDirectory in
            for port in ["-1", "0", "65536", "not-a-port"] {
                XCTAssertThrowsError(try ServeCommand.parse([
                    "--model", modelDirectory.path,
                    "--host", "127.0.0.1",
                    "--port", port,
                    "--engine-token", "secret-token",
                ]), "port \(port) must be rejected")
            }
        }
    }

    func testParseRejectsEmptyEngineToken() throws {
        try withModelDirectory { modelDirectory in
            XCTAssertThrowsError(try ServeCommand.parse([
                "--model", modelDirectory.path,
                "--host", "127.0.0.1",
                "--port", "8080",
                "--engine-token", "",
            ]))
        }
    }

    func testParseRejectsUnknownDuplicateAndIncompleteOptions() throws {
        try withModelDirectory { modelDirectory in
            let base = [
                "--model", modelDirectory.path,
                "--host", "127.0.0.1",
                "--port", "8080",
                "--engine-token", "secret-token",
            ]

            XCTAssertThrowsError(try ServeCommand.parse(base + ["--extra", "value"]))
            XCTAssertThrowsError(try ServeCommand.parse(base + ["--port", "8081"]))
            XCTAssertThrowsError(try ServeCommand.parse(Array(base.dropLast())))
        }
    }

    func testEveryEndpointRejectsMissingOrWrongBearerTokenWithoutEchoingSecret() async {
        let expectedToken = "expected-secret-token-\(UUID().uuidString)"
        let coordinator = GenerationCoordinator(generator: AuthenticationGenerator())
        let server = HTTPServer(
            host: "127.0.0.1",
            port: 8080,
            engineToken: expectedToken,
            coordinator: coordinator
        )
        let endpoints: [(method: String, path: String, body: Data)] = [
            ("GET", "/health", Data()),
            ("GET", "/ready", Data()),
            (
                "POST",
                "/generate",
                Data(#"{"request_id":"req_auth","prompt":"Hello","temperature":0,"max_tokens":1}"#.utf8)
            ),
            ("POST", "/cancel", Data(#"{"request_id":"req_auth"}"#.utf8)),
        ]

        for endpoint in endpoints {
            for authorization in [nil, "Bearer wrong-token", "Basic wrong-token"] as [String?] {
                let writer = RecordingHTTPWriter()
                let request = makeHTTPRequest(
                    method: endpoint.method,
                    path: endpoint.path,
                    authorization: authorization,
                    body: endpoint.body
                )

                await server.handle(request, writer: writer)

                let response = await writer.combinedData()
                XCTAssertEqual(responseStatus(response), 401)
                XCTAssertFalse(String(decoding: response, as: UTF8.self).contains(expectedToken))
            }
        }
    }

    func testExactBearerTokenAuthenticatesHealthEndpoint() async {
        let coordinator = GenerationCoordinator(generator: AuthenticationGenerator())
        let server = HTTPServer(
            host: "127.0.0.1",
            port: 8080,
            engineToken: "expected-token",
            coordinator: coordinator
        )
        let writer = RecordingHTTPWriter()
        let request = makeHTTPRequest(
            method: "GET",
            path: "/health",
            authorization: "Bearer expected-token",
            body: Data()
        )

        await server.handle(request, writer: writer)

        let response = await writer.combinedData()
        XCTAssertEqual(responseStatus(response), 200)
    }

    private func withModelDirectory(
        _ body: (URL) throws -> Void
    ) throws {
        let modelDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(
            at: modelDirectory,
            withIntermediateDirectories: false
        )
        defer { try? FileManager.default.removeItem(at: modelDirectory) }

        try body(modelDirectory)
    }

    private func makeHTTPRequest(
        method: String,
        path: String,
        authorization: String?,
        body: Data
    ) -> Data {
        var headers = [
            "Host: 127.0.0.1",
            "Content-Length: \(body.count)",
        ]
        if let authorization {
            headers.append("Authorization: \(authorization)")
        }
        let head = (["\(method) \(path) HTTP/1.1"] + headers)
            .joined(separator: "\r\n") + "\r\n\r\n"
        var request = Data(head.utf8)
        request.append(body)
        return request
    }

    private func responseStatus(_ response: Data) -> Int? {
        let responseText = String(decoding: response, as: UTF8.self)
        let statusLine = responseText.components(separatedBy: "\r\n").first
        return statusLine.flatMap {
            let pieces = $0.split(separator: " ")
            guard pieces.count >= 2 else { return nil }
            return Int(pieces[1])
        }
    }
}

private actor RecordingHTTPWriter: HTTPResponseWriting {
    private var writes: [Data] = []

    func write(_ data: Data) async throws {
        writes.append(data)
    }

    func close() async {}

    func combinedData() -> Data {
        writes.reduce(into: Data()) { result, write in
            result.append(write)
        }
    }
}

private actor AuthenticationGenerator: TextGenerating {
    func load() async throws {}

    func warmUp() async throws {}

    func generate(
        _ request: GenerationRequest,
        onEvent: @escaping @Sendable (GenerationEvent) async throws -> Void
    ) async throws {
        try await onEvent(.started(requestID: request.requestID))
        try await onEvent(.finished(.stop))
    }
}
