import Foundation
import LoxaMLXCore
import Network

enum HTTPServerError: Error, Sendable, Equatable, LocalizedError {
    case alreadyStarted
    case listenerFailed(String)
    case listenerCancelledBeforeReady
    case connectionFailed(String)

    var errorDescription: String? {
        switch self {
        case .alreadyStarted:
            "The HTTP server has already been started"
        case .listenerFailed(let message):
            "The loopback listener failed: \(message)"
        case .listenerCancelledBeforeReady:
            "The loopback listener was cancelled before becoming ready"
        case .connectionFailed(let message):
            "The loopback connection failed: \(message)"
        }
    }
}

protocol HTTPResponseWriting: Sendable {
    func write(_ data: Data) async throws
    func close() async
}

final class HTTPServer: @unchecked Sendable {
    private let host: String
    private let port: UInt16
    private let engineToken: String
    private let coordinator: GenerationCoordinator
    private let queue = DispatchQueue(label: "com.loxa.mlx.http-server")
    private let lifecycle = ListenerLifecycle()
    private let listenerLock = NSLock()
    private var listener: NWListener?

    init(
        host: String,
        port: UInt16,
        engineToken: String,
        coordinator: GenerationCoordinator
    ) {
        self.host = host
        self.port = port
        self.engineToken = engineToken
        self.coordinator = coordinator
    }

    func start() async throws {
        let parameters = NWParameters.tcp
        parameters.allowLocalEndpointReuse = true
        let endpointPort = NWEndpoint.Port(rawValue: port) ?? .any
        parameters.requiredLocalEndpoint = .hostPort(
            host: NWEndpoint.Host(host),
            port: endpointPort
        )

        let newListener = try NWListener(using: parameters)
        try installListener(newListener)

        let lifecycle = self.lifecycle
        newListener.stateUpdateHandler = { state in
            switch state {
            case .ready:
                Task { await lifecycle.markReady() }
            case .failed(let error):
                let message = String(describing: error)
                Task { await lifecycle.markFailed(message) }
            case .cancelled:
                Task { await lifecycle.markCancelled() }
            default:
                break
            }
        }
        newListener.newConnectionHandler = { [weak self] connection in
            self?.accept(connection)
        }
        newListener.start(queue: queue)

        try await lifecycle.waitUntilReady()
    }

    func waitUntilStopped() async throws {
        try await lifecycle.waitUntilStopped()
    }

    func stop() {
        listenerSnapshot()?.cancel()
    }

    func handle(_ requestData: Data, writer: any HTTPResponseWriting) async {
        let request: HTTPRequest
        do {
            request = try HTTPRequestParser.parse(requestData)
        } catch {
            try? await writeJSON(
                HTTPErrorPayload(error: "bad_request"),
                status: 400,
                writer: writer
            )
            await writer.close()
            return
        }

        guard isAuthorized(request.headers["authorization"]) else {
            try? await writeJSON(
                HTTPErrorPayload(error: "unauthorized"),
                status: 401,
                writer: writer
            )
            await writer.close()
            return
        }

        await route(request, writer: writer)
        await writer.close()
    }

    private func route(_ request: HTTPRequest, writer: any HTTPResponseWriting) async {
        switch (request.method, request.path) {
        case ("GET", "/health"):
            try? await writeJSON(
                HealthPayload(ok: true),
                status: 200,
                writer: writer
            )

        case ("GET", "/ready"):
            let ready = await coordinator.isReady()
            try? await writeJSON(
                ReadinessPayload(ready: ready),
                status: 200,
                writer: writer
            )

        case ("POST", "/generate"):
            await handleGenerate(request, writer: writer)

        case ("POST", "/cancel"):
            await handleCancel(request, writer: writer)

        default:
            let knownPaths = ["/health", "/ready", "/generate", "/cancel"]
            let status = knownPaths.contains(request.path) ? 405 : 404
            let error = status == 405 ? "method_not_allowed" : "not_found"
            try? await writeJSON(
                HTTPErrorPayload(error: error),
                status: status,
                writer: writer
            )
        }
    }

    private func handleGenerate(
        _ request: HTTPRequest,
        writer: any HTTPResponseWriting
    ) async {
        let generationRequest: GenerationRequest
        do {
            generationRequest = try JSONDecoder().decode(
                GenerationRequest.self,
                from: request.body
            )
        } catch {
            try? await writeJSON(
                HTTPErrorPayload(error: "invalid_generation_request"),
                status: 400,
                writer: writer
            )
            return
        }

        let stream = NDJSONStream(writer: writer)
        do {
            try await coordinator.generate(generationRequest) { event in
                try await stream.emit(event)
            }
            try? await stream.finish()
        } catch let error as GenerationCoordinatorError {
            if await stream.hasBegun {
                try? await stream.finish()
                return
            }

            let status: Int
            let errorCode: String
            switch error {
            case .notReady:
                status = 503
                errorCode = "not_ready"
            case .generationAlreadyInProgress:
                status = 409
                errorCode = "generation_in_progress"
            }
            try? await writeJSON(
                HTTPErrorPayload(error: errorCode),
                status: status,
                writer: writer
            )
        } catch {
            try? await stream.finish()
        }
    }

    private func handleCancel(
        _ request: HTTPRequest,
        writer: any HTTPResponseWriting
    ) async {
        let cancellation: CancellationPayload
        do {
            cancellation = try JSONDecoder().decode(
                CancellationPayload.self,
                from: request.body
            )
        } catch {
            try? await writeJSON(
                HTTPErrorPayload(error: "invalid_cancellation_request"),
                status: 400,
                writer: writer
            )
            return
        }

        _ = await coordinator.cancel(requestID: cancellation.requestID)
        try? await writeJSON(
            SuccessPayload(ok: true),
            status: 200,
            writer: writer
        )
    }

    private func isAuthorized(_ authorization: String?) -> Bool {
        guard let authorization else {
            return false
        }
        return constantTimeEqual(
            Array(authorization.utf8),
            Array("Bearer \(engineToken)".utf8)
        )
    }

    private func constantTimeEqual(_ lhs: [UInt8], _ rhs: [UInt8]) -> Bool {
        let count = max(lhs.count, rhs.count)
        var difference = lhs.count ^ rhs.count
        for index in 0..<count {
            let left = index < lhs.count ? lhs[index] : 0
            let right = index < rhs.count ? rhs[index] : 0
            difference |= Int(left ^ right)
        }
        return difference == 0
    }

    private func writeJSON<Value: Encodable>(
        _ value: Value,
        status: Int,
        writer: any HTTPResponseWriting
    ) async throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        let body = try encoder.encode(value)
        let head = HTTPResponseEncoder.head(
            status: status,
            headers: [
                "Cache-Control": "no-store",
                "Connection": "close",
                "Content-Length": String(body.count),
                "Content-Type": "application/json",
            ]
        )
        var response = head
        response.append(body)
        try await writer.write(response)
    }

    private func installListener(_ newListener: NWListener) throws {
        listenerLock.lock()
        defer { listenerLock.unlock() }
        guard listener == nil else {
            throw HTTPServerError.alreadyStarted
        }
        listener = newListener
    }

    private func listenerSnapshot() -> NWListener? {
        listenerLock.lock()
        defer { listenerLock.unlock() }
        return listener
    }

    private func accept(_ connection: NWConnection) {
        connection.start(queue: queue)
        Task { [weak self] in
            await self?.serve(connection)
        }
    }

    private func serve(_ connection: NWConnection) async {
        let writer = NetworkHTTPWriter(connection: connection)
        var requestData = Data()

        do {
            while true {
                do {
                    _ = try HTTPRequestParser.parse(requestData)
                    await handle(requestData, writer: writer)
                    return
                } catch HTTPRequestParsingError.incomplete {
                    let received = try await receive(from: connection)
                    if let data = received.data {
                        requestData.append(data)
                    }
                    if received.isComplete {
                        await handle(requestData, writer: writer)
                        return
                    }
                } catch {
                    await handle(requestData, writer: writer)
                    return
                }
            }
        } catch {
            await writer.close()
        }
    }

    private func receive(from connection: NWConnection) async throws -> ReceivedData {
        try await withCheckedThrowingContinuation { continuation in
            connection.receive(
                minimumIncompleteLength: 1,
                maximumLength: 8_192
            ) { data, _, isComplete, error in
                if let error {
                    continuation.resume(
                        throwing: HTTPServerError.connectionFailed(
                            String(describing: error)
                        )
                    )
                } else {
                    continuation.resume(
                        returning: ReceivedData(data: data, isComplete: isComplete)
                    )
                }
            }
        }
    }
}

private struct ReceivedData: Sendable {
    let data: Data?
    let isComplete: Bool
}

private actor ListenerLifecycle {
    private typealias Outcome = Result<Void, HTTPServerError>

    private var readyOutcome: Outcome?
    private var readyWaiters: [CheckedContinuation<Outcome, Never>] = []
    private var stopOutcome: Outcome?
    private var stopWaiters: [CheckedContinuation<Outcome, Never>] = []

    func markReady() {
        resolveReady(.success(()))
    }

    func markFailed(_ message: String) {
        let outcome: Outcome = .failure(.listenerFailed(message))
        resolveReady(outcome)
        resolveStop(outcome)
    }

    func markCancelled() {
        if readyOutcome == nil {
            resolveReady(.failure(.listenerCancelledBeforeReady))
        }
        resolveStop(.success(()))
    }

    func waitUntilReady() async throws {
        let outcome: Outcome
        if let readyOutcome {
            outcome = readyOutcome
        } else {
            outcome = await withCheckedContinuation { continuation in
                readyWaiters.append(continuation)
            }
        }
        try outcome.get()
    }

    func waitUntilStopped() async throws {
        let outcome: Outcome
        if let stopOutcome {
            outcome = stopOutcome
        } else {
            outcome = await withCheckedContinuation { continuation in
                stopWaiters.append(continuation)
            }
        }
        try outcome.get()
    }

    private func resolveReady(_ outcome: Outcome) {
        guard readyOutcome == nil else {
            return
        }
        readyOutcome = outcome
        let waiters = readyWaiters
        readyWaiters.removeAll()
        for waiter in waiters {
            waiter.resume(returning: outcome)
        }
    }

    private func resolveStop(_ outcome: Outcome) {
        guard stopOutcome == nil else {
            return
        }
        stopOutcome = outcome
        let waiters = stopWaiters
        stopWaiters.removeAll()
        for waiter in waiters {
            waiter.resume(returning: outcome)
        }
    }
}

private actor NetworkHTTPWriter: HTTPResponseWriting {
    private let connection: NWConnection

    init(connection: NWConnection) {
        self.connection = connection
    }

    func write(_ data: Data) async throws {
        try await withCheckedThrowingContinuation {
            (continuation: CheckedContinuation<Void, any Error>) in
            connection.send(content: data, completion: .contentProcessed { error in
                if let error {
                    continuation.resume(
                        throwing: HTTPServerError.connectionFailed(
                            String(describing: error)
                        )
                    )
                } else {
                    continuation.resume()
                }
            })
        }
    }

    func close() async {
        connection.cancel()
    }
}

private actor NDJSONStream {
    private let writer: any HTTPResponseWriting
    private var begun = false
    private var writable = true
    private var finished = false

    init(writer: any HTTPResponseWriting) {
        self.writer = writer
    }

    var hasBegun: Bool {
        begun
    }

    func emit(_ event: GenerationEvent) async throws {
        guard writable, !finished else {
            return
        }

        do {
            if !begun {
                begun = true
                try await writer.write(
                    HTTPResponseEncoder.head(
                        status: 200,
                        headers: [
                            "Cache-Control": "no-store",
                            "Connection": "close",
                            "Content-Type": "application/x-ndjson",
                            "Transfer-Encoding": "chunked",
                        ]
                    )
                )
            }

            var line = try GenerationEventEncoder.encode(event)
            line.append(0x0A)
            try await writer.write(HTTPResponseEncoder.chunk(line))
        } catch {
            writable = false
            throw error
        }
    }

    func finish() async throws {
        guard begun, writable, !finished else {
            return
        }
        finished = true

        do {
            try await writer.write(Data("0\r\n\r\n".utf8))
        } catch {
            writable = false
            throw error
        }
    }
}

private enum HTTPResponseEncoder {
    static func head(status: Int, headers: [String: String]) -> Data {
        var lines = ["HTTP/1.1 \(status) \(reasonPhrase(for: status))"]
        for name in headers.keys.sorted() {
            if let value = headers[name] {
                lines.append("\(name): \(value)")
            }
        }
        return Data((lines.joined(separator: "\r\n") + "\r\n\r\n").utf8)
    }

    static func chunk(_ data: Data) -> Data {
        var chunk = Data(String(data.count, radix: 16).utf8)
        chunk.append(Data("\r\n".utf8))
        chunk.append(data)
        chunk.append(Data("\r\n".utf8))
        return chunk
    }

    private static func reasonPhrase(for status: Int) -> String {
        switch status {
        case 200: "OK"
        case 400: "Bad Request"
        case 401: "Unauthorized"
        case 404: "Not Found"
        case 405: "Method Not Allowed"
        case 409: "Conflict"
        case 503: "Service Unavailable"
        default: "Internal Server Error"
        }
    }
}

private enum GenerationEventEncoder {
    static func encode(_ event: GenerationEvent) throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        return try encoder.encode(WireGenerationEvent(event: event))
    }
}

private struct WireGenerationEvent: Encodable {
    let event: GenerationEvent

    private enum CodingKeys: String, CodingKey {
        case type
        case requestID = "request_id"
        case text
        case promptTokens = "prompt_tokens"
        case completionTokens = "completion_tokens"
        case reason
        case message
    }

    func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch event {
        case .started(let requestID):
            try container.encode("started", forKey: .type)
            try container.encode(requestID, forKey: .requestID)
        case .token(let text):
            try container.encode("token", forKey: .type)
            try container.encode(text, forKey: .text)
        case .usage(let usage):
            try container.encode("usage", forKey: .type)
            try container.encode(usage.promptTokens, forKey: .promptTokens)
            try container.encode(usage.completionTokens, forKey: .completionTokens)
        case .finished(let reason):
            try container.encode("finished", forKey: .type)
            try container.encode(reason.rawValue, forKey: .reason)
        case .error(let message):
            try container.encode("error", forKey: .type)
            try container.encode(message, forKey: .message)
        }
    }
}

private struct HealthPayload: Encodable, Sendable {
    let ok: Bool
}

private struct ReadinessPayload: Encodable, Sendable {
    let ready: Bool
}

private struct SuccessPayload: Encodable, Sendable {
    let ok: Bool
}

private struct HTTPErrorPayload: Encodable, Sendable {
    let error: String
}

private struct CancellationPayload: Decodable, Sendable {
    let requestID: String

    private enum CodingKeys: String, CodingKey {
        case requestID = "request_id"
    }
}
