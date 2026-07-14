import Foundation
import LoxaMLXCore
import XCTest
@testable import LoxaMLXServer

final class ReadinessTests: XCTestCase {
    func testCoordinatorIsNotReadyBeforeLoadingStarts() async {
        let generator = LoadingGenerator()
        let coordinator = GenerationCoordinator(generator: generator)

        let ready = await coordinator.isReady()

        XCTAssertFalse(ready)
    }

    func testCoordinatorBecomesReadyOnlyAfterLoadAndWarmUpSucceed() async {
        let generator = LoadingGenerator()
        let coordinator = GenerationCoordinator(generator: generator)

        let loading = Task { await coordinator.startLoading() }
        await generator.waitUntilLoadStarted()
        let readyWhileLoading = await coordinator.isReady()
        XCTAssertFalse(readyWhileLoading)

        await generator.allowLoadToFinish()
        await generator.waitUntilWarmUpStarted()
        let readyWhileWarmingUp = await coordinator.isReady()
        XCTAssertFalse(readyWhileWarmingUp)

        await generator.allowWarmUpToFinish()
        await loading.value
        let readyAfterWarmUp = await coordinator.isReady()
        XCTAssertTrue(readyAfterWarmUp)
    }

    func testCoordinatorRemainsUnreadyWhenLoadFails() async {
        let generator = LoadingGenerator(loadFailure: .load)
        let coordinator = GenerationCoordinator(generator: generator)

        let loading = Task { await coordinator.startLoading() }
        await generator.waitUntilLoadStarted()
        await generator.allowLoadToFinish()
        await loading.value

        let ready = await coordinator.isReady()
        let warmUpStarted = await generator.didStartWarmUp()
        XCTAssertFalse(ready)
        XCTAssertFalse(warmUpStarted)
    }

    func testCoordinatorRemainsUnreadyWhenWarmUpFails() async {
        let generator = LoadingGenerator(warmUpFailure: .warmUp)
        let coordinator = GenerationCoordinator(generator: generator)

        let loading = Task { await coordinator.startLoading() }
        await generator.waitUntilLoadStarted()
        await generator.allowLoadToFinish()
        await generator.waitUntilWarmUpStarted()
        await generator.allowWarmUpToFinish()
        await loading.value

        let ready = await coordinator.isReady()
        XCTAssertFalse(ready)
    }

    func testCoordinatorRejectsGenerationUntilReady() async throws {
        let generator = LoadingGenerator()
        let coordinator = GenerationCoordinator(generator: generator)
        let request = try GenerationRequest(
            requestID: "req_unready",
            prompt: "Hello",
            temperature: 0,
            maxTokens: 1
        )

        do {
            try await coordinator.generate(request) { _ in }
            XCTFail("expected generation to be rejected")
        } catch let error as GenerationCoordinatorError {
            XCTAssertEqual(error, .notReady)
        }
    }

    func testHealthSucceedsBeforeLoadingStarts() async throws {
        let coordinator = GenerationCoordinator(generator: LoadingGenerator())
        let server = makeServer(coordinator: coordinator)
        let writer = ReadinessHTTPWriter()

        await server.handle(makeRequest(path: "/health"), writer: writer)

        let response = await writer.combinedData()
        XCTAssertEqual(responseStatus(response), 200)
        XCTAssertEqual(try responseBoolean(response, key: "ok"), true)
    }

    func testReadyEndpointIsFalseWhileLoadingAndWarmUpArePending() async throws {
        let generator = LoadingGenerator()
        let coordinator = GenerationCoordinator(generator: generator)
        let server = makeServer(coordinator: coordinator)
        let loading = Task { await coordinator.startLoading() }
        await generator.waitUntilLoadStarted()

        let loadingWriter = ReadinessHTTPWriter()
        await server.handle(makeRequest(path: "/ready"), writer: loadingWriter)
        let loadingResponse = await loadingWriter.combinedData()
        XCTAssertEqual(try responseBoolean(loadingResponse, key: "ready"), false)

        await generator.allowLoadToFinish()
        await generator.waitUntilWarmUpStarted()
        let warmUpWriter = ReadinessHTTPWriter()
        await server.handle(makeRequest(path: "/ready"), writer: warmUpWriter)
        let warmUpResponse = await warmUpWriter.combinedData()
        XCTAssertEqual(try responseBoolean(warmUpResponse, key: "ready"), false)

        await generator.allowWarmUpToFinish()
        await loading.value
    }

    func testReadyEndpointIsFalseAfterLoadFailureAndTrueAfterSuccessfulWarmUp() async throws {
        let failingGenerator = LoadingGenerator(loadFailure: .load)
        let failingCoordinator = GenerationCoordinator(generator: failingGenerator)
        let failingLoad = Task { await failingCoordinator.startLoading() }
        await failingGenerator.waitUntilLoadStarted()
        await failingGenerator.allowLoadToFinish()
        await failingLoad.value
        let failedWriter = ReadinessHTTPWriter()
        await makeServer(coordinator: failingCoordinator).handle(
            makeRequest(path: "/ready"),
            writer: failedWriter
        )
        let failedResponse = await failedWriter.combinedData()
        XCTAssertEqual(try responseBoolean(failedResponse, key: "ready"), false)

        let successfulGenerator = LoadingGenerator()
        let successfulCoordinator = GenerationCoordinator(generator: successfulGenerator)
        let successfulLoad = Task { await successfulCoordinator.startLoading() }
        await successfulGenerator.waitUntilLoadStarted()
        await successfulGenerator.allowLoadToFinish()
        await successfulGenerator.waitUntilWarmUpStarted()
        await successfulGenerator.allowWarmUpToFinish()
        await successfulLoad.value
        let readyWriter = ReadinessHTTPWriter()
        await makeServer(coordinator: successfulCoordinator).handle(
            makeRequest(path: "/ready"),
            writer: readyWriter
        )
        let readyResponse = await readyWriter.combinedData()
        XCTAssertEqual(responseStatus(readyResponse), 200)
        XCTAssertEqual(try responseBoolean(readyResponse, key: "ready"), true)
    }

    private func makeServer(coordinator: GenerationCoordinator) -> HTTPServer {
        HTTPServer(
            host: "127.0.0.1",
            port: 8080,
            engineToken: "test-token",
            coordinator: coordinator
        )
    }

    private func makeRequest(path: String) -> Data {
        Data(
            "GET \(path) HTTP/1.1\r\nAuthorization: Bearer test-token\r\nContent-Length: 0\r\n\r\n".utf8
        )
    }

    private func responseStatus(_ response: Data) -> Int? {
        String(decoding: response, as: UTF8.self)
            .components(separatedBy: "\r\n")
            .first
            .flatMap { line in
                let pieces = line.split(separator: " ")
                guard pieces.count >= 2 else { return nil }
                return Int(pieces[1])
            }
    }

    private func responseBoolean(_ response: Data, key: String) throws -> Bool? {
        let delimiter = Data("\r\n\r\n".utf8)
        guard let range = response.range(of: delimiter) else {
            return nil
        }
        let body = Data(response[range.upperBound...])
        let object = try JSONSerialization.jsonObject(with: body)
        return (object as? [String: Any])?[key] as? Bool
    }
}

private actor ReadinessHTTPWriter: HTTPResponseWriting {
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

private enum LoadingFailure: Error, Sendable {
    case load
    case warmUp
}

private actor LoadingGenerator: TextGenerating {
    private let loadFailure: LoadingFailure?
    private let warmUpFailure: LoadingFailure?
    private let loadGate = TestGate()
    private let warmUpGate = TestGate()
    private var loadStarted = false
    private var warmUpStarted = false

    init(
        loadFailure: LoadingFailure? = nil,
        warmUpFailure: LoadingFailure? = nil
    ) {
        self.loadFailure = loadFailure
        self.warmUpFailure = warmUpFailure
    }

    func load() async throws {
        loadStarted = true
        await loadGate.wait()
        if let loadFailure {
            throw loadFailure
        }
    }

    func warmUp() async throws {
        warmUpStarted = true
        await warmUpGate.wait()
        if let warmUpFailure {
            throw warmUpFailure
        }
    }

    func generate(
        _ request: GenerationRequest,
        onEvent: @escaping @Sendable (GenerationEvent) async throws -> Void
    ) async throws {
        try await onEvent(.started(requestID: request.requestID))
        try await onEvent(.finished(.stop))
    }

    func allowLoadToFinish() async {
        await loadGate.open()
    }

    func allowWarmUpToFinish() async {
        await warmUpGate.open()
    }

    func waitUntilLoadStarted() async {
        while !loadStarted {
            await Task.yield()
        }
    }

    func waitUntilWarmUpStarted() async {
        while !warmUpStarted {
            await Task.yield()
        }
    }

    func didStartWarmUp() -> Bool {
        warmUpStarted
    }
}

private actor TestGate {
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        if isOpen {
            return
        }

        await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }

    func open() {
        guard !isOpen else {
            return
        }
        isOpen = true
        let waiting = waiters
        waiters.removeAll()
        for continuation in waiting {
            continuation.resume()
        }
    }
}
