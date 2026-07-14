import Foundation
import LoxaMLXCore
import XCTest
@testable import LoxaMLXServer

final class CancellationTests: XCTestCase {
    func testCoordinatorRejectsASecondActiveGeneration() async throws {
        let generator = CancellableGenerator()
        let coordinator = GenerationCoordinator(generator: generator)
        await coordinator.startLoading()
        let firstRequest = try makeRequest(id: "req_first")
        let secondRequest = try makeRequest(id: "req_second")
        let firstEvents = EventRecorder()

        let firstGeneration = Task {
            try await coordinator.generate(firstRequest) { event in
                await firstEvents.record(event)
            }
        }
        await firstEvents.waitForCount(1)

        do {
            try await coordinator.generate(secondRequest) { _ in }
            XCTFail("expected a conflicting generation")
        } catch let error as GenerationCoordinatorError {
            XCTAssertEqual(error, .generationAlreadyInProgress)
        }

        let cancelled = await coordinator.cancel(requestID: firstRequest.requestID)
        XCTAssertTrue(cancelled)
        _ = try? await firstGeneration.value
    }

    func testCancelCancelsMatchingTaskAndEmitsOneCancelledTerminal() async throws {
        let generator = CancellableGenerator()
        let coordinator = GenerationCoordinator(generator: generator)
        await coordinator.startLoading()
        let request = try makeRequest(id: "req_cancel")
        let events = EventRecorder()

        let generation = Task {
            try await coordinator.generate(request) { event in
                await events.record(event)
            }
        }
        await events.waitForCount(1)

        let cancelled = await coordinator.cancel(requestID: request.requestID)
        _ = try? await generation.value

        XCTAssertTrue(cancelled)
        let recorded = await events.values()
        XCTAssertEqual(recorded, [
            .started(requestID: request.requestID),
            .finished(.cancelled),
        ])
    }

    func testCancelIsKeyedByRequestIDAndMissingRequestsAreHarmless() async throws {
        let generator = CancellableGenerator()
        let coordinator = GenerationCoordinator(generator: generator)
        await coordinator.startLoading()
        let request = try makeRequest(id: "req_active")
        let events = EventRecorder()

        let generation = Task {
            try await coordinator.generate(request) { event in
                await events.record(event)
            }
        }
        await events.waitForCount(1)

        let mismatched = await coordinator.cancel(requestID: "req_other")
        let matching = await coordinator.cancel(requestID: request.requestID)
        _ = try? await generation.value
        let afterTerminal = await coordinator.cancel(requestID: request.requestID)

        XCTAssertFalse(mismatched)
        XCTAssertTrue(matching)
        XCTAssertFalse(afterTerminal)
    }

    func testActiveGenerationIsRemovedAfterSuccess() async throws {
        let generator = ImmediateGenerator(result: .success)
        let coordinator = GenerationCoordinator(generator: generator)
        await coordinator.startLoading()

        try await coordinator.generate(makeRequest(id: "req_one")) { _ in }
        try await coordinator.generate(makeRequest(id: "req_two")) { _ in }

        let generatedIDs = await generator.generatedRequestIDs()
        XCTAssertEqual(generatedIDs, ["req_one", "req_two"])
    }

    func testActiveGenerationIsRemovedAfterFailureWithoutDoubleTerminal() async throws {
        let generator = ImmediateGenerator(result: .failure)
        let coordinator = GenerationCoordinator(generator: generator)
        await coordinator.startLoading()
        let firstEvents = EventRecorder()

        do {
            try await coordinator.generate(makeRequest(id: "req_one")) { event in
                await firstEvents.record(event)
            }
            XCTFail("expected fake generation failure")
        } catch is FakeGenerationError {
            // Expected.
        }

        do {
            try await coordinator.generate(makeRequest(id: "req_two")) { _ in }
            XCTFail("expected fake generation failure")
        } catch is FakeGenerationError {
            // A second generation reached the fake, proving cleanup.
        }

        let recorded = await firstEvents.values()
        XCTAssertEqual(recorded.count, 2)
        XCTAssertEqual(recorded.first, .started(requestID: "req_one"))
        guard let terminal = recorded.last else {
            return XCTFail("expected one error terminal event")
        }
        guard case .error = terminal else {
            return XCTFail("expected one error terminal event")
        }
    }

    private func makeRequest(id: String) throws -> GenerationRequest {
        try GenerationRequest(
            requestID: id,
            prompt: "Hello",
            temperature: 0,
            maxTokens: 1
        )
    }
}

private actor EventRecorder {
    private var events: [GenerationEvent] = []

    func record(_ event: GenerationEvent) {
        events.append(event)
    }

    func values() -> [GenerationEvent] {
        events
    }

    func waitForCount(_ count: Int) async {
        while events.count < count {
            await Task.yield()
        }
    }
}

private actor CancellableGenerator: TextGenerating {
    func load() async throws {}

    func warmUp() async throws {}

    func generate(
        _ request: GenerationRequest,
        onEvent: @escaping @Sendable (GenerationEvent) async throws -> Void
    ) async throws {
        try await onEvent(.started(requestID: request.requestID))
        try await Task.sleep(for: .seconds(60))
    }
}

private enum ImmediateResult: Sendable {
    case success
    case failure
}

private enum FakeGenerationError: Error, Sendable {
    case failed
}

private actor ImmediateGenerator: TextGenerating {
    private let result: ImmediateResult
    private var requestIDs: [String] = []

    init(result: ImmediateResult) {
        self.result = result
    }

    func load() async throws {}

    func warmUp() async throws {}

    func generate(
        _ request: GenerationRequest,
        onEvent: @escaping @Sendable (GenerationEvent) async throws -> Void
    ) async throws {
        requestIDs.append(request.requestID)
        try await onEvent(.started(requestID: request.requestID))

        switch result {
        case .success:
            try await onEvent(.finished(.stop))
        case .failure:
            throw FakeGenerationError.failed
        }
    }

    func generatedRequestIDs() -> [String] {
        requestIDs
    }
}
